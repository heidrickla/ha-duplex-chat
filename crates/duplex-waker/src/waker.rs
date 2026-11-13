//! One poll: list the broker's requests, start each listed session that is closed, and
//! report every attempt. Nothing is started from anything but the current listing.

use crate::broker::{Broker, BrokerError, Report, Request};
use crate::launch::{self, Launcher, Tools};
use crate::log::Log;
use crate::registry::{self, Liveness, ProcessTable};
pub use duplex_proto::waker::report_id;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// A report the broker did not take is retried this long; then it and every later
/// report for its request are dropped.
const REPORT_RETRY: Duration = Duration::from_secs(24 * 3600);

struct Unreported {
    report: Report,
    since: Instant,
    /// A failed attempt was logged; later ones are not, so an outage logs once.
    logged: bool,
}

pub struct Waker<'a> {
    pub broker: &'a dyn Broker,
    pub launcher: &'a mut dyn Launcher,
    pub procs: &'a dyn ProcessTable,
    pub tools: &'a dyn Tools,
    pub sessions_dir: PathBuf,
    pub log: &'a Log,
    /// The current outage's cause, logged once when it begins.
    outage: Option<String>,
    /// Reports the broker has not taken yet, per request id, oldest first. The broker
    /// must see a run's reports in the order they happened, each once, so a report is
    /// never replaced and never sent while an earlier one for its request waits. A
    /// request listed again while here is not started again: its start is already made.
    unreported: HashMap<String, VecDeque<Unreported>>,
    /// Reports numbered so far, per request id, for this process's life: the next report
    /// for a request takes the next number, so no two of its reports share an id.
    numbered: HashMap<String, u32>,
    /// This waker process's creation time, UTC milliseconds: part of every report id, so
    /// a restarted waker's reports never reuse an id an earlier process sent.
    pub started_ms: u64,
}

impl<'a> Waker<'a> {
    pub fn new(
        broker: &'a dyn Broker,
        launcher: &'a mut dyn Launcher,
        procs: &'a dyn ProcessTable,
        tools: &'a dyn Tools,
        sessions_dir: PathBuf,
        log: &'a Log,
    ) -> Self {
        Self {
            broker,
            launcher,
            procs,
            tools,
            sessions_dir,
            log,
            outage: None,
            unreported: HashMap::new(),
            numbered: HashMap::new(),
            started_ms: process_start_ms(procs),
        }
    }

    pub fn tick(&mut self) {
        for ended in self.launcher.reap() {
            self.log.line(&format!(
                "request {}: pid {} exited {}",
                ended.request_id,
                ended.pid,
                launch::code_text(ended.code)
            ));
            // The start was reported made; this run was then ended, so report that too.
            if ended.opened_elsewhere {
                let report = Report::new(false, ended.route, launch::OPENED_ELSEWHERE);
                self.send_report(&ended.request_id, report);
            }
        }
        // Reports first: a listing fetched after the broker took a start's report already
        // leaves that request out (contract: no relisting for 15 minutes after a start).
        self.retry_reports();
        let listed = match self.broker.requests() {
            Ok(listed) => {
                if self.outage.take().is_some() {
                    self.log.line("broker reachable again");
                }
                listed
            }
            Err(err) => {
                let text = err.to_string();
                if self.outage.is_none() {
                    self.log
                        .line(&format!("{text}; starting nothing until it answers"));
                }
                self.outage = Some(text);
                return;
            }
        };
        for request in listed {
            if self.unreported.contains_key(&request.id) {
                continue;
            }
            let report = self.handle(&request);
            self.send_report(&request.id, report);
        }
    }

    /// The attempt for one listed request, as its report.
    fn handle(&mut self, request: &Request) -> Report {
        let names = format!(
            "request {} ({} {}, binding {}, {:?})",
            request.id,
            request.provider,
            request.native_id,
            request.binding_id,
            request.title.as_deref().unwrap_or("")
        );
        let route = route_for(&request.provider);
        if let Err(why) = self.closed(request) {
            self.log.line(&format!("{names}: not started: {why}"));
            return Report::new(false, route, &why);
        }
        let plan = match launch::plan(request, self.tools) {
            Ok(plan) => plan,
            Err(why) => {
                self.log.line(&format!("{names}: not started: {why}"));
                return Report::new(false, route, &why);
            }
        };
        // Again just before the launch: finding the tools takes time, and a session
        // opened meanwhile must not get a second process.
        if let Err(why) = self.closed(request) {
            self.log.line(&format!("{names}: not started: {why}"));
            return Report::new(false, route, &why);
        }
        match self.launcher.start(&request.id, &request.native_id, &plan) {
            Ok(started) => {
                self.log.line(&format!(
                    "{names}: started by {} as pid {}",
                    plan.route, started.pid
                ));
                Report::new(true, plan.route, &started.detail)
            }
            Err(why) => {
                self.log
                    .line(&format!("{names}: start by {} failed: {why}", plan.route));
                Report::new(false, plan.route, &why)
            }
        }
    }

    /// Ok only once the session is established closed: no start of the waker's own still
    /// runs for it and, for Claude, no live process holds it. A `claude -p --resume`
    /// beside a live process forks the transcript silently.
    fn closed(&mut self, request: &Request) -> Result<(), String> {
        if let Some(pid) = self.launcher.running(&request.native_id) {
            return Err(format!(
                "session is live (a waker start, pid {pid}, still runs)"
            ));
        }
        if request.provider != "claude" {
            return Ok(());
        }
        match registry::session_liveness(&self.sessions_dir, &request.native_id, self.procs) {
            Liveness::Closed => Ok(()),
            Liveness::Live { pid } => {
                self.log.line(&format!(
                    "request {}: session {} is live (pid {pid})",
                    request.id, request.native_id
                ));
                Err("session is live".to_owned())
            }
            Liveness::Unknown(why) => Err(format!("cannot establish the session is closed: {why}")),
        }
    }

    /// Numbers a report, then sends it, or queues it behind the reports for its request
    /// that wait. Its id stays the same on every retry.
    fn send_report(&mut self, request_id: &str, mut report: Report) {
        let n = self.numbered.entry(request_id.to_owned()).or_insert(0);
        *n += 1;
        report.report_id = report_id(request_id, self.started_ms, *n);
        let entry = Unreported {
            report,
            since: Instant::now(),
            logged: false,
        };
        if let Some(waiting) = self.unreported.get_mut(request_id) {
            waiting.push_back(entry);
            self.log.line(&format!(
                "request {request_id}: report queued behind {} earlier",
                waiting.len() - 1
            ));
            return;
        }
        self.unreported
            .insert(request_id.to_owned(), VecDeque::from([entry]));
        self.flush(request_id);
    }

    fn retry_reports(&mut self) {
        let pending: Vec<String> = self.unreported.keys().cloned().collect();
        for id in pending {
            self.flush(&id);
        }
    }

    /// Sends the request's waiting reports oldest first and stops at the first one the
    /// broker does not take. A refused report is dropped alone; one still not taken after
    /// REPORT_RETRY is dropped with every report behind it, so none arrives out of order.
    fn flush(&mut self, id: &str) {
        while let Some(front) = self.unreported.get(id).and_then(VecDeque::front) {
            let sent = self.broker.started(id, &front.report);
            let expired = front.since.elapsed() > REPORT_RETRY;
            let logged = front.logged;
            match sent {
                Ok(()) => self.log.line(&format!("request {id}: report taken")),
                Err(err) if refused(&err) => {
                    self.log
                        .line(&format!("request {id}: report refused ({err})"));
                }
                Err(_) if expired => {
                    let dropped = self.unreported.remove(id).map_or(0, |q| q.len());
                    self.log.line(&format!(
                        "request {id}: {dropped} report(s) dropped after 24 h"
                    ));
                    return;
                }
                Err(err) => {
                    if !logged {
                        self.log.line(&format!(
                            "request {id}: report not taken ({err}); retrying it, not the start"
                        ));
                    }
                    if let Some(front) = self.unreported.get_mut(id).and_then(VecDeque::front_mut) {
                        front.logged = true;
                    }
                    return;
                }
            }
            if let Some(waiting) = self.unreported.get_mut(id) {
                waiting.pop_front();
            }
        }
        self.unreported.remove(id);
    }
}

/// This process's creation time in UTC milliseconds; the current time when it cannot be
/// read, which is later than any earlier process's start all the same.
fn process_start_ms(procs: &dyn ProcessTable) -> u64 {
    // FILETIME counts 100 ns from 1601; the Unix epoch is 11644473600 s later.
    const UNIX_EPOCH_FILETIME_MS: u64 = 11_644_473_600_000;
    match procs.state(std::process::id()) {
        registry::ProcState::Running(created) if created / 10_000 > UNIX_EPOCH_FILETIME_MS => {
            created / 10_000 - UNIX_EPOCH_FILETIME_MS
        }
        _ => std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
    }
}

/// A 4xx other than a timeout or throttle will not change on a retry.
fn refused(err: &BrokerError) -> bool {
    matches!(err, BrokerError::Status { code, .. } if (400..500).contains(code) && *code != 408 && *code != 429)
}

fn route_for(provider: &str) -> &'static str {
    match provider {
        "claude" => launch::CLAUDE_ROUTE,
        "codex" => launch::CODEX_ROUTE,
        _ => "none",
    }
}
