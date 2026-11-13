// No console: the waker runs from a logon task and must never show a window.
#![cfg_attr(windows, windows_subsystem = "windows")]

#[cfg(windows)]
mod run {
    use duplex_waker::broker::HttpBroker;
    use duplex_waker::config::{Paths, WakerConfig};
    use duplex_waker::launch::SystemLauncher;
    use duplex_waker::log::Log;
    use duplex_waker::machine::MachineTools;
    use duplex_waker::registry::WinProcesses;
    use duplex_waker::waker::Waker;
    use std::sync::Arc;
    use std::time::Duration;

    const POLL: Duration = Duration::from_secs(30);

    /// Held for the process's life: a second copy for the same home (a manual start beside
    /// the task) exits.
    fn single_instance(mutex: &str) -> bool {
        use windows_sys::Win32::Foundation::{ERROR_ALREADY_EXISTS, GetLastError};
        use windows_sys::Win32::System::Threading::CreateMutexW;
        let name: Vec<u16> = mutex.encode_utf16().chain(std::iter::once(0)).collect();
        // SAFETY: a named mutex the process keeps until it exits; the handle is never closed.
        unsafe {
            let handle = CreateMutexW(std::ptr::null(), 0, name.as_ptr());
            !handle.is_null() && GetLastError() != ERROR_ALREADY_EXISTS
        }
    }

    pub fn main() {
        let Ok(paths) = Paths::from_env() else {
            return;
        };
        let log = Log::new(&paths.log_file());
        if !single_instance(&paths.instance_name()) {
            log.line("another waker is running; exiting");
            return;
        }
        log.line(&format!("waker {} started", env!("CARGO_PKG_VERSION")));
        // waker.json is read until it is valid, so the task can start before the file exists.
        let mut config_problem: Option<String> = None;
        let config = loop {
            match WakerConfig::load(&paths.config_file()) {
                Ok(config) => break config,
                Err(why) => {
                    if config_problem.as_deref() != Some(why.as_str()) {
                        log.line(&format!("{why}; waiting for it"));
                        config_problem = Some(why);
                    }
                    std::thread::sleep(POLL);
                }
            }
        };
        log.line(&format!("broker {}", config.broker_url));
        let broker = HttpBroker::new(&config.broker_url, &config.token);
        let tools = MachineTools {
            paths: paths.clone(),
        };
        let mut launcher = SystemLauncher::new(Arc::new(WinProcesses), log.clone());
        let mut waker = Waker::new(
            &broker,
            &mut launcher,
            &WinProcesses,
            &tools,
            paths.claude_sessions(),
            &log,
        );
        loop {
            waker.tick();
            std::thread::sleep(POLL);
        }
    }
}

#[cfg(windows)]
fn main() {
    run::main();
}

#[cfg(not(windows))]
fn main() {
    eprintln!("duplex-waker runs on Windows only");
    std::process::exit(2);
}
