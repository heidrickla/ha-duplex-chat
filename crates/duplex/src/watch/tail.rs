//! The complete JSON lines of a file that may not exist yet, a poll at a time.
//!
//! `begin` takes the starting place when the adapter starts: the end of the file as it is
//! then (up to `history` bytes of what it holds come back apart, for a reader that needs the
//! state it ended in), so a line written after that and before the first poll is read. The
//! active file is found again at every poll. A file that appears later, or takes the place
//! of the one followed, is read from its start, after the rest of the one it replaces (its
//! lines come marked `replayed`); the same file found under another name is followed on from
//! where it was read. A line still being written waits for its newline. A line that is not a
//! JSON object, or is longer than `MAX_READ`, is logged by position and length (never its
//! content) and passed over. Nothing is consumed until `commit`.
//!
//! No handle is held on the file: a handle held open on Windows can block the writer's own
//! rename or cleanup, so the tail reopens the file by path.

use crate::log;
use serde_json::Value;
use std::fs::File;
use std::io::{self, Read as _, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// The most one poll reads.
pub const MAX_READ: usize = 8 * 1024 * 1024;
/// How far back a Codex rollout is read for the turn state and the latest rate-limit
/// snapshot when the adapter starts.
pub const HISTORY_BYTES: u64 = 4 * 1024 * 1024;
const CHUNK: usize = 1024 * 1024;
/// How far back from the end of a file the starting place looks for the last line break.
const BEGIN_WINDOW: u64 = 64 * 1024;

/// A line, and whether it came from a file the tail met after it began: that file may open
/// with history, so the reader keeps only the events written since the adapter started.
#[derive(Clone, Debug, PartialEq)]
pub struct Read {
    pub line: Value,
    pub replayed: bool,
}

/// What tells one file from another that took its name; a rename keeps it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Identity(u64, u64);

fn identity_of(path: &Path) -> io::Result<Identity> {
    let meta = std::fs::metadata(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(Identity(meta.dev(), meta.ino()))
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        Ok(Identity(0, meta.creation_time()))
    }
}

/// Where a tail has read to. `skipping` is where a line longer than `MAX_READ` began when
/// its end has not been reached; the bytes from there to `offset` are discarded.
#[derive(Clone, Debug)]
struct Place {
    path: PathBuf,
    identity: Identity,
    offset: u64,
    skipping: Option<u64>,
}

type Locate = Box<dyn FnMut() -> Option<PathBuf> + Send>;

pub struct Tail {
    locate: Locate,
    place: Option<Place>,
    next: Option<Place>,
}

fn reject(path: &Path, at: u64, length: usize, why: &str) {
    let name = path
        .file_name()
        .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
    log::warn(format!(
        "rejected a line of {length} bytes at offset {at} of {name}: {why}"
    ));
}

/// The JSON objects in `path[offset..end]`, up to `MAX_READ` bytes of lines per call, with
/// the offset to resume at and where a line longer than `MAX_READ` began when its end is not
/// yet in sight. The offset is the start of a line not yet ended.
fn scan(
    path: &Path,
    offset: u64,
    mut skipping: Option<u64>,
    end: u64,
    mut drop_head: bool,
    keep: bool,
) -> io::Result<(Vec<Value>, u64, Option<u64>)> {
    let mut lines = Vec::new();
    let mut buf: Vec<u8> = Vec::new();
    let mut start = offset;
    let mut read_to = offset;
    let mut budget = MAX_READ as i64;
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset))?;
    while read_to < end && (skipping.is_some() || budget > 0) {
        let want = usize::try_from(end - read_to).unwrap_or(CHUNK).min(CHUNK);
        let mut data = vec![0u8; want];
        let got = file.read(&mut data)?;
        if got == 0 {
            break;
        }
        data.truncate(got);
        read_to += got as u64;
        let mut data = data.as_slice();
        if let Some(began) = skipping {
            // Inside a line longer than MAX_READ.
            let Some(newline) = data.iter().position(|b| *b == b'\n') else {
                continue;
            };
            let stop = read_to - got as u64 + newline as u64 + 1;
            reject(
                path,
                began,
                (stop - began) as usize,
                "longer than the read limit",
            );
            skipping = None;
            start = stop;
            data = &data[newline + 1..];
        }
        buf.extend_from_slice(data);
        let cut = buf.iter().rposition(|b| *b == b'\n').map_or(0, |i| i + 1);
        let mut at = start;
        let mut pieces: Vec<&[u8]> = buf[..cut].split(|b| *b == b'\n').collect();
        pieces.pop();
        for raw in pieces {
            let length = raw.len() + 1;
            if drop_head {
                drop_head = false;
            } else if raw.len() >= MAX_READ {
                reject(path, at, length, "longer than the read limit");
            } else if keep && !raw.iter().all(u8::is_ascii_whitespace) {
                match serde_json::from_slice::<Value>(raw) {
                    Ok(parsed @ Value::Object(_)) => lines.push(parsed),
                    Ok(_) => reject(path, at, length, "not a JSON object"),
                    Err(_) => reject(path, at, length, "not JSON"),
                }
            }
            at += length as u64;
        }
        buf.drain(..cut);
        start += cut as u64;
        budget -= cut as i64;
        if buf.len() >= MAX_READ {
            // It can only be longer than the limit.
            skipping = Some(start);
            buf.clear();
            start = read_to;
        }
    }
    let resume = if skipping.is_some() { read_to } else { start };
    Ok((lines, resume, skipping))
}

impl Tail {
    pub fn new(locate: impl FnMut() -> Option<PathBuf> + Send + 'static) -> Self {
        Self {
            locate: Box::new(locate),
            place: None,
            next: None,
        }
    }

    /// Takes the starting place. Returns the complete lines of the last `history` bytes of
    /// the file. A file that cannot be read now leaves no place, and the file is read from
    /// its start when it can.
    pub fn begin(&mut self, history: u64) -> Vec<Value> {
        let started = (|| -> io::Result<Option<(Vec<Value>, Place)>> {
            let Some(current) = (self.locate)() else {
                return Ok(None);
            };
            let identity = identity_of(&current)?;
            let size = std::fs::metadata(&current)?.len();
            let window = if history > 0 { history } else { BEGIN_WINDOW };
            let start = size.saturating_sub(window);
            let (lines, mut offset, skipping) =
                scan(&current, start, None, size, start > 0, history > 0)?;
            if skipping.is_none() && offset == start && history == 0 && start > 0 {
                offset = size; // the window is one unfinished line: start after it
            }
            Ok(Some((
                lines,
                Place {
                    path: current,
                    identity,
                    offset,
                    skipping,
                },
            )))
        })();
        match started {
            Ok(Some((lines, place))) => {
                self.next = Some(place.clone());
                self.place = Some(place);
                lines
            }
            Ok(None) => Vec::new(),
            Err(err) => {
                log::warn(format!("could not take the starting place: {err}"));
                Vec::new()
            }
        }
    }

    /// The lines written since the last commit. A file that cannot be read now gives nothing
    /// and is tried again.
    pub fn poll(&mut self) -> Vec<Read> {
        let (lines, next) = self.read();
        self.next = next;
        lines
    }

    /// What `poll` returns now, leaving the place a `commit` would take as it is.
    pub fn peek(&mut self) -> Vec<Read> {
        self.read().0
    }

    pub fn commit(&mut self) {
        self.place = self.next.clone();
    }

    fn read(&mut self) -> (Vec<Read>, Option<Place>) {
        let place = self.place.clone();
        match self.poll_from(place.clone()) {
            Ok(done) => done,
            Err(_) => (Vec::new(), place),
        }
    }

    fn poll_from(&mut self, mut place: Option<Place>) -> io::Result<(Vec<Read>, Option<Place>)> {
        let Some(current) = (self.locate)() else {
            return Ok((Vec::new(), place));
        };
        let (identity, size) = match identity_of(&current)
            .and_then(|i| Ok((i, std::fs::metadata(&current)?.len())))
        {
            Ok(found) => found,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok((Vec::new(), place)),
            Err(err) => return Err(err),
        };
        let mut out = Vec::new();
        match &place {
            Some(held) if current != held.path || held.identity != identity => {
                if identity == held.identity {
                    // The file followed, under another name.
                    place = Some(Place {
                        path: current.clone(),
                        ..held.clone()
                    });
                } else {
                    out.extend(rest(held).into_iter().map(|line| Read {
                        line,
                        replayed: false,
                    }));
                    place = None;
                }
            }
            // Cut short in place: what it holds now is new.
            Some(held) if size < held.offset => place = None,
            _ => {}
        }
        let replayed = place.is_none();
        let held = place.unwrap_or(Place {
            path: current.clone(),
            identity,
            offset: 0,
            skipping: None,
        });
        let (lines, offset, skipping) =
            scan(&current, held.offset, held.skipping, size, false, true)?;
        out.extend(lines.into_iter().map(|line| Read { line, replayed }));
        Ok((
            out,
            Some(Place {
                path: current,
                identity,
                offset,
                skipping,
            }),
        ))
    }
}

/// What is left unread in a file the tail has moved off, if still there.
fn rest(place: &Place) -> Vec<Value> {
    let read = || -> io::Result<Vec<Value>> {
        if identity_of(&place.path)? != place.identity {
            return Ok(Vec::new());
        }
        let end = std::fs::metadata(&place.path)?.len();
        Ok(scan(&place.path, place.offset, place.skipping, end, false, true)?.0)
    };
    read().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    pub struct Scratch(pub PathBuf);

    impl Scratch {
        pub fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let dir = std::env::temp_dir().join(format!(
                "duplex-tail-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn append(path: &Path, text: &str) {
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        file.write_all(text.as_bytes()).unwrap();
    }

    fn follow(path: &Path) -> Tail {
        let path = path.to_owned();
        Tail::new(move || path.is_file().then(|| path.clone()))
    }

    fn texts(reads: &[Read]) -> Vec<String> {
        reads.iter().map(|r| r.line["n"].to_string()).collect()
    }

    #[test]
    fn lines_written_after_the_start_are_read_once() {
        let scratch = Scratch::new();
        let path = scratch.0.join("t.jsonl");
        append(&path, "{\"n\":1}\n");
        let mut tail = follow(&path);
        assert!(tail.begin(0).is_empty());
        assert!(
            tail.poll().is_empty(),
            "history before the start is not read"
        );
        append(&path, "{\"n\":2}\n{\"n\":3}\n");
        assert_eq!(texts(&tail.peek()), ["2", "3"]);
        assert_eq!(texts(&tail.poll()), ["2", "3"]);
        tail.commit();
        assert!(tail.poll().is_empty());
    }

    #[test]
    fn nothing_is_consumed_until_commit() {
        let scratch = Scratch::new();
        let path = scratch.0.join("t.jsonl");
        let mut tail = follow(&path);
        tail.begin(0);
        append(&path, "{\"n\":1}\n");
        assert_eq!(tail.poll().len(), 1);
        assert_eq!(
            tail.poll().len(),
            1,
            "an uncommitted poll reads the same lines"
        );
        tail.commit();
        assert!(tail.poll().is_empty());
    }

    #[test]
    fn a_line_still_being_written_waits_for_its_newline() {
        let scratch = Scratch::new();
        let path = scratch.0.join("t.jsonl");
        let mut tail = follow(&path);
        tail.begin(0);
        append(&path, "{\"n\":1}\n{\"n\":");
        assert_eq!(texts(&tail.poll()), ["1"]);
        tail.commit();
        append(&path, "2}\n");
        assert_eq!(texts(&tail.poll()), ["2"]);
    }

    #[test]
    fn lines_that_are_not_json_objects_are_passed_over() {
        let scratch = Scratch::new();
        let path = scratch.0.join("t.jsonl");
        let mut tail = follow(&path);
        tail.begin(0);
        append(&path, "garbage\n[1,2]\n\n  \n{\"n\":7}\n");
        assert_eq!(texts(&tail.poll()), ["7"]);
    }

    #[test]
    fn a_file_that_appears_later_is_read_from_its_start_and_marked_replayed() {
        let scratch = Scratch::new();
        let path = scratch.0.join("late.jsonl");
        let mut tail = follow(&path);
        assert!(tail.begin(0).is_empty());
        append(&path, "{\"n\":1}\n");
        let reads = tail.poll();
        assert_eq!(texts(&reads), ["1"]);
        assert!(reads[0].replayed);
        tail.commit();
        append(&path, "{\"n\":2}\n");
        let more = tail.poll();
        assert!(!more[0].replayed);
    }

    #[test]
    fn a_replacing_file_is_read_after_the_rest_of_the_one_it_replaces() {
        let scratch = Scratch::new();
        let (a, b) = (scratch.0.join("a.jsonl"), scratch.0.join("b.jsonl"));
        append(&a, "{\"n\":0}\n");
        let current = Arc::new(Mutex::new(a.clone()));
        let shared = Arc::clone(&current);
        let mut tail = Tail::new(move || Some(shared.lock().unwrap().clone()));
        tail.begin(0);
        append(&a, "{\"n\":1}\n");
        append(&b, "{\"n\":10}\n");
        *current.lock().unwrap() = b;
        let reads = tail.poll();
        assert_eq!(texts(&reads), ["1", "10"]);
        assert_eq!(
            reads.iter().map(|r| r.replayed).collect::<Vec<_>>(),
            [false, true],
            "the old file's tail is not replayed, the new file is"
        );
    }

    #[test]
    fn a_file_cut_short_in_place_is_read_again_from_its_start() {
        let scratch = Scratch::new();
        let path = scratch.0.join("t.jsonl");
        let mut tail = follow(&path);
        tail.begin(0);
        append(&path, "{\"n\":1}\n{\"n\":2}\n");
        tail.poll();
        tail.commit();
        std::fs::write(&path, "{\"n\":9}\n").unwrap();
        let reads = tail.poll();
        assert_eq!(texts(&reads), ["9"]);
        assert!(reads[0].replayed);
    }

    #[test]
    fn history_comes_back_apart_for_a_reader_that_needs_the_state() {
        let scratch = Scratch::new();
        let path = scratch.0.join("t.jsonl");
        append(&path, "{\"n\":1}\n{\"n\":2}\n");
        let mut tail = follow(&path);
        let history = tail.begin(HISTORY_BYTES);
        assert_eq!(history.len(), 2);
        assert!(tail.poll().is_empty());
    }

    #[test]
    fn a_line_longer_than_the_read_limit_is_skipped_to_its_end() {
        let scratch = Scratch::new();
        let path = scratch.0.join("t.jsonl");
        let mut tail = follow(&path);
        tail.begin(0);
        let long = "x".repeat(MAX_READ + 10);
        append(&path, &format!("{{\"n\":1}}\n{long}\n{{\"n\":2}}\n"));
        let mut seen = texts(&tail.poll());
        tail.commit();
        // The budget is spent on the first poll; a later one reaches the rest.
        for _ in 0..3 {
            seen.extend(texts(&tail.poll()));
            tail.commit();
        }
        assert_eq!(seen, ["1", "2"]);
    }
}
