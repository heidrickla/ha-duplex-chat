//! The waker's log file: one UTC-stamped line per event, names and ids only.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Past this size the log moves to `<name>.1`, replacing the older one.
const ROTATE_BYTES: u64 = 1024 * 1024;

#[derive(Clone)]
pub struct Log {
    path: PathBuf,
}

impl Log {
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_owned(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Appends one line. A failed write is dropped: the waker has no other place to say so.
    pub fn line(&self, text: &str) {
        if std::fs::metadata(&self.path).is_ok_and(|m| m.len() > ROTATE_BYTES) {
            let mut old = self.path.clone().into_os_string();
            old.push(".1");
            let _ = std::fs::rename(&self.path, old);
        }
        if let Some(dir) = self.path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            let _ = writeln!(file, "{} {}", utc_now(), text.replace(['\r', '\n'], " "));
        }
    }
}

/// Now as `YYYY-MM-DDTHH:MM:SS.mmmZ`: milliseconds, so a kill can be timed from the log.
pub fn utc_now() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let stamp = utc_stamp(now.as_secs());
    format!(
        "{}.{:03}Z",
        stamp.trim_end_matches('Z'),
        now.subsec_millis()
    )
}

/// `YYYY-MM-DDTHH:MM:SSZ` for seconds since the Unix epoch (civil-from-days, H. Hinnant).
pub fn utc_stamp(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stamps_known_instants() {
        assert_eq!(utc_stamp(0), "1970-01-01T00:00:00Z");
        assert_eq!(utc_stamp(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(utc_stamp(1_794_380_321), "2026-11-11T06:58:41Z");
    }
}
