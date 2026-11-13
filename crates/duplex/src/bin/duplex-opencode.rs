// No console of its own: the logon task starts it in the user's session, where a console
// program would open a window. `duplex opencode` is the same adapter with a console.
#![cfg_attr(windows, windows_subsystem = "windows")]

use std::process::ExitCode;

fn main() -> ExitCode {
    match duplex_adapter::opencode::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(why) => {
            duplex_adapter::opencode::Log::new(Some(
                duplex_adapter::config::config_dir().join("opencode.log"),
            ))
            .warn(&why);
            ExitCode::FAILURE
        }
    }
}
