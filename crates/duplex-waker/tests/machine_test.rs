#![cfg(windows)]

mod common;

use common::Scratch;
use duplex_waker::config::Paths;
use duplex_waker::machine::{MachineTools, find_on_path};
use std::ffi::OsString;

fn tools(scratch: &Scratch) -> MachineTools {
    MachineTools {
        paths: Paths {
            profile: scratch.dir("profile"),
            local_app_data: scratch.dir("local"),
            app_data: scratch.dir("roaming"),
        },
    }
}

fn path_of(dirs: &[&std::path::Path]) -> OsString {
    std::env::join_paths(dirs).unwrap()
}

#[test]
fn a_file_is_found_in_the_first_path_folder_that_holds_it() {
    let scratch = Scratch::new("on-path");
    let (empty, first, second) = (
        scratch.dir("empty"),
        scratch.dir("first"),
        scratch.dir("second"),
    );
    std::fs::write(first.join("codex.exe"), b"").unwrap();
    std::fs::write(second.join("codex.exe"), b"").unwrap();
    let path = path_of(&[&empty, &first, &second]);
    assert_eq!(
        find_on_path("codex.exe", &path),
        Some(first.join("codex.exe"))
    );
    assert_eq!(find_on_path("claude.exe", &path), None);
    assert_eq!(find_on_path("codex.exe", &OsString::new()), None);
}

#[test]
fn codex_is_the_vendored_binary_else_the_one_on_the_path_and_nothing_else() {
    let scratch = Scratch::new("codex");
    let machine = tools(&scratch);
    // A shim in the profile folder is not looked at.
    let shim = machine.paths.profile.join("shims");
    std::fs::create_dir_all(&shim).unwrap();
    std::fs::write(shim.join("codex.exe"), b"").unwrap();
    let on_path = scratch.dir("bin");
    assert_eq!(machine.codex_on(&path_of(&[&on_path])), None);
    std::fs::write(on_path.join("codex.exe"), b"").unwrap();
    assert_eq!(
        machine.codex_on(&path_of(&[&on_path])),
        Some(on_path.join("codex.exe"))
    );
    let vendored = machine.paths.app_data.join(
        "npm/node_modules/@openai/codex/node_modules/@openai/codex-win32-x64/vendor/x86_64-pc-windows-msvc/bin",
    );
    std::fs::create_dir_all(&vendored).unwrap();
    std::fs::write(vendored.join("codex.exe"), b"").unwrap();
    assert_eq!(
        machine.codex_on(&path_of(&[&on_path])),
        Some(vendored.join("codex.exe"))
    );
}
