use std::path::Path;
use std::process::{Command, Output};

fn install(dir: &Path, extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_memex"))
        .args(["rex", "install", "--dir"])
        .arg(dir)
        .args(extra)
        // Keep the user's own Rex config out of the default-directory lookup.
        .env_remove("REX_CONFIG")
        .output()
        .unwrap()
}

fn stdout(output: &Output) -> String {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout.clone()).unwrap()
}

#[test]
fn install_writes_the_module_and_leaves_a_foreign_one_alone() {
    let dir = tempfile::tempdir().unwrap();
    let module = dir.path().join("memex.lua");

    let first = stdout(&install(dir.path(), &[]));
    assert!(first.starts_with("created "), "{first}");
    assert!(first.contains("require(\"memex\").setup{}"), "{first}");
    let written = std::fs::read_to_string(&module).unwrap();
    assert!(written.starts_with("-- Installed by `memex rex install`"));
    assert!(written.contains(env!("CARGO_PKG_VERSION")));
    assert!(written.contains("function M.setup(opts)"));

    // Once init.lua loads it, a reinstall only says what to reload.
    std::fs::write(dir.path().join("init.lua"), "require(\"memex\").setup{}\n").unwrap();
    let again = stdout(&install(dir.path(), &[]));
    assert!(again.starts_with("unchanged "), "{again}");
    assert!(again.contains("already loads it"), "{again}");

    std::fs::write(&module, "-- my own memex module\n").unwrap();
    let refused = install(dir.path(), &[]);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("--force"));
    assert_eq!(
        std::fs::read_to_string(&module).unwrap(),
        "-- my own memex module\n"
    );

    let forced = stdout(&install(dir.path(), &["--force"]));
    assert!(forced.starts_with("updated "), "{forced}");
    assert_eq!(std::fs::read_to_string(&module).unwrap(), written);
}
