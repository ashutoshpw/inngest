// Internal prototype: preserve the source repository's license/provenance.
// No copied/generated Lua is checked in or relicensed under workspace metadata.
use std::{
    env, fs,
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
};

fn main() {
    let source = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .join("../../../pkg/execution/state/redis_state/lua/saveResponse.lua");
    println!("cargo:rerun-if-changed={}", source.display());
    let body = fs::read(&source).expect("repository SaveResponse source required");
    // The accepted source has no includes. Any source/include change must be
    // requalified against the Go manifest, never silently embedded.
    let mut hash = Command::new("sha256sum")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("sha256sum is required to verify repository Lua provenance");
    hash.stdin.take().unwrap().write_all(&body).unwrap();
    let result = hash.wait_with_output().unwrap();
    assert!(result.status.success());
    assert_eq!(
        String::from_utf8(result.stdout)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap(),
        "402c8d9e0a6de6de2d6117c0b69f5a6e4823410c05437dc46c30a7aa0d3b9409",
        "SaveResponse source changed: review Go source/includes and fixture first"
    );
    assert_eq!(body.len(), 1611);
    fs::write(
        PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("saveResponse.lua"),
        body,
    )
    .unwrap();
}
