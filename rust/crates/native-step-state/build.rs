// Internal prototype: preserve the source repository's license/provenance.
// No copied/generated Lua is checked in or relicensed under workspace metadata.
use std::{
    env, fs,
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
};

fn verify(body: &[u8], algorithm: &str, expected: &str) {
    let mut child = Command::new(algorithm)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(body).unwrap();
    let result = child.wait_with_output().unwrap();
    assert!(result.status.success());
    assert_eq!(
        String::from_utf8(result.stdout)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap(),
        expected,
        "Lua manifest drift: stop and requalify"
    );
}

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
    enqueue();
}

fn enqueue() {
    let root = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap())
        .join("../../../pkg/execution/state/redis_state/lua");
    let path = root.join("queue/enqueue.lua");
    println!("cargo:rerun-if-changed={}", path.display());
    let raw = fs::read(path).unwrap();
    verify(
        &raw,
        "sha256sum",
        "4a0576efc4302310714fbafc40e43949e364c10ecc452be9b9efdfc2ddaeb261",
    );
    let mut expanded = String::from_utf8(raw).unwrap();
    // Go uses single-pass replacement of each directive in the raw source.
    // ends_with occurs twice; both occurrences are replaced by the same bytes.
    for (name, hash) in [
        (
            "update_pointer_score.lua",
            "6f51187afcd9f2327fda3f04450281865ef1f8da376723ef635d4444030feb7b",
        ),
        (
            "ends_with.lua",
            "2be156ca0bb896383eec4df7be4e4a46ee1783ae3cbf99dfc8582782c9a382f9",
        ),
        (
            "update_account_queues.lua",
            "ec910780e6e67cc89bc12529bb9c8c63ca6c4ff247a3b04398ae790ed6411515",
        ),
        (
            "get_partition_item.lua",
            "437a7906e2524c8709ce92fefbc3eb6b7a96d9459f37b39c9023434f217f51e1",
        ),
        (
            "enqueue_to_partition.lua",
            "f26c662ab34c2a344a0a3b8c524fb7149e83fbe30b9fe304780de02712d3aa69",
        ),
        (
            "update_backlog_pointer.lua",
            "ff83100b08cd1234627429f067ca5e153cd6fbfe56c0f9a2e0a0ff8ff7da77b2",
        ),
    ] {
        let path = root.join("includes").join(name);
        println!("cargo:rerun-if-changed={}", path.display());
        let body = fs::read(path).unwrap();
        verify(&body, "sha256sum", hash);
        expanded = expanded.replace(
            &format!("-- $include({name})"),
            &String::from_utf8(body).unwrap(),
        );
    }
    assert_eq!(expanded.len(), 23635);
    verify(
        expanded.as_bytes(),
        "sha1sum",
        "3868a424cbc217eed464dfef3f1618d64794c103",
    );
    verify(
        expanded.as_bytes(),
        "sha256sum",
        "3e4d40b2c83b4a6e51127c1888a5cd3d452bd41afee9caca1d330af86c3837e0",
    );
    fs::write(
        PathBuf::from(env::var_os("OUT_DIR").unwrap()).join("enqueue.lua"),
        expanded,
    )
    .unwrap();
}
