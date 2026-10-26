fn main() {
    if let Err(error) = opcode_bridge::run_stdio() {
        eprintln!("opcode-worker error code {}", error.code as u32);
        std::process::exit(2);
    }
}
