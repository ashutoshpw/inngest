//! One-shot artifact for Go interoperability tests; never a shipped CLI.
use native_step_state::*;
use serde::Deserialize;
use std::{
    io::{self, Read},
    time::Duration,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Input {
    store: StoreConfig,
    state: StateConfig,
    budget_ms: u64,
    disabled_retries: bool,
    operation: Operation,
}
#[derive(Deserialize)]
#[serde(tag = "kind", deny_unknown_fields, rename_all = "camelCase")]
enum Operation {
    PrepareSave {
        response: Vec<u8>,
        config: PreparationConfig,
    },
    Prepare {
        response: Vec<u8>,
        config: PreparationConfig,
    },
    Save {
        step: Vec<u8>,
        output: Vec<u8>,
        delta: i64,
    },
}
fn main() {
    let mut raw = Vec::new();
    io::stdin()
        .take(32 * 1024 * 1024)
        .read_to_end(&mut raw)
        .unwrap();
    // No input echo (it contains credentials). Malformed test input fails closed.
    let input: Input = match serde_json::from_slice(&raw) {
        Ok(input) => input,
        Err(_) => {
            eprintln!("invalid helper input");
            std::process::exit(2);
        }
    };
    let prepared = match input.operation {
        Operation::PrepareSave { response, config } => prepare_response(&response, &config)
            .and_then(|step| SaveRequest::prepared(&input.state, step)),
        Operation::Prepare { response, config } => {
            let output = match prepare_response(&response, &config) {
                Ok(step) => serde_json::json!({"step": step.step_id(), "output": step.output()}),
                Err(error) => preparation_failure(error),
            };
            println!("{output}");
            return;
        }
        Operation::Save {
            step,
            output,
            delta,
        } => SaveRequest::new(&input.state, step, output, delta),
    };
    let output = match prepared {
        Err(error) => preparation_failure(error),
        Ok(request) => match Runtime::new(input.store) {
            Err(error) => preparation_failure(error),
            Ok(mut runtime) => serde_json::to_value(runtime.save_step(
                &CallContext::new(Duration::from_millis(input.budget_ms)),
                &request,
                input.disabled_retries,
            ))
            .unwrap(),
        },
    };
    println!("{output}");
}

fn preparation_failure(error: PreparationError) -> serde_json::Value {
    serde_json::json!({"preparationError": error, "message": error.to_string(), "standardError": error.standard_error()})
}
