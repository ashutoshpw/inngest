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
    Response {
        input: Box<response::ResponseInput>,
    },
    Enqueue {
        response: Vec<u8>,
        config: PreparationConfig,
        discovery: Box<DiscoveryConfig>,
    },
    PrepareSave {
        response: Vec<u8>,
        config: PreparationConfig,
        #[serde(default)]
        delta: i64,
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
        .take(32 * 1024 * 1024 + 1)
        .read_to_end(&mut raw)
        .unwrap();
    if raw.len() > 32 * 1024 * 1024 {
        eprintln!("helper input exceeds 32 MiB");
        std::process::exit(2);
    }
    // No input echo (it contains credentials). Malformed test input fails closed.
    let input: Input = match serde_json::from_slice(&raw) {
        Ok(input) => input,
        Err(_) => {
            eprintln!("invalid helper input");
            std::process::exit(2);
        }
    };
    let prepared = match input.operation {
        Operation::Response { input: response } => {
            let output = response::run(
                *response,
                input.state,
                input.store,
                input.budget_ms,
                input.disabled_retries,
            );
            println!("{}", serde_json::to_string(&output).unwrap());
            return;
        }
        Operation::Enqueue {
            response,
            config,
            discovery,
        } => {
            let request = prepare_response(&response, &config)
                .and_then(|step| EnqueueRequest::new(&input.state, &step, &discovery));
            let output = match request {
                Err(error) => preparation_failure(error),
                Ok(request) => match Runtime::new(input.store) {
                    Err(error) => preparation_failure(error),
                    Ok(mut runtime) => serde_json::to_value(runtime.enqueue(
                        &CallContext::new(Duration::from_millis(input.budget_ms)),
                        &request,
                    ))
                    .unwrap(),
                },
            };
            println!("{output}");
            return;
        }
        Operation::PrepareSave {
            response,
            config,
            delta,
        } => prepare_response(&response, &config).and_then(|step| {
            SaveRequest::new(
                &input.state,
                step.step_id().as_bytes().to_vec(),
                step.output().to_vec(),
                delta,
            )
        }),
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
