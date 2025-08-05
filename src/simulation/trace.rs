use std::{
    io::BufRead,
    sync::{Arc, Mutex, OnceLock},
};

use tracing_subscriber::{
    EnvFilter, Layer,
    fmt::{MakeWriter, writer::MutexGuardWriter},
    layer::SubscriberExt,
    util::{SubscriberInitExt, TryInitError},
};

use super::proto::ActiveTrace;
use crate::simulation::ENV_TRACE_SERVER;

const ENV_RUST_LOG: &str = "RUST_LOG";
const ENV_SIM_LOG: &str = "SIM_LOG";

pub fn init() {
    static DID_INIT: OnceLock<bool> = OnceLock::new();
    if DID_INIT.set(true).is_ok() {
        try_init().expect("unreachable: checked OnceLock before init");
    }
}

pub fn try_init() -> Result<(), TryInitError> {
    let print_layer = if let Ok(directive) = std::env::var(ENV_RUST_LOG) {
        let layer = tracing_subscriber::fmt::layer()
            .with_writer(|| TestWriter)
            .event_format(tracing_subscriber::fmt::format().with_line_number(true))
            .with_level(true)
            .with_filter(EnvFilter::new(directive));
        Some(layer)
    } else {
        None
    };

    let json_layer = if std::env::var(ENV_TRACE_SERVER).is_ok() {
        tracing::info!("setting up irpc tracing subscriber");
        let directive = std::env::var(ENV_SIM_LOG).unwrap_or_else(|_| "debug".to_string());
        let layer = tracing_subscriber::fmt::layer()
            .json()
            .with_writer(global_writer())
            .with_filter(EnvFilter::new(directive));
        Some(layer)
    } else {
        None
    };
    tracing_subscriber::registry()
        .with(print_layer)
        .with(json_layer)
        .try_init()
}

pub fn global_writer() -> LineWriter {
    static WRITER: OnceLock<LineWriter> = OnceLock::new();
    WRITER.get_or_init(Default::default).clone()
}

pub async fn submit_logs(client: &ActiveTrace) -> anyhow::Result<()> {
    let writer = global_writer();
    writer.submit(client).await?;
    Ok(())
}

/// A tracing writer that interacts well with test output capture.
///
/// Using this writer will make sure that the output is captured normally and only printed
/// when the test fails.
#[derive(Debug)]
struct TestWriter;

impl std::io::Write for TestWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        print!(
            "{}",
            std::str::from_utf8(buf).expect("tried to log invalid UTF-8")
        );
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stdout().flush()
    }
}

#[derive(Clone, Default)]
pub struct LineWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl<'a> MakeWriter<'a> for LineWriter {
    type Writer = MutexGuardWriter<'a, Vec<u8>>;

    fn make_writer(&'a self) -> Self::Writer {
        self.buf.make_writer()
    }
}

impl LineWriter {
    pub fn clear(&self) {
        self.buf.lock().expect("lock poisoned").clear();
    }

    pub async fn submit(&self, client: &ActiveTrace) -> anyhow::Result<()> {
        let lines = {
            let mut buf = self.buf.lock().expect("lock poisoned");
            let lines = buf
                .lines()
                .filter_map(|line| match line {
                    Ok(line) => Some(line),
                    Err(err) => {
                        tracing::warn!("Skipping invalid log line: {err:?}");
                        None
                    }
                })
                .collect();
            buf.clear();
            lines
        };
        client.put_logs(lines).await?;
        Ok(())
    }
}
