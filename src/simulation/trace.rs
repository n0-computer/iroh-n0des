use std::{
    io::BufRead,
    sync::{Arc, Mutex, OnceLock},
};

use tracing_subscriber::{
    fmt::{writer::MutexGuardWriter, MakeWriter},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};

use super::proto::TraceClient;

pub(crate) fn try_init_global_subscriber() {
    static DID_INIT: OnceLock<bool> = OnceLock::new();
    if DID_INIT.set(true).is_err() {
        return;
    }
    tracing::info!("setting up irpc tracing subscriber");
    let print_layer = {
        let directive = std::env::var("RUST_LOG").unwrap_or_else(|_| "".to_string());
        let filter = EnvFilter::new(directive);
        tracing_subscriber::fmt::layer()
            .with_writer(|| TestWriter)
            .event_format(tracing_subscriber::fmt::format().with_line_number(true))
            .with_level(true)
            .with_filter(filter)
    };

    let writer = global_writer();
    let json_layer = {
        let default_directive = "debug".to_string();
        let directive = std::env::var("SIM_LOG").unwrap_or_else(|_| default_directive.to_string());
        let filter = EnvFilter::new(directive);
        tracing_subscriber::fmt::layer()
            .json()
            .with_writer(writer)
            .with_filter(filter)
    };

    let registry = tracing_subscriber::registry()
        .with(json_layer)
        .with(print_layer);

    registry.try_init().ok();
}

pub(crate) fn global_writer() -> LineWriter {
    static WRITER: OnceLock<LineWriter> = OnceLock::new();
    WRITER.get_or_init(|| Default::default()).clone()
}

pub(crate) async fn submit_logs(client: &TraceClient) -> anyhow::Result<()> {
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

    pub async fn submit(&self, client: &TraceClient) -> anyhow::Result<()> {
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
