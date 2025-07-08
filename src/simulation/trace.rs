use std::{
    cell::RefCell,
    collections::HashMap,
    io,
    sync::{Arc, Mutex, MutexGuard, OnceLock},
};

use tracing::{
    span::{Attributes, Id},
    Subscriber,
};
use tracing_subscriber::{
    fmt::MakeWriter,
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
    util::SubscriberInitExt,
    EnvFilter, Layer,
};

pub type BucketId = Option<usize>;
pub type Buckets = Arc<Mutex<HashMap<BucketId, Vec<u8>>>>;
pub type BucketsGuard<'a> = MutexGuard<'a, HashMap<BucketId, Vec<u8>>>;

// pub(crate) fn print_bucket_stats() {
//     let buckets = get_buckets();
//     let buckets = buckets.lock().unwrap();
//     for (k, v) in buckets.iter() {
//         println!("KEY {k:?} has {}", v.len());
//     }
// }

// pub(crate) fn write_buckets() -> anyhow::Result<()> {
//     let path = std::path::PathBuf::from("./logs");
//     std::fs::create_dir_all(&path)?;
//     let buckets = get_buckets();
//     let buckets = buckets.lock().unwrap();
//     for (k, v) in buckets.iter() {
//         let filename = match k {
//             None => format!("other.log"),
//             Some(i) => format!("node-{i}.log"),
//         };
//         let path = path.join(filename);
//         std::fs::write(path, v)?;
//     }
//     Ok(())
// }

pub(crate) fn try_init_global_subscriber() {
    let print_layer = {
        let default_directive = format!("{}=debug,iroh=info", env!("CARGO_CRATE_NAME"));
        let directive = std::env::var("RUST_LOG").unwrap_or_else(|_| default_directive.to_string());
        let filter = EnvFilter::new(directive);
        tracing_subscriber::fmt::layer()
            .with_writer(|| TestWriter)
            .event_format(tracing_subscriber::fmt::format().with_line_number(true))
            .with_level(true)
            .with_filter(filter)
    };

    let buckets = get_buckets();
    let bucket_writer = BucketWriter { buckets };
    let json_layer = {
        let default_directive = format!("{}=debug,iroh=debug", env!("CARGO_CRATE_NAME"));
        let directive = std::env::var("SIM_LOG").unwrap_or_else(|_| default_directive.to_string());
        let filter = EnvFilter::new(directive);
        tracing_subscriber::fmt::layer()
            .json()
            .with_writer(bucket_writer)
            .with_filter(filter)
    };

    let registry = tracing_subscriber::registry()
        .with(SimNodeTracker)
        .with(json_layer)
        .with(print_layer);

    registry.try_init().ok();
}

pub(crate) fn get_buckets() -> Buckets {
    static BUCKETS: OnceLock<Buckets> = OnceLock::new();
    BUCKETS.get_or_init(|| Default::default()).clone()
}

pub(crate) fn clear_bucket(idx: usize) -> Option<String> {
    let buckets = get_buckets();
    let mut buckets = buckets.lock().expect("poisoned");
    let bucket = buckets.get_mut(&Some(idx))?;
    let content = std::mem::take(bucket);
    String::from_utf8(content).ok()
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

struct BucketWriter {
    buckets: Buckets,
}

impl<'a> MakeWriter<'a> for BucketWriter {
    type Writer = BucketGuardWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        let state = CURRENT_SIM_NODE.with(|tls| tls.borrow().clone());
        let node = state.map(|(_id, val)| val);
        BucketGuardWriter(node, self.buckets.lock().unwrap())
    }
}

pub struct BucketGuardWriter<'a>(BucketId, BucketsGuard<'a>);

impl<'a> BucketGuardWriter<'a> {
    fn get(&mut self) -> &mut dyn std::io::Write {
        self.1.entry(self.0).or_default()
    }
}

impl io::Write for BucketGuardWriter<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.get().write(buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.get().flush()
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[io::IoSlice<'_>]) -> io::Result<usize> {
        self.get().write_vectored(bufs)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.get().write_all(buf)
    }

    #[inline]
    fn write_fmt(&mut self, fmt: std::fmt::Arguments<'_>) -> io::Result<()> {
        self.get().write_fmt(fmt)
    }
}

thread_local! {
    static CURRENT_SIM_NODE: RefCell<Option<(Id, usize)>> = RefCell::new(None);
}

pub struct SimNodeTracker;

impl<S> Layer<S> for SimNodeTracker
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if attrs.metadata().name() == "sim-node" {
            let span = ctx.span(id).unwrap();
            let mut visitor = FindSimNode(None);
            attrs.record(&mut visitor);
            if let Some(node) = visitor.0 {
                span.extensions_mut().insert(SimNode(node));
            }
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id) {
            if let Some(sim) = span.extensions().get::<SimNode>() {
                CURRENT_SIM_NODE.with(|tls| *tls.borrow_mut() = Some((id.clone(), sim.0)));
            } else {
                let mut current = span.parent();
                while let Some(span) = current {
                    if let Some(sim) = span.extensions().get::<SimNode>() {
                        CURRENT_SIM_NODE.with(|tls| *tls.borrow_mut() = Some((id.clone(), sim.0)));
                        break;
                    }
                    current = span.parent();
                }
            }
        }
    }

    fn on_exit(&self, id: &Id, _ctx: Context<'_, S>) {
        // Clear the value on exit
        CURRENT_SIM_NODE.with(|tls| {
            let mut inner = tls.borrow_mut();
            if let Some((last_id, _)) = inner.as_mut() {
                if id == last_id {
                    *inner = None;
                }
            }
        });
    }
}

struct FindSimNode(pub Option<usize>);

impl tracing::field::Visit for FindSimNode {
    fn record_u64(&mut self, field: &tracing::field::Field, val: u64) {
        if field.name() == "idx" {
            self.0 = Some(val as usize);
        }
    }

    fn record_i64(&mut self, field: &tracing::field::Field, val: i64) {
        if field.name() == "idx" && val >= 0 {
            self.0 = Some(val as usize);
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

#[derive(Debug)]
struct SimNode(pub usize);
