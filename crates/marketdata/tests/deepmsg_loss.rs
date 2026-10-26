//! The M3 acceptance test: a lossy incremental stream, repaired through the
//! recovery channel.
//!
//! It starts deepmsg's **production** archiving media driver with its send-side
//! loss generator (`deepmsg.debug.send.data.loss.drop.every=4`), publishes
//! 1024-byte market-data frames (one datagram each), subscribes with
//! `reliable=false` so the holes reach the application, and lets the
//! subscriber recover. The test skips when the deepmsg checkout or the driver
//! binary is absent, so the default workspace run does not need it.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use deepbook_marketdata::{
    DeepmsgConfig, DeepmsgPublisher, DeepmsgSubscriber, Incremental, MarketDataService,
    ServiceConfig, Subscriber, WireMessage, encode,
};
use deepbook_types::Side;

const MESSAGES: u64 = 200;
const FRAME_SIZE: usize = 1024;
const DROP_EVERY: u32 = 4;

struct Driver {
    child: Child,
    root: PathBuf,
    aeron: PathBuf,
    base_port: u16,
}

impl Drop for Driver {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn deepmsg_dir() -> PathBuf {
    std::env::var("DEEPBOOK_DEEPMSG_DIR").map_or_else(
        |_| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../../deepmsg"),
        PathBuf::from,
    )
}

fn driver_binary(directory: &Path) -> Option<PathBuf> {
    ["release", "debug"]
        .into_iter()
        .map(|profile| {
            directory
                .join("target")
                .join(profile)
                .join("deepmsg-archiving-media-driver")
        })
        .find(|candidate| candidate.is_file())
}

fn start_driver() -> Option<Driver> {
    let directory = deepmsg_dir();
    let Some(binary) = driver_binary(&directory) else {
        eprintln!(
            "SKIPPED deepmsg loss test: no deepmsg-archiving-media-driver under {}",
            directory.display()
        );
        return None;
    };

    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let root = std::env::temp_dir().join(format!(
        "deepbook-marketdata-{}-{nonce}",
        std::process::id()
    ));
    let aeron = root.join("aeron");
    let archive = root.join("archive");
    let logs = root.join("logs");
    for path in [&aeron, &archive, &logs] {
        std::fs::create_dir_all(path).expect("run directory");
    }
    let log_path = logs.join("driver.log");
    let log = File::create(&log_path).expect("driver log");
    let log_err = log.try_clone().expect("driver log clone");

    let base_port = 27_000 + u16::try_from(std::process::id() % 100).expect("pid") * 50;
    let mut command = Command::new(&binary);
    command
        .args([
            format!("-Ddeepmsg.dir={}", aeron.display()),
            format!("-Daeron.dir={}", aeron.display()),
            "-Ddeepmsg.dir.delete.on.start=false".to_owned(),
            "-Daeron.dir.delete.on.start=false".to_owned(),
            "-Ddeepmsg.dir.delete.on.shutdown=false".to_owned(),
            "-Daeron.dir.delete.on.shutdown=false".to_owned(),
            format!("-Daeron.archive.dir={}", archive.display()),
            "-Daeron.threading.mode=SHARED".to_owned(),
            "-Daeron.archive.threading.mode=SHARED".to_owned(),
            format!(
                "-Daeron.archive.control.channel=aeron:udp?endpoint=localhost:{}",
                base_port + 43
            ),
            "-Daeron.archive.control.response.stream.id=20".to_owned(),
            "-Daeron.archive.local.control.channel=aeron:ipc?term-length=64k".to_owned(),
            "-Daeron.archive.local.control.stream.id=10".to_owned(),
            format!("-Ddeepmsg.debug.send.data.loss.drop.every={DROP_EVERY}"),
            "-Daeron.conductor.idle.strategy=spin".to_owned(),
            "-Daeron.sender.idle.strategy=spin".to_owned(),
            "-Daeron.receiver.idle.strategy=spin".to_owned(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    // The driver's environment names beat a `-D`; strip both families.
    for (key, _) in std::env::vars() {
        if key.starts_with("DEEPMSG_") || key.starts_with("AERON_") {
            command.env_remove(key);
        }
    }
    let mut child = command.spawn().expect("spawn the deepmsg driver");

    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(status) = child.try_wait().expect("driver wait") {
            panic!("the driver exited ({status}):\n{}", tail(&log_path));
        }
        let archive_running =
            std::fs::read_to_string(&log_path).is_ok_and(|text| text.contains("archive running"));
        if aeron.join("cnc.dat").is_file() && archive_running {
            return Some(Driver {
                child,
                root,
                aeron,
                base_port,
            });
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("the driver did not become ready:\n{}", tail(&log_path));
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn tail(path: &Path) -> String {
    let Ok(text) = std::fs::read_to_string(path) else {
        return "<no log>".to_owned();
    };
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(40)..].join("\n")
}

fn padded(incremental: Incremental) -> Vec<u8> {
    let mut frame = encode(&WireMessage::Incremental(incremental));
    frame.resize(FRAME_SIZE, 0);
    frame
}

#[test]
fn a_lossy_stream_recovers_through_the_recovery_channel() {
    let Some(driver) = start_driver() else {
        return;
    };
    let base = driver.base_port;
    let incremental = format!("aeron:udp?endpoint=127.0.0.1:{base}|term-length=64k");
    let incremental_lossy = format!("{incremental}|reliable=false");
    let request = format!("aeron:udp?endpoint=127.0.0.1:{}|term-length=64k", base + 1);
    let request_lossy = format!("{request}|reliable=false");
    let response = format!("aeron:udp?endpoint=127.0.0.1:{}|term-length=64k", base + 2);
    let response_lossy = format!("{response}|reliable=false");

    let config = DeepmsgConfig::new(
        &driver.aeron,
        incremental.clone(),
        &incremental_lossy,
        &request,
        &request_lossy,
        &response,
        &response_lossy,
    );
    let mut publisher = DeepmsgPublisher::connect(&config).expect("publisher");
    let mut subscriber_client = DeepmsgSubscriber::connect(&config).expect("subscriber");

    let mut service = MarketDataService::new(ServiceConfig {
        symbol: 1,
        recent_capacity: 128,
    });
    let mut subscriber = Subscriber::new(1);

    // Put a base snapshot in the subscriber before any incrementals. The
    // snapshot frame is small and goes on the same lossy publication, so send
    // it a few times.
    let mut based = false;
    for _ in 0..20 {
        publisher
            .publish_snapshot(&service.snapshot())
            .expect("snapshot");
        let _ = subscriber_client.poll(&mut subscriber);
        if subscriber.last_md_seq() == Some(0) {
            based = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(based, "the subscriber never received a base snapshot");

    for seq in 1..=MESSAGES {
        let side = if seq % 2 == 0 { Side::Buy } else { Side::Sell };
        let price = 100 + i64::try_from(seq % 11).expect("price");
        let qty = if seq % 5 == 0 {
            0
        } else {
            1 + i64::try_from(seq % 7).expect("qty")
        };
        let incremental = service
            .on_book_update(seq, side, price, qty)
            .expect("book update");
        publisher
            .publish_frame(&padded(incremental))
            .expect("publish incremental");
        if seq % 4 == 0 {
            let _ = publisher.poll_recovery(&service);
            let _ = subscriber_client.poll(&mut subscriber);
        }
    }

    // The incremental tail may have been lost without a later frame to expose
    // the hole; the periodic snapshot is the production answer.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_snapshot = Instant::now();
    loop {
        let _ = publisher.poll_recovery(&service);
        let _ = subscriber_client.poll(&mut subscriber);
        if subscriber.last_md_seq() == Some(service.last_md_seq()) && subscriber.is_synced() {
            break;
        }
        // The production service broadcasts a snapshot once a second; this is
        // the test's timer.
        if last_snapshot.elapsed() >= Duration::from_millis(250) {
            let _ = publisher.publish_snapshot(&service.snapshot());
            last_snapshot = Instant::now();
        }
        if Instant::now() >= deadline {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    assert_eq!(
        service.book(),
        subscriber.book(),
        "the recovered book differs (received={}, requests={}, served={})",
        subscriber_client.received_incrementals(),
        subscriber_client.recovery_requests(),
        publisher.requests_served(),
    );
    assert_eq!(Some(service.last_md_seq()), subscriber.last_md_seq());
    assert!(
        subscriber_client.received_incrementals() < usize::try_from(MESSAGES).expect("messages"),
        "the loss generator did not create an application-visible gap"
    );
    assert!(
        publisher.requests_served() > 0,
        "no recovery request reached the service"
    );
    println!(
        "loss test: received={} requests={} served={} last={}",
        subscriber_client.received_incrementals(),
        subscriber_client.recovery_requests(),
        publisher.requests_served(),
        service.last_md_seq(),
    );
}
