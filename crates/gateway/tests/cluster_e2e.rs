//! M4c process e2e: FIX client → `fix-gateway --cluster` → deepmsg cluster →
//! `engine-container`, a cluster snapshot, `kill -9` of the container, a
//! restart, and a book that continues where it stopped.
//!
//! ```text
//! cargo build -p deepbook-engine-cluster --bin engine-container
//! cargo test -p deepbook-gateway --test cluster_e2e -- --ignored --nocapture
//! ```
//!
//! The test is `#[ignore]` because it starts a media driver, a consensus
//! module and two service/client processes. It skips (does not fail) when the
//! deepmsg binaries named by `DEEPBOOK_DEEPMSG_DIR` are not built; everything
//! else is a failure with the child's log tail.
//!
//! The topology is one node, IPC:
//!
//! ```text
//! deepmsg-archiving-media-driver   media driver + archive
//! deepmsg-cluster-node             the single member
//! engine-container                 deepbook's ClusteredService
//! fix-gateway --cluster            the process under test
//! ```
//!
//! A second, raw cluster client sends `Command::Snapshot` and reads the
//! `Event::Snapshot` book hash, so the recovery check compares engine state
//! rather than a FIX report.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::fs::File;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command as Process, Stdio};
use std::rc::Rc;
use std::time::{Duration, Instant};

use deepbook_engine_core::{Command as EngineCommand, Event, decode_event, encode_command};
use deepbook_fix::codec::{MessageBuilder, MsgType, ParsedMessage, tag};
use deepbook_fix::message::{ApplicationMessage, ExecType, OrdStatus, decode_application};
use deepbook_gateway::frame::FrameDecoder;
use deepmsg_client::client::Client;
use deepmsg_client::fragment_assembler::MessageHeader;
use deepmsg_cluster_client::{AeronCluster, AeronClusterContext, EgressListener};
use deepmsg_cluster_common::counters::{
    CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID, CLUSTER_SNAPSHOT_COUNTER_TYPE_ID, find_cluster_counter,
};
use deepmsg_cnc::file::CncFile;
use deepmsg_core::logbuffer::append::Appended;

const CLUSTER_ID: i32 = 7;
/// `ModuleState.ACTIVE`.
const MODULE_ACTIVE: i64 = 1;
/// The container mark file calls a file active for ten seconds.
const MARK_FILE_WAIT: Duration = Duration::from_secs(11);
const READY_TIMEOUT: Duration = Duration::from_secs(20);
const FIX_TIMEOUT: Duration = Duration::from_secs(25);

fn recovered_book(backend: &str, default_base: u16) {
    let Some(mut harness) = Harness::start(backend, default_base) else {
        eprintln!("SKIPPED: the deepmsg or engine-container binaries are not built");
        return;
    };

    let mut fix = harness.connect_fix();
    logon(&mut fix);
    fix.read_until(MsgType::Logon);

    // A resting buy, then a sell that crosses 40 of it.
    new_order(&mut fix, "C-1", "1", b"1", 100, "100.25");
    let accepted = execution_report(fix.read_application());
    assert_eq!(ExecType::New, accepted.exec_type);
    assert_eq!(OrdStatus::New, accepted.ord_status);
    assert_eq!("C-1", accepted.cl_ord_id);
    assert_eq!(100, accepted.leaves_qty);
    assert_eq!(Some("1".to_owned()), accepted.account);

    new_order(&mut fix, "C-2", "2", b"2", 40, "100.25");
    // The engine acknowledges the accepted order before the fills.
    let acknowledgment = execution_report(fix.read_application());
    assert_eq!("C-2", acknowledgment.cl_ord_id);
    assert_eq!(ExecType::New, acknowledgment.exec_type);
    assert_eq!(40, acknowledgment.leaves_qty);
    let first = execution_report(fix.read_application());
    let second = execution_report(fix.read_application());
    let (taker, maker) = if first.cl_ord_id == "C-2" {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!("C-2", taker.cl_ord_id);
    assert_eq!(ExecType::Trade, taker.exec_type);
    assert_eq!(OrdStatus::Filled, taker.ord_status);
    assert_eq!(40, taker.cum_qty);
    assert_eq!(Some(40), taker.last_qty);
    assert_eq!(Some(10_025), taker.last_px);
    assert_eq!("C-1", maker.cl_ord_id);
    assert_eq!(ExecType::Trade, maker.exec_type);
    assert_eq!(OrdStatus::PartiallyFilled, maker.ord_status);
    assert_eq!(40, maker.cum_qty);
    assert_eq!(60, maker.leaves_qty);

    // The cancel path, and its reject path, before the recovery.
    new_order(&mut fix, "C-3", "1", b"1", 10, "99.00");
    assert_eq!(
        ExecType::New,
        execution_report(fix.read_application()).exec_type
    );
    cancel(&mut fix, "C-4", "C-3");
    let canceled = execution_report(fix.read_application());
    assert_eq!(ExecType::Canceled, canceled.exec_type);
    assert_eq!(OrdStatus::Canceled, canceled.ord_status);
    cancel(&mut fix, "C-5", "NOPE");
    match fix.read_application() {
        ApplicationMessage::OrderCancelReject(reject) => {
            assert_eq!("C-5", reject.cl_ord_id);
            assert_eq!("NOPE", reject.orig_cl_ord_id);
        }
        other => panic!("expected an OrderCancelReject, got {other:?}"),
    }

    // The engine's own view of the book, through a second cluster session.
    let mut raw = RawClient::connect(&harness.aeron).expect("raw cluster client");
    let before = raw.snapshot().expect("snapshot before the restart");

    // A cluster snapshot gives the restarted container a recovery point.
    harness.snapshot_cluster().expect("cluster snapshot");

    // kill -9 the engine's host: the service container and the consensus
    // module. A container alone cannot rejoin in this deepmsg revision: the
    // recovery-state counter that `ClusteredServiceAgent::recover_state` reads
    // is allocated by the module at *its* startup, not when a service rejoins
    // (`tests/src/cluster.rs`: "the recovery-state counter is allocated once,
    // at that startup"). The archive, the log and the snapshot stay on disk.
    drop(raw);
    harness.kill_container();
    harness.kill_node();
    // The FIX session's inbound-silence timer is 30 s; the restart waits 11 s
    // for the mark files. Heartbeats keep the peer from sending TestRequest.
    let deadline = Instant::now() + MARK_FILE_WAIT;
    while Instant::now() < deadline {
        fix.keep_alive();
        std::thread::sleep(Duration::from_secs(5));
    }
    harness.start_node().expect("restart the module");
    std::thread::sleep(Duration::from_millis(200));
    harness.start_container().expect("restart the container");

    // A fresh observer session: the old module's sessions died with it. What
    // the recovery check compares is the engine state, not a session.
    let mut recovered =
        RawClient::connect_with_retry(&harness.aeron, READY_TIMEOUT).expect("raw client again");
    let after = recovered.snapshot().expect("snapshot after the restart");
    assert_eq!(
        before.book_hash, after.book_hash,
        "the restored book hash differs: before={before:?} after={after:?}"
    );
    assert_eq!(before.md_seq, after.md_seq);
    assert!(after.last_log_pos >= before.last_log_pos);

    // The same FIX session, and the same book: the remaining 60 fills.
    new_order(&mut fix, "C-6", "2", b"2", 60, "100.25");
    let acknowledgment = execution_report(fix.read_application());
    assert_eq!("C-6", acknowledgment.cl_ord_id);
    assert_eq!(ExecType::New, acknowledgment.exec_type);
    let first = execution_report(fix.read_application());
    let second = execution_report(fix.read_application());
    let (taker, maker) = if first.cl_ord_id == "C-6" {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!("C-6", taker.cl_ord_id);
    assert_eq!(ExecType::Trade, taker.exec_type);
    assert_eq!(OrdStatus::Filled, taker.ord_status);
    assert_eq!(60, taker.cum_qty);
    assert_eq!("C-1", maker.cl_ord_id);
    assert_eq!(OrdStatus::Filled, maker.ord_status);
    assert_eq!(100, maker.cum_qty);
    assert_eq!(0, maker.leaves_qty);

    // The gateway's projection answers the status from the last report.
    status(&mut fix, "C-1");
    let reported = execution_report(fix.read_application());
    assert_eq!(ExecType::OrderStatus, reported.exec_type);
    assert_eq!(OrdStatus::Filled, reported.ord_status);
    assert_eq!("C-1", reported.cl_ord_id);

    logout(&mut fix);
    fix.read_until(MsgType::Logout);

    // A second snapshot after the continuation: the book is empty, so the hash
    // is the empty book's, not the pre-kill one.
    let final_state = recovered.snapshot().expect("final snapshot");
    assert_ne!(before.book_hash, final_state.book_hash);

    drop(fix);
    drop(recovered);
    harness.shutdown();
}

// -- the child processes -----------------------------------------------------

struct Harness {
    root: PathBuf,
    aeron: PathBuf,
    cluster: PathBuf,
    logs: PathBuf,
    node_args: Vec<String>,
    driver: Option<Child>,
    node: Option<Child>,
    container: Option<Child>,
    gateway: Option<Child>,
    gateway_addr: SocketAddr,
    generation: u32,
    backend: String,
}

impl Harness {
    fn start(backend: &str, default_base: u16) -> Option<Self> {
        let deepmsg = deepmsg_dir();
        let profile = std::env::var("DEEPMSG_PROFILE").unwrap_or_else(|_| "release".to_owned());
        let binary = |name: &str| deepmsg.join("target").join(&profile).join(name);
        let driver = binary("deepmsg-archiving-media-driver");
        let node = binary("deepmsg-cluster-node");
        let tool = binary("deepmsg-cluster-tool");
        let container = engine_container_binary();
        if !driver.is_file() || !node.is_file() || !tool.is_file() || container.is_none() {
            eprintln!(
                "missing deepmsg binaries under {} (build them; see crates/spike/README.md)",
                deepmsg.display()
            );
            return None;
        }

        let base: u16 = std::env::var("E2E_BASE_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(default_base);
        let root = std::env::var("E2E_RUN_DIR").map_or_else(
            |_| {
                let nonce = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |duration| duration.as_nanos());
                std::env::temp_dir().join(format!(
                    "deepbook-engine-e2e-{}-{base}-{nonce}",
                    std::process::id()
                ))
            },
            PathBuf::from,
        );
        let _ = std::fs::remove_dir_all(&root);
        let aeron = root.join("aeron-0");
        let archive = root.join("archive");
        let cluster = root.join("cluster");
        let logs = root.join("logs");
        for directory in [
            &aeron,
            &archive,
            &cluster,
            &logs,
            &root.join("fix-sessions"),
        ] {
            std::fs::create_dir_all(directory).expect("run directory");
        }

        let members = format!(
            "0,localhost:{base},localhost:{},localhost:{},localhost:0,localhost:{}",
            base + 10,
            base + 20,
            base + 40
        );

        let driver_log = logs.join("driver.log");
        let mut driver =
            spawn(&driver, &driver_args(&aeron, &archive, base), &driver_log).expect("driver");
        wait_for_driver(&aeron, &driver_log, &mut driver).expect("driver ready");

        let node_args = cluster_args(&aeron, &cluster, &members);

        let mut harness = Self {
            aeron,
            cluster,
            logs,
            node_args,
            driver: Some(driver),
            node: None,
            container: None,
            gateway: None,
            gateway_addr: "127.0.0.1:0".parse().expect("address"),
            generation: 0,
            backend: backend.to_owned(),
            root,
        };
        harness.start_node().expect("cluster node");
        // The module waits for its service's JoinLog ack before it can become
        // ACTIVE, so the container is started before the wait.
        std::thread::sleep(Duration::from_millis(200));
        harness.start_container().expect("service container");
        harness.wait_until_active().expect("the module is active");
        harness.start_gateway(base + 50).expect("gateway");
        Some(harness)
    }

    fn start_node(&mut self) -> Result<(), String> {
        let log = self.logs.join(format!("node-{}.log", self.generation));
        let deepmsg = deepmsg_dir();
        let profile = std::env::var("DEEPMSG_PROFILE").unwrap_or_else(|_| "release".to_owned());
        let binary = deepmsg
            .join("target")
            .join(&profile)
            .join("deepmsg-cluster-node");
        let child = spawn(&binary, &self.node_args, &log)?;
        self.node = Some(child);
        Ok(())
    }

    fn kill_node(&mut self) {
        if let Some(mut child) = self.node.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn start_container(&mut self) -> Result<(), String> {
        let log = self.logs.join(format!("container-{}.log", self.generation));
        self.generation += 1;
        let mut args = self.node_args.clone();
        args.push("-Ddeepbook.engine.symbol=1".to_owned());
        args.push("-Ddeepbook.engine.max-orders=1024".to_owned());
        args.push("-Ddeepbook.engine.recent-window=1024".to_owned());
        let binary =
            engine_container_binary().ok_or_else(|| "engine-container is not built".to_owned())?;
        let child = spawn(&binary, &args, &log)?;
        self.container = Some(child);
        let deadline = Instant::now() + READY_TIMEOUT;
        while Instant::now() < deadline {
            if let Some(status) = self
                .container
                .as_mut()
                .and_then(|child| child.try_wait().ok().flatten())
            {
                return Err(format!(
                    "the container exited while starting ({status}):\n{}",
                    tail(&log)
                ));
            }
            if std::fs::read_to_string(&log)
                .is_ok_and(|text| text.contains("STARTED service=deepbook-engine"))
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(format!(
            "the container did not start in {READY_TIMEOUT:?}:\n{}",
            tail(&log)
        ))
    }

    fn kill_container(&mut self) {
        if let Some(mut child) = self.container.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn start_gateway(&mut self, port: u16) -> Result<(), String> {
        let address = format!("127.0.0.1:{port}");
        self.gateway_addr = address.parse().map_err(|_| "address".to_owned())?;
        let log = self.logs.join("gateway.log");
        let binary = Path::new(env!("CARGO_BIN_EXE_fix-gateway"));
        let args = vec![
            "--bind".to_owned(),
            address,
            "--session-dir".to_owned(),
            self.root.join("fix-sessions").display().to_string(),
            "--cluster".to_owned(),
            "--aeron-dir".to_owned(),
            self.aeron.display().to_string(),
            "--symbol".to_owned(),
            "AAPL".to_owned(),
            "--symbol-id".to_owned(),
            "1".to_owned(),
            "--backend".to_owned(),
            self.backend.clone(),
            "--new-leader-timeout-ms".to_owned(),
            "4000".to_owned(),
        ];
        self.gateway = Some(spawn(binary, &args, &log)?);

        let deadline = Instant::now() + READY_TIMEOUT;
        while Instant::now() < deadline {
            if let Some(status) = self
                .gateway
                .as_mut()
                .and_then(|child| child.try_wait().ok().flatten())
            {
                return Err(format!(
                    "the gateway exited while starting ({status}):\n{}",
                    tail(&log)
                ));
            }
            if TcpStream::connect(self.gateway_addr).is_ok() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Err(format!(
            "the gateway did not accept a connection in {READY_TIMEOUT:?}:\n{}",
            tail(&log)
        ))
    }

    fn connect_fix(&self) -> FixClient {
        FixClient::connect(self.gateway_addr)
    }

    fn counter(&self, type_id: i32) -> Option<i64> {
        let cnc = CncFile::open(&self.aeron, Duration::from_secs(1)).ok()?;
        let counters = cnc.counters()?;
        let id = find_cluster_counter(&counters, type_id, CLUSTER_ID)?;
        counters.value(id)
    }

    fn wait_until_active(&mut self) -> Result<(), String> {
        let deadline = Instant::now() + READY_TIMEOUT;
        let mut last = None;
        while Instant::now() < deadline {
            last = self.counter(CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID);
            if last == Some(MODULE_ACTIVE) {
                return Ok(());
            }
            if self
                .node
                .as_mut()
                .and_then(|child| child.try_wait().ok().flatten())
                .is_some()
            {
                return Err(format!(
                    "the cluster node exited:\n{}",
                    tail(&self.logs.join("node.log"))
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(format!(
            "the module did not become ACTIVE in {READY_TIMEOUT:?} (last={last:?}):\n{}",
            tail(&self.logs.join("node.log"))
        ))
    }

    fn snapshot_cluster(&self) -> Result<(), String> {
        let deepmsg = deepmsg_dir();
        let profile = std::env::var("DEEPMSG_PROFILE").unwrap_or_else(|_| "release".to_owned());
        let tool = deepmsg
            .join("target")
            .join(&profile)
            .join("deepmsg-cluster-tool");
        let output = Process::new(&tool)
            .arg(format!("-Daeron.dir={}", self.aeron.display()))
            .arg(format!("-Daeron.cluster.dir={}", self.cluster.display()))
            .arg(&self.cluster)
            .arg("snapshot")
            .output()
            .map_err(|error| format!("{}: {error}", tool.display()))?;
        if !output.status.success() {
            return Err(format!(
                "cluster tool snapshot failed ({}):\nstdout:\n{}\nstderr:\n{}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if self
                .counter(CLUSTER_SNAPSHOT_COUNTER_TYPE_ID)
                .is_some_and(|count| count > 0)
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err("the module did not count a snapshot in 20 s".to_owned())
    }

    fn shutdown(&mut self) {
        for slot in [
            &mut self.gateway,
            &mut self.container,
            &mut self.node,
            &mut self.driver,
        ] {
            if let Some(mut child) = slot.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        if std::env::var("E2E_KEEP").is_err() {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn deepmsg_dir() -> PathBuf {
    std::env::var("DEEPBOOK_DEEPMSG_DIR").map_or_else(
        |_| Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../deepmsg"),
        PathBuf::from,
    )
}

fn engine_container_binary() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("DEEPBOOK_ENGINE_CONTAINER") {
        let path = PathBuf::from(path);
        return path.is_file().then_some(path);
    }
    let gateway = Path::new(env!("CARGO_BIN_EXE_fix-gateway"));
    let candidate =
        gateway.with_file_name(format!("engine-container{}", std::env::consts::EXE_SUFFIX));
    candidate.is_file().then_some(candidate)
}

fn driver_args(aeron: &Path, archive: &Path, base: u16) -> Vec<String> {
    vec![
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
            base + 40
        ),
        "-Daeron.archive.control.response.stream.id=20".to_owned(),
        "-Daeron.archive.local.control.channel=aeron:ipc?term-length=64k".to_owned(),
        "-Daeron.archive.local.control.stream.id=10".to_owned(),
        "-Daeron.conductor.idle.strategy=spin".to_owned(),
        "-Daeron.sender.idle.strategy=spin".to_owned(),
        "-Daeron.receiver.idle.strategy=spin".to_owned(),
    ]
}

fn cluster_args(aeron: &Path, cluster: &Path, members: &str) -> Vec<String> {
    vec![
        format!("-Daeron.dir={}", aeron.display()),
        format!("-Daeron.cluster.dir={}", cluster.display()),
        format!("-Daeron.cluster.id={CLUSTER_ID}"),
        "-Daeron.cluster.member.id=0".to_owned(),
        format!("-Daeron.cluster.members={members}"),
        "-Daeron.cluster.ingress.channel=aeron:ipc?term-length=64k".to_owned(),
        "-Daeron.cluster.egress.channel=aeron:ipc?term-length=64k".to_owned(),
        "-Daeron.cluster.consensus.channel=aeron:ipc?term-length=128k".to_owned(),
        "-Daeron.cluster.log.channel=aeron:ipc?term-length=512k".to_owned(),
        "-Daeron.cluster.control.channel=aeron:ipc?term-length=128k".to_owned(),
        "-Daeron.cluster.replication.channel=aeron:udp?endpoint=localhost:0".to_owned(),
        "-Daeron.cluster.leader.heartbeat.timeout=30s".to_owned(),
        "-Daeron.cluster.service.count=1".to_owned(),
        "-Daeron.cluster.service.id=0".to_owned(),
        "-Daeron.cluster.service.responder=true".to_owned(),
        "-Daeron.cluster.service.stream.id=104".to_owned(),
        "-Daeron.cluster.consensus.module.stream.id=105".to_owned(),
        "-Daeron.cluster.replay.channel=aeron:ipc".to_owned(),
        "-Daeron.cluster.replay.stream.id=103".to_owned(),
        "-Daeron.cluster.snapshot.channel=aeron:ipc?alias=snapshot".to_owned(),
        "-Daeron.cluster.snapshot.stream.id=106".to_owned(),
        "-Daeron.archive.local.control.channel=aeron:ipc?term-length=64k".to_owned(),
        "-Daeron.archive.local.control.stream.id=10".to_owned(),
        "-Daeron.archive.control.response.stream.id=20".to_owned(),
        "-Daeron.cluster.idle.strategy=org.agrona.concurrent.BusySpinIdleStrategy".to_owned(),
    ]
}

fn spawn(binary: &Path, args: &[String], log: &Path) -> Result<Child, String> {
    let stdout = File::create(log).map_err(|error| format!("{}: {error}", log.display()))?;
    let stderr = stdout
        .try_clone()
        .map_err(|error| format!("{}: {error}", log.display()))?;
    let mut command = Process::new(binary);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    // A leaked DEEPMSG_*/AERON_* value beats -D in the driver; the arguments
    // above are the only configuration a child gets.
    for (key, _) in std::env::vars() {
        if key.starts_with("DEEPMSG_") || key.starts_with("AERON_") {
            command.env_remove(key);
        }
    }
    command
        .spawn()
        .map_err(|error| format!("{}: {error}", binary.display()))
}

fn wait_for_driver(aeron: &Path, log: &Path, driver: &mut Child) -> Result<(), String> {
    let cnc = aeron.join("cnc.dat");
    let deadline = Instant::now() + READY_TIMEOUT;
    while Instant::now() < deadline {
        if let Some(status) = driver.try_wait().map_err(|error| error.to_string())? {
            return Err(format!(
                "the driver exited before it was ready ({status}):\n{}",
                tail(log)
            ));
        }
        let archive_running =
            std::fs::read_to_string(log).is_ok_and(|text| text.contains("archive running"));
        if cnc.exists() && archive_running {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Err(format!(
        "the driver did not become ready in {READY_TIMEOUT:?}:\n{}",
        tail(log)
    ))
}

fn tail(path: &Path) -> String {
    let Ok(text) = std::fs::read_to_string(path) else {
        return "<no log>".to_owned();
    };
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(40)..].join("\n")
}

// -- the raw cluster client --------------------------------------------------

/// The egress listener's only job is to copy payloads for the test thread.
struct RawListener(Rc<RefCell<VecDeque<Vec<u8>>>>);

impl EgressListener for RawListener {
    fn on_message(
        &mut self,
        _cluster_session_id: i64,
        _timestamp: i64,
        payload: &[u8],
        _header: &MessageHeader,
    ) {
        self.0.borrow_mut().push_back(payload.to_vec());
    }
}

struct RawClient {
    client: Client,
    session: AeronCluster,
    egress: Rc<RefCell<VecDeque<Vec<u8>>>>,
    last_keep_alive: Instant,
}

impl RawClient {
    fn connect(aeron: &Path) -> Result<Self, String> {
        let mut client = Client::connect(aeron).map_err(|error| error.to_string())?;
        let egress = Rc::new(RefCell::new(VecDeque::new()));
        let context = AeronClusterContext::new()
            .ingress_channel(Some("aeron:ipc"))
            .egress_channel(Some("aeron:ipc"))
            .client_name("deepbook-engine-e2e")
            .egress_listener(Box::new(RawListener(Rc::clone(&egress))));
        let session =
            AeronCluster::connect(context, &mut client).map_err(|error| error.to_string())?;
        Ok(Self {
            client,
            session,
            egress,
            last_keep_alive: Instant::now(),
        })
    }

    fn connect_with_retry(aeron: &Path, timeout: Duration) -> Result<Self, String> {
        let deadline = Instant::now() + timeout;
        loop {
            match Self::connect(aeron) {
                Ok(client) => return Ok(client),
                Err(error) => {
                    if Instant::now() >= deadline {
                        return Err(format!(
                            "the cluster did not accept a session in {timeout:?}: {error}"
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    fn drive(&mut self) {
        let _ = self.client.poll();
        if let Err(error) = self.session.poll_egress(&mut self.client) {
            panic!("raw egress: {error}");
        }
        if self.last_keep_alive.elapsed() >= Duration::from_secs(3) {
            self.session.send_keep_alive(&mut self.client);
            self.last_keep_alive = Instant::now();
        }
    }

    fn snapshot(&mut self) -> Result<deepbook_engine_core::SnapshotEvent, String> {
        let deadline = Instant::now() + FIX_TIMEOUT;
        let payload = encode_command(&EngineCommand::Snapshot);
        loop {
            if Instant::now() >= deadline {
                return Err("the cluster did not accept the snapshot command".to_owned());
            }
            match self.session.offer(&mut self.client, &payload) {
                Some(Appended::Ok { .. }) => break,
                Some(Appended::EndOfLog | Appended::MidRotation | Appended::BackPressured) => {}
                Some(other) => return Err(format!("snapshot offer: {other:?}")),
                None => {}
            }
            self.drive();
            std::thread::sleep(Duration::from_millis(1));
        }
        loop {
            if Instant::now() >= deadline {
                return Err("no Snapshot event before the deadline".to_owned());
            }
            self.drive();
            loop {
                let payload = self.egress.borrow_mut().pop_front();
                let Some(payload) = payload else {
                    break;
                };
                if let Ok(Event::Snapshot(snapshot)) = decode_event(&payload) {
                    return Ok(snapshot);
                }
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

// -- the FIX client ----------------------------------------------------------

fn message(msg_type: MsgType, seq: u64, body: &[(u32, Vec<u8>)]) -> Vec<u8> {
    let mut builder =
        MessageBuilder::new(msg_type, "CLIENT", "DEEPBOOK", seq, "20261025-12:34:56.000");
    for (tag, value) in body {
        builder = builder.body(*tag, value.clone());
    }
    builder.build()
}

fn logon(fix: &mut FixClient) {
    fix.send(
        MsgType::Logon,
        &[
            (tag::ENCRYPT_METHOD, b"0".to_vec()),
            (tag::HEART_BT_INT, b"30".to_vec()),
        ],
    );
}

fn new_order(
    fix: &mut FixClient,
    cl_ord_id: &str,
    account: &str,
    side: &[u8],
    qty: i64,
    price: &str,
) {
    fix.send(
        MsgType::NewOrderSingle,
        &[
            (tag::CL_ORD_ID, cl_ord_id.as_bytes().to_vec()),
            (tag::ACCOUNT, account.as_bytes().to_vec()),
            (tag::SYMBOL, b"AAPL".to_vec()),
            (tag::SIDE, side.to_vec()),
            (tag::ORDER_QTY, qty.to_string().into_bytes()),
            (tag::ORD_TYPE, b"2".to_vec()),
            (tag::PRICE, price.as_bytes().to_vec()),
            (tag::TIME_IN_FORCE, b"0".to_vec()),
            (tag::TRANSACT_TIME, b"20261025-12:34:56.000".to_vec()),
        ],
    );
}

fn cancel(fix: &mut FixClient, cl_ord_id: &str, orig: &str) {
    fix.send(
        MsgType::OrderCancelRequest,
        &[
            (tag::CL_ORD_ID, cl_ord_id.as_bytes().to_vec()),
            (tag::ORIG_CL_ORD_ID, orig.as_bytes().to_vec()),
            (tag::SYMBOL, b"AAPL".to_vec()),
            (tag::SIDE, b"1".to_vec()),
            (tag::ORDER_QTY, b"10".to_vec()),
            (tag::TRANSACT_TIME, b"20261025-12:34:57.000".to_vec()),
        ],
    );
}

fn status(fix: &mut FixClient, cl_ord_id: &str) {
    fix.send(
        MsgType::OrderStatusRequest,
        &[
            (tag::CL_ORD_ID, cl_ord_id.as_bytes().to_vec()),
            (tag::SYMBOL, b"AAPL".to_vec()),
            (tag::SIDE, b"1".to_vec()),
        ],
    );
}

fn logout(fix: &mut FixClient) {
    fix.send(MsgType::Logout, &[(tag::TEXT, b"bye".to_vec())]);
}

/// A FIX client with one decoder for the connection. A decoder per read would
/// drop the frames that shared a TCP segment with the frame just returned.
struct FixClient {
    stream: TcpStream,
    decoder: FrameDecoder,
    next_seq: u64,
}

impl FixClient {
    fn connect(address: SocketAddr) -> Self {
        let stream = TcpStream::connect(address).expect("connect the gateway");
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("read timeout");
        Self {
            stream,
            decoder: FrameDecoder::new(),
            next_seq: 1,
        }
    }

    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(bytes)
    }

    fn read_frame(&mut self) -> ParsedMessage {
        let mut buffer = [0_u8; 4_096];
        let deadline = Instant::now() + FIX_TIMEOUT;
        loop {
            if let Some(frame) = self.decoder.next_frame() {
                return frame.expect("a complete frame");
            }
            if Instant::now() >= deadline {
                panic!("no frame from the gateway before the deadline");
            }
            match self.stream.read(&mut buffer) {
                Ok(0) => panic!("the gateway closed before sending a frame"),
                Ok(count) => self.decoder.push(&buffer[..count]),
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut => {}
                Err(error) => panic!("read: {error}"),
            }
        }
    }

    fn send(&mut self, msg_type: MsgType, body: &[(u32, Vec<u8>)]) {
        let raw = message(msg_type, self.next_seq, body);
        self.next_seq += 1;
        self.write_all(&raw).expect("send");
    }

    /// One inbound message the gateway counts as the peer being alive.
    fn keep_alive(&mut self) {
        self.send(MsgType::Heartbeat, &[]);
    }

    /// Answer a TestRequest the way a FIX engine does, so a long wait for the
    /// cluster does not end the session.
    fn answer_test_request(&mut self, frame: &ParsedMessage) {
        if let Ok(deepbook_fix::SessionMessage::TestRequest { test_req_id }) =
            deepbook_fix::decode_session(frame)
        {
            self.send(
                MsgType::Heartbeat,
                &[(tag::TEST_REQ_ID, test_req_id.into_bytes())],
            );
        }
    }

    /// Read until `expected`, skipping heartbeats and answering TestRequests.
    fn read_until(&mut self, expected: MsgType) -> ParsedMessage {
        loop {
            let frame = self.read_frame();
            let message_type = frame.msg_type().expect("a message type");
            if message_type == MsgType::TestRequest {
                self.answer_test_request(&frame);
                continue;
            }
            if message_type == MsgType::Heartbeat {
                continue;
            }
            assert_eq!(expected, message_type, "unexpected session message");
            return frame;
        }
    }

    /// Skip session-level traffic and return the next application message.
    fn read_application(&mut self) -> ApplicationMessage {
        loop {
            let frame = self.read_frame();
            let message_type = frame.msg_type().expect("a message type");
            if message_type == MsgType::TestRequest {
                self.answer_test_request(&frame);
                continue;
            }
            if message_type == MsgType::Heartbeat {
                continue;
            }
            assert!(
                !message_type.is_admin(),
                "the session ended while waiting for a report: {message_type:?}"
            );
            return decode_application(&frame).expect("an application message");
        }
    }
}

fn execution_report(message: ApplicationMessage) -> deepbook_fix::ExecutionReport {
    match message {
        ApplicationMessage::ExecutionReport(report) => report,
        other => panic!("expected an ExecutionReport, got {other:?}"),
    }
}

#[test]
#[ignore = "starts a media driver, a cluster node and a service container"]
fn a_recovered_engine_continues_the_book() {
    recovered_book("epoll", 25_900);
}

#[test]
#[ignore = "starts a media driver, a cluster node and a service container"]
fn a_recovered_engine_continues_the_book_over_uring() {
    recovered_book("uring", 26_100);
}
