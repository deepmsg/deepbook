//! The three deepmsg processes, their properties, readiness and restart.
//!
//! The topology is the one a single-node deployment runs:
//!
//! ```text
//! deepmsg-archiving-media-driver   media driver + archive
//! deepmsg-cluster-node             consensus module
//! deepmsg-clustered-service-container  the business (stock `basic-auction`)
//! ```
//!
//! The client-facing ingress/egress and the cluster's log/consensus channels
//! are IPC: all four processes share one driver on one machine. The archive's
//! control channel is UDP on the member string's archive endpoint, which is how
//! the consensus module finds the archive.
//!
//! A restart uses a **new aeron directory** and keeps the archive and cluster
//! directories: the CnC and the driver's shared memory are ephemeral, while
//! `recording.log`, the node state and the archive are the recovery inputs.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use deepmsg_cluster_common::counters::{CLUSTER_SNAPSHOT_COUNTER_TYPE_ID, find_cluster_counter};
use deepmsg_cnc::file::CncFile;

use crate::config::{Config, Paths, Ports};

pub(crate) const READY_TIMEOUT: Duration = Duration::from_secs(20);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// One single-node cluster, owned as child processes.
pub struct Processes {
    config: Config,
    paths: Paths,
    ports: Ports,
    generation: u32,
    driver: Option<Child>,
    node: Option<Child>,
    container: Option<Child>,
}

impl Processes {
    /// Start the driver, the module and the container.
    ///
    /// # Errors
    ///
    /// A missing binary, a filesystem error, or a process that ends while it
    /// was coming up.
    pub fn start(config: Config) -> Result<Self, String> {
        config.check_binaries()?;
        let paths = Paths::new(&config.run_root)?;
        let ports = Ports::new(config.base_port);
        let mut processes = Self {
            config,
            paths,
            ports,
            generation: 0,
            driver: None,
            node: None,
            container: None,
        };
        processes.start_all()?;
        Ok(processes)
    }

    /// The aeron directory the client should connect to.
    #[must_use]
    pub fn aeron_dir(&self) -> PathBuf {
        self.paths.aeron(self.generation)
    }

    /// The cluster directory, for `deepmsg-cluster-tool`.
    #[must_use]
    pub fn cluster_dir(&self) -> &Path {
        &self.paths.cluster
    }

    /// The run's root, kept on failure and printed by the CLI.
    #[must_use]
    pub fn run_dir(&self) -> &Path {
        &self.paths.root
    }

    /// Ask the cluster to take a snapshot through the operator tool.
    ///
    /// # Errors
    ///
    /// The tool is missing, cannot be run, or exits non-zero.
    pub fn snapshot(&self) -> Result<(), String> {
        let tool = self.config.binary("deepmsg-cluster-tool");
        let output = Command::new(&tool)
            .arg(d("aeron.dir", self.aeron_dir().display()))
            .arg(d("aeron.cluster.dir", self.cluster_dir().display()))
            .arg(self.cluster_dir())
            .arg("snapshot")
            .output()
            .map_err(|error| format!("{}: {error}", tool.display()))?;

        if output.status.success() {
            return Ok(());
        }
        Err(format!(
            "cluster tool snapshot failed ({}):\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ))
    }

    /// The module's snapshot counter (`205`), the signal that the snapshot the
    /// toggle asked for has been taken.
    #[must_use]
    pub fn snapshot_count(&self) -> Option<i64> {
        let cnc = CncFile::open(&self.aeron_dir(), Duration::from_secs(5)).ok()?;
        let counters = cnc.counters()?;
        let id = find_cluster_counter(
            &counters,
            CLUSTER_SNAPSHOT_COUNTER_TYPE_ID,
            self.config.cluster_id,
        )?;
        counters.value(id)
    }

    /// Wait until the module reports a snapshot, or the deadline passes.
    ///
    /// # Errors
    ///
    /// The timeout, with the last counter reading.
    pub fn wait_for_snapshot(&self, timeout: Duration) -> Result<i64, String> {
        let deadline = Instant::now() + timeout;
        let mut last = None;
        while Instant::now() < deadline {
            last = self.snapshot_count();
            if last.is_some_and(|count| count > 0) {
                return Ok(last.expect("just checked"));
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Err(format!(
            "the module did not count a snapshot in {timeout:?} (last={last:?})"
        ))
    }

    /// `kill -9` every process, then come back on the same archive and cluster
    /// directories with a fresh aeron directory.
    ///
    /// # Errors
    ///
    /// A process that will not start again.
    pub fn restart(&mut self) -> Result<(), String> {
        self.kill_all();
        // A SIGKILL leaves the cluster mark file's activity timestamp recent.
        // The next module and container refuse to start with "mark file is
        // active" until the mark file's liveness timeout has passed, so the
        // restart waits it out rather than reporting a connect timeout.
        std::thread::sleep(Duration::from_secs(10));
        self.generation += 1;
        self.start_all()
    }

    /// Stop every process with `SIGKILL`. Used by `restart` and by `Drop`.
    pub fn kill_all(&mut self) {
        for slot in [&mut self.container, &mut self.node, &mut self.driver] {
            if let Some(mut child) = slot.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
        // Let the OS release the UDP endpoints and the shared memory before the
        // next generation binds them.
        std::thread::sleep(Duration::from_millis(500));
    }

    fn start_all(&mut self) -> Result<(), String> {
        let aeron = self.aeron_dir();
        std::fs::create_dir_all(&aeron)
            .map_err(|error| format!("create {}: {error}", aeron.display()))?;

        let driver_log = self.logs().join(format!("driver-{}.log", self.generation));
        let driver = self.config.binary("deepmsg-archiving-media-driver");
        let driver = spawn(&driver, &self.driver_args(&aeron), &driver_log)?;
        self.driver = Some(driver);
        self.wait_for_driver(&aeron, &driver_log)?;

        let node_log = self.logs().join(format!("node-{}.log", self.generation));
        let node = self.config.binary("deepmsg-cluster-node");
        let node = spawn(&node, &self.node_args(&aeron), &node_log)?;
        self.node = Some(node);
        std::thread::sleep(Duration::from_millis(200));

        let container_log = self
            .logs()
            .join(format!("container-{}.log", self.generation));
        let container = self.config.binary("deepmsg-clustered-service-container");
        let container = spawn(&container, &self.container_args(&aeron), &container_log)?;
        self.container = Some(container);

        self.ensure_children_alive(&node_log, &container_log)
    }

    /// A module or container that exits while it is joining is a startup
    /// failure, and its log says why. Without this check the next symptom is a
    /// client connect timeout, which hides the cause.
    fn ensure_children_alive(
        &mut self,
        node_log: &Path,
        container_log: &Path,
    ) -> Result<(), String> {
        std::thread::sleep(Duration::from_millis(500));
        for (name, slot, log) in [
            ("node", self.node.as_mut(), node_log),
            ("container", self.container.as_mut(), container_log),
        ] {
            if let Some(child) = slot {
                if let Some(status) = child
                    .try_wait()
                    .map_err(|error| format!("{name} wait: {error}"))?
                {
                    return Err(format!(
                        "the {name} exited while starting ({status}):\n{}",
                        tail(log)
                    ));
                }
            }
        }
        Ok(())
    }

    fn wait_for_driver(&mut self, aeron: &Path, log: &Path) -> Result<(), String> {
        let driver = self
            .driver
            .as_mut()
            .ok_or_else(|| "the driver was not started".to_owned())?;
        wait_for_driver_ready(aeron, log, driver)
    }

    fn logs(&self) -> &Path {
        &self.paths.logs
    }

    fn driver_args(&self, aeron: &Path) -> Vec<String> {
        let mut args = vec![
            // The driver's native name is `deepmsg.dir`; `aeron.dir` is the
            // reference's alias. Pass both, and never inherit the environment:
            // `DEEPMSG_DIR` beats a `-D`, and a leaked value once pointed the
            // driver at a source checkout.
            d("deepmsg.dir", aeron.display()),
            d("aeron.dir", aeron.display()),
            d("deepmsg.dir.delete.on.start", false),
            d("aeron.dir.delete.on.start", false),
            d("deepmsg.dir.delete.on.shutdown", false),
            d("aeron.dir.delete.on.shutdown", false),
            d("aeron.archive.dir", self.paths.archive.display()),
            d("aeron.threading.mode", "SHARED"),
            d("aeron.archive.threading.mode", "SHARED"),
            d(
                "aeron.archive.control.channel",
                format!("aeron:udp?endpoint=localhost:{}", self.ports.archive()),
            ),
            d("aeron.archive.control.response.stream.id", 20),
            d(
                "aeron.archive.local.control.channel",
                "aeron:ipc?term-length=64k",
            ),
            d("aeron.archive.local.control.stream.id", 10),
            d("aeron.conductor.idle.strategy", "spin"),
            d("aeron.sender.idle.strategy", "spin"),
            d("aeron.receiver.idle.strategy", "spin"),
        ];
        if self.config.durable {
            args.push(d("aeron.archive.file.sync.level", 2));
            args.push(d("aeron.archive.catalog.file.sync.level", 2));
        }
        args
    }

    fn node_args(&self, aeron: &Path) -> Vec<String> {
        vec![
            d("aeron.dir", aeron.display()),
            d("aeron.cluster.dir", self.cluster_dir().display()),
            d("aeron.cluster.id", self.config.cluster_id),
            d("aeron.cluster.member.id", 0),
            d("aeron.cluster.members", self.ports.member_string()),
            d("aeron.cluster.ingress.channel", "aeron:ipc?term-length=64k"),
            d("aeron.cluster.egress.channel", "aeron:ipc?term-length=64k"),
            d(
                "aeron.cluster.consensus.channel",
                "aeron:ipc?term-length=128k",
            ),
            d("aeron.cluster.log.channel", "aeron:ipc?term-length=512k"),
            d(
                "aeron.cluster.control.channel",
                "aeron:ipc?term-length=128k",
            ),
            d(
                "aeron.cluster.replication.channel",
                "aeron:udp?endpoint=localhost:0",
            ),
            d("aeron.cluster.leader.heartbeat.timeout", "30s"),
            d("aeron.cluster.service.count", 1),
            d("aeron.cluster.service.id", 0),
            d("aeron.cluster.service.responder", "true"),
            d("aeron.cluster.service.stream.id", 104),
            d("aeron.cluster.consensus.module.stream.id", 105),
            d("aeron.cluster.replay.channel", "aeron:ipc"),
            d("aeron.cluster.replay.stream.id", 103),
            d("aeron.cluster.snapshot.channel", "aeron:ipc?alias=snapshot"),
            d("aeron.cluster.snapshot.stream.id", 106),
            d(
                "aeron.archive.local.control.channel",
                "aeron:ipc?term-length=64k",
            ),
            d("aeron.archive.local.control.stream.id", 10),
            d("aeron.archive.control.response.stream.id", 20),
            d(
                "aeron.cluster.idle.strategy",
                "org.agrona.concurrent.BusySpinIdleStrategy",
            ),
        ]
    }

    fn container_args(&self, aeron: &Path) -> Vec<String> {
        let mut args = self.node_args(aeron);
        args.push(d("deepmsg.cluster.service", "basic-auction"));
        args
    }
}

impl Drop for Processes {
    fn drop(&mut self) {
        self.kill_all();
        if !self.config.keep {
            let _ = std::fs::remove_dir_all(&self.paths.root);
        }
    }
}

/// The client's connect budget. Kept here so a scenario does not invent its own.
#[must_use]
pub const fn connect_timeout() -> Duration {
    CONNECT_TIMEOUT
}

pub(crate) fn d(name: &str, value: impl std::fmt::Display) -> String {
    format!("-D{name}={value}")
}

pub(crate) fn spawn(binary: &Path, args: &[String], log: &Path) -> Result<Child, String> {
    let stdout = File::create(log).map_err(|error| format!("{}: {error}", log.display()))?;
    let stderr = stdout
        .try_clone()
        .map_err(|error| format!("{}: {error}", log.display()))?;

    let mut command = Command::new(binary);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));

    // The driver reads its configuration from `DEEPMSG_*` and the reference's
    // `AERON_*` environment as well as from `-D` arguments, and a deepmsg
    // environment name beats the reference's `-D` name. A parent that sets
    // `DEEPMSG_DIR` to locate the binaries would therefore point the child's
    // aeron directory at the source checkout. Strip both families; the
    // explicit arguments above are the only configuration a child gets.
    for (key, _) in std::env::vars() {
        if key.starts_with("DEEPMSG_") || key.starts_with("AERON_") {
            command.env_remove(key);
        }
    }

    command
        .spawn()
        .map_err(|error| format!("{}: {error}", binary.display()))
}

pub(crate) fn tail(path: &Path) -> String {
    let Ok(text) = std::fs::read_to_string(path) else {
        return "<no log>".to_owned();
    };
    let lines: Vec<&str> = text.lines().collect();
    lines[lines.len().saturating_sub(40)..].join("\n")
}

/// Wait until the driver's CnC file exists **and** its archive is serving.
///
/// The log line, not the mark file: the mark file is persistent and exists
/// from a previous generation, so waiting for it would let a restart look
/// ready before the new archive had served anything.
pub(crate) fn wait_for_driver_ready(
    aeron: &Path,
    log: &Path,
    driver: &mut Child,
) -> Result<(), String> {
    let cnc = aeron.join("cnc.dat");
    let deadline = Instant::now() + READY_TIMEOUT;

    while Instant::now() < deadline {
        if let Some(status) = driver
            .try_wait()
            .map_err(|error| format!("driver wait: {error}"))?
        {
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

    let archive_running =
        std::fs::read_to_string(log).is_ok_and(|text| text.contains("archive running"));
    Err(format!(
        "the driver did not become ready in {READY_TIMEOUT:?} \
         (cnc={} archive_running={}):\n{}",
        cnc.exists(),
        archive_running,
        tail(log)
    ))
}
