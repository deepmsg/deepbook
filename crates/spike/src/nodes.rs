//! Three deepmsg nodes as child processes, for the M0 leader-kill check.
//!
//! The topology is the reference's `TestCluster.withStaticNodes(3)` at the
//! process level:
//!
//! ```text
//! node-<id>/
//!   aeron/      media driver + archive for this member
//!   archive/    the member's archive directory
//!   cluster/    the member's consensus state
//!   logs/       driver, module and container output
//! client/
//!   aeron/      a driver the client owns, so a killed node cannot take it down
//! ```
//!
//! Each member's ingress, consensus and log channels have an endpoint of their
//! own and the member string names all three; the channels are UDP because the
//! members are separate processes with separate drivers. Every member runs its
//! own consensus module **and** its own `basic-auction` container: the
//! container talks to the module over that member's local IPC control channel,
//! so a member without one cannot serve after it becomes leader.
//!
//! # Timing
//!
//! `aeron.cluster.leader.heartbeat.timeout` is **2 s**, the value the
//! reference's own `TestCluster` uses on non-Windows runners
//! (`aeron-test-support/.../TestCluster.java:169-180`). A leader's death is
//! noticed only after that timeout, so gate 4's five-second budget is a
//! property of this test configuration, not of the product's default
//! deployment, where the timeout is ten seconds.

use std::path::{Path, PathBuf};
use std::process::Child;
use std::time::{Duration, Instant};

use deepmsg_cluster_common::counters::{
    CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID, CLUSTER_ELECTION_COUNT_TYPE_ID,
    CLUSTER_ELECTION_STATE_TYPE_ID, CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID, CLUSTER_NODE_ROLE_TYPE_ID,
    CLUSTER_SESSION_TYPE_ID, find_cluster_counter,
};
use deepmsg_cnc::file::CncFile;

use crate::config::{Config, Ports};
use crate::process::{d, spawn, tail, wait_for_driver_ready};

/// The number of members gate 4 stands up.
pub const MEMBERS: usize = 3;

/// `Cluster.Role`'s follower code, as counter 201 carries it.
pub const ROLE_FOLLOWER: i64 = 0;

/// `Cluster.Role`'s leader code.
pub const ROLE_LEADER: i64 = 2;

/// `ElectionState.CLOSED`.
pub const ELECTION_CLOSED: i64 = 17;

/// `ModuleState.ACTIVE` (counter 200).
pub const MODULE_ACTIVE: i64 = 1;

/// The heartbeat timeout the reference's non-Windows test harness uses.
pub const LEADER_HEARTBEAT_TIMEOUT: &str = "2s";

/// The image liveness timeout on a **member's** driver.
///
/// The reference's default is 10 s. A failing member's log image is what
/// `prepare_for_new_leadership` waits for (`await_local_sockets_closed`) before
/// the election can canvass, so the default spends most of gate 4's budget
/// there. The client's driver keeps the default for the opposite reason (see
/// `start_client_driver`).
const MEMBER_IMAGE_LIVENESS: &str = "500ms";

const CHILD_SETTLE: Duration = Duration::from_millis(500);
const COUNTER_TIMEOUT: Duration = Duration::from_secs(1);

/// The four directories one member (or the client) owns.
struct DirSet {
    aeron: PathBuf,
    archive: PathBuf,
    cluster: PathBuf,
    logs: PathBuf,
}

impl DirSet {
    fn create(root: &Path) -> Result<Self, String> {
        let dirs = Self {
            aeron: root.join("aeron"),
            archive: root.join("archive"),
            cluster: root.join("cluster"),
            logs: root.join("logs"),
        };
        for directory in [&dirs.aeron, &dirs.archive, &dirs.cluster, &dirs.logs] {
            std::fs::create_dir_all(directory)
                .map_err(|error| format!("create {}: {error}", directory.display()))?;
        }
        Ok(dirs)
    }
}

/// One member's three processes.
struct Node {
    id: usize,
    dirs: DirSet,
    driver: Option<Child>,
    module: Option<Child>,
    container: Option<Child>,
}

impl Node {
    /// A member that has not been killed: its driver is still owned.
    fn is_live(&self) -> bool {
        self.driver.is_some()
    }

    /// `SIGKILL` the member's container, module and driver, in that order.
    ///
    /// The handles are dropped, so a later [`ClusterNodes::kill_all`] leaves
    /// the killed member alone and the counters stop being read from it.
    fn kill(&mut self) {
        for slot in [&mut self.container, &mut self.module, &mut self.driver] {
            if let Some(mut child) = slot.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

/// Three members and the client's own driver, owned as child processes.
pub struct ClusterNodes {
    config: Config,
    root: PathBuf,
    ports: Ports,
    members: String,
    nodes: Vec<Node>,
    client_dirs: DirSet,
    client_driver: Option<Child>,
}

impl ClusterNodes {
    /// Create the run directories and start every process.
    ///
    /// # Errors
    ///
    /// A missing binary, a filesystem error, or a process that ends while it
    /// was coming up.
    pub fn start(config: Config) -> Result<Self, String> {
        config.check_binaries()?;
        let root = config.run_root.clone();
        if root.exists() {
            std::fs::remove_dir_all(&root)
                .map_err(|error| format!("remove {}: {error}", root.display()))?;
        }
        std::fs::create_dir_all(&root)
            .map_err(|error| format!("create {}: {error}", root.display()))?;

        let ports = Ports::new(config.base_port);
        let members = ports.members_string(MEMBERS);
        let mut nodes = Vec::with_capacity(MEMBERS);
        for id in 0..MEMBERS {
            nodes.push(Node {
                id,
                dirs: DirSet::create(&root.join(format!("node-{id}")))?,
                driver: None,
                module: None,
                container: None,
            });
        }
        let client_dirs = DirSet::create(&root.join("client"))?;

        let mut cluster = Self {
            config,
            root,
            ports,
            members,
            nodes,
            client_dirs,
            client_driver: None,
        };
        cluster.start_client_driver()?;
        cluster.start_nodes()?;
        Ok(cluster)
    }

    /// The aeron directory of the client's own driver.
    #[must_use]
    pub fn client_aeron_dir(&self) -> PathBuf {
        self.client_dirs.aeron.clone()
    }

    /// The port block, for the client's channels.
    #[must_use]
    pub const fn ports(&self) -> Ports {
        self.ports
    }

    /// The number of members.
    #[must_use]
    pub fn members(&self) -> usize {
        self.nodes.len()
    }

    /// The run root, kept on failure and printed by the CLI.
    #[must_use]
    pub fn run_dir(&self) -> &Path {
        &self.root
    }

    /// Wait until every **live** member reports `ELECTION_CLOSED` and exactly
    /// one of them is the leader, and answer its id.
    ///
    /// Reading only the live members is what makes this usable after the kill:
    /// the dead member's CnC file is gone with its driver, and the two
    /// survivors are the quorum.
    ///
    /// # Errors
    ///
    /// The timeout, with every member's last election/role reading.
    pub fn wait_for_leader(&self, timeout: Duration) -> Result<usize, String> {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Some(leader) = self.agreed_leader() {
                return Ok(leader);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        Err(format!(
            "the live members did not agree on one leader in {timeout:?}: {}",
            self.diagnostic()
        ))
    }

    /// `kill -9` one member's processes. The rest of the cluster is untouched.
    ///
    /// # Errors
    ///
    /// An id outside `0..MEMBERS`.
    pub fn kill_node(&mut self, id: usize) -> Result<(), String> {
        let node = self
            .nodes
            .get_mut(id)
            .ok_or_else(|| format!("no member {id}"))?;
        node.kill();
        Ok(())
    }

    fn start_client_driver(&mut self) -> Result<(), String> {
        let log = self.client_dirs.logs.join("driver.log");
        // The client keeps the driver's default image liveness timeout. Its
        // egress image must outlive the election: the client's context sets
        // `rejoin=false` on the egress subscription, so an image that closes
        // before the new leader publishes is not rejoined.
        let arguments = driver_args(
            &self.config,
            &self.client_dirs,
            self.ports.client_archive(),
            None,
        );
        let mut driver = spawn(
            &self.config.binary("deepmsg-archiving-media-driver"),
            &arguments,
            &log,
        )?;
        wait_for_driver_ready(&self.client_dirs.aeron, &log, &mut driver)?;
        self.client_driver = Some(driver);
        Ok(())
    }

    fn start_nodes(&mut self) -> Result<(), String> {
        // Every driver first: a member's consensus module connects to its
        // archive when it launches, and the archive is in that driver.
        for node in &mut self.nodes {
            let log = node.dirs.logs.join("driver.log");
            let arguments = driver_args(
                &self.config,
                &node.dirs,
                self.ports.archive_of(node.id),
                Some(MEMBER_IMAGE_LIVENESS),
            );
            let mut driver = spawn(
                &self.config.binary("deepmsg-archiving-media-driver"),
                &arguments,
                &log,
            )?;
            wait_for_driver_ready(&node.dirs.aeron, &log, &mut driver)?;
            node.driver = Some(driver);
        }

        for node in &mut self.nodes {
            let log = node.dirs.logs.join("module.log");
            let arguments =
                module_args(&self.config, self.ports, &node.dirs, &self.members, node.id);
            let module = spawn(
                &self.config.binary("deepmsg-cluster-node"),
                &arguments,
                &log,
            )?;
            node.module = Some(module);
        }
        std::thread::sleep(Duration::from_millis(200));

        for node in &mut self.nodes {
            let log = node.dirs.logs.join("container.log");
            let arguments =
                container_args(&self.config, self.ports, &node.dirs, &self.members, node.id);
            let container = spawn(
                &self.config.binary("deepmsg-clustered-service-container"),
                &arguments,
                &log,
            )?;
            node.container = Some(container);
        }

        std::thread::sleep(CHILD_SETTLE);
        Self::ensure_children_alive(&mut self.nodes)
    }

    /// A module or container that exits while it is joining is a startup
    /// failure, and its log says why. Without this check the next symptom is an
    /// election that never closes, which hides the cause.
    fn ensure_children_alive(nodes: &mut [Node]) -> Result<(), String> {
        for node in nodes {
            for (name, slot) in [
                ("module", node.module.as_mut()),
                ("container", node.container.as_mut()),
            ] {
                let Some(child) = slot else {
                    continue;
                };
                if let Some(status) = child
                    .try_wait()
                    .map_err(|error| format!("member {} {name} wait: {error}", node.id))?
                {
                    let log = node.dirs.logs.join(format!("{name}.log"));
                    return Err(format!(
                        "member {} {name} exited while starting ({status}):\n{}",
                        node.id,
                        tail(&log)
                    ));
                }
            }
        }
        Ok(())
    }

    /// The live members' agreed leader, or `None` while they are still
    /// electing, catching up, or not yet `ACTIVE`.
    #[must_use]
    pub fn agreed_leader(&self) -> Option<usize> {
        let live: Vec<&Node> = self.nodes.iter().filter(|node| node.is_live()).collect();
        // Two of three is the smallest quorum this topology can elect with.
        if live.len() < 2 {
            return None;
        }

        let mut leaders = Vec::new();
        for node in live {
            if self.counter(node.id, CLUSTER_ELECTION_STATE_TYPE_ID)? != ELECTION_CLOSED {
                return None;
            }
            match self.counter(node.id, CLUSTER_NODE_ROLE_TYPE_ID)? {
                ROLE_LEADER => leaders.push(node.id),
                ROLE_FOLLOWER => {}
                _ => return None,
            }
        }

        if leaders.len() == 1 {
            // `ModuleState::ACTIVE`: the leader sends `NewLeaderEvent`s only
            // from its ACTIVE duty cycle (`agent/mod.rs:940-941`). Election
            // closed and role LEADER are earlier states.
            if self.counter(leaders[0], CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID)
                == Some(MODULE_ACTIVE)
            {
                Some(leaders[0])
            } else {
                None
            }
        } else {
            None
        }
    }

    /// One cluster counter of a live member, by type id.
    fn counter(&self, id: usize, type_id: i32) -> Option<i64> {
        let node = self.nodes.get(id)?;
        let cnc = CncFile::open(&node.dirs.aeron, COUNTER_TIMEOUT).ok()?;
        let counters = cnc.counters()?;
        let counter_id = find_cluster_counter(&counters, type_id, self.config.cluster_id)?;
        counters.value(counter_id)
    }

    /// The session ids a member's session counters name.
    ///
    /// The key is the cluster id (4 bytes) then the session id (8 bytes), so
    /// this is the member's session-manager membership as the driver sees it.
    /// Gate 4 compares it with the client's session id: an event for a
    /// different id is ignored by the client, and that looks like "no event".
    #[must_use]
    pub fn session_ids(&self, id: usize) -> Option<Vec<i64>> {
        let node = self.nodes.get(id)?;
        let cnc = CncFile::open(&node.dirs.aeron, COUNTER_TIMEOUT).ok()?;
        let counters = cnc.counters()?;
        let cluster_id = self.config.cluster_id as u32;
        let mut ids = Vec::new();
        counters.for_each(|counter| {
            if counter.type_id != CLUSTER_SESSION_TYPE_ID {
                return;
            }
            let Some(key) = counters.key(counter.counter_id) else {
                return;
            };
            let Some(cluster) = key.get(0..4) else {
                return;
            };
            if u32::from_le_bytes([cluster[0], cluster[1], cluster[2], cluster[3]]) != cluster_id {
                return;
            }
            let Some(session) = key.get(4..12) else {
                return;
            };
            ids.push(i64::from_le_bytes(session.try_into().expect("eight bytes")));
        });
        Some(ids)
    }

    /// Every member's liveness, module state, election state and role, as one
    /// line for a failure or a slow-election trace.
    #[must_use]
    pub fn diagnostic(&self) -> String {
        self.nodes
            .iter()
            .map(|node| {
                format!(
                    "member {} alive={} module={:?} election={:?} role={:?} \
                     election_count={:?} term={:?}",
                    node.id,
                    node.is_live(),
                    self.counter(node.id, CLUSTER_CONSENSUS_MODULE_STATE_TYPE_ID),
                    self.counter(node.id, CLUSTER_ELECTION_STATE_TYPE_ID),
                    self.counter(node.id, CLUSTER_NODE_ROLE_TYPE_ID),
                    self.counter(node.id, CLUSTER_ELECTION_COUNT_TYPE_ID),
                    self.counter(node.id, CLUSTER_LEADERSHIP_TERM_ID_TYPE_ID),
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Whether each member's three processes are running, exited or killed.
    ///
    /// Counters outlive a process: a member whose module died still has a
    /// readable CnC file, with its last counters. This is the difference
    /// between "the election finished but the leader's module is gone" and
    /// "the leader is running and never sent the event".
    #[must_use]
    pub fn process_report(&mut self) -> String {
        let mut parts = Vec::new();
        for node in &mut self.nodes {
            let module = process_state(&mut node.module);
            let container = process_state(&mut node.container);
            let driver = process_state(&mut node.driver);
            parts.push(format!(
                "member {} module={module} container={container} driver={driver}",
                node.id
            ));
        }
        parts.join("; ")
    }
}

fn process_state(slot: &mut Option<Child>) -> String {
    match slot {
        Some(child) => match child.try_wait() {
            Ok(None) => "running".to_owned(),
            Ok(Some(status)) => format!("exited({status})"),
            Err(error) => format!("wait error {error}"),
        },
        None => "killed".to_owned(),
    }
}

impl Drop for ClusterNodes {
    fn drop(&mut self) {
        if let Some(mut driver) = self.client_driver.take() {
            let _ = driver.kill();
            let _ = driver.wait();
        }
        for node in &mut self.nodes {
            node.kill();
        }
        if !self.config.keep {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}

fn driver_args(
    config: &Config,
    dirs: &DirSet,
    archive_port: u16,
    image_liveness: Option<&str>,
) -> Vec<String> {
    let mut args = vec![
        // The driver's native name is `deepmsg.dir`; `aeron.dir` is the
        // reference's alias. Pass both, and never inherit the environment:
        // `DEEPMSG_DIR` beats a `-D`, and a leaked value once pointed the
        // driver at a source checkout.
        d("deepmsg.dir", dirs.aeron.display()),
        d("aeron.dir", dirs.aeron.display()),
        d("deepmsg.dir.delete.on.start", false),
        d("aeron.dir.delete.on.start", false),
        d("deepmsg.dir.delete.on.shutdown", false),
        d("aeron.dir.delete.on.shutdown", false),
        d("aeron.archive.dir", dirs.archive.display()),
        d("aeron.threading.mode", "SHARED"),
        d("aeron.archive.threading.mode", "SHARED"),
        // A fixed archive endpoint: a follower replicating from this member's
        // archive dials the endpoint in the member string.
        d(
            "aeron.archive.control.channel",
            format!("aeron:udp?endpoint=localhost:{archive_port}"),
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
    if let Some(timeout) = image_liveness {
        args.push(d("aeron.image.liveness.timeout", timeout));
    }
    if config.durable {
        args.push(d("aeron.archive.file.sync.level", 2));
        args.push(d("aeron.archive.catalog.file.sync.level", 2));
    }
    args
}

fn module_args(
    config: &Config,
    ports: Ports,
    dirs: &DirSet,
    members: &str,
    member_id: usize,
) -> Vec<String> {
    vec![
        d("aeron.dir", dirs.aeron.display()),
        d("aeron.cluster.dir", dirs.cluster.display()),
        d("aeron.cluster.id", config.cluster_id),
        d("aeron.cluster.member.id", member_id),
        d("aeron.cluster.members", members),
        d(
            "aeron.cluster.ingress.channel",
            format!(
                "aeron:udp?term-length=128k|endpoint=localhost:{}",
                ports.ingress_of(member_id)
            ),
        ),
        d(
            "aeron.cluster.consensus.channel",
            format!(
                "aeron:udp?term-length=128k|endpoint=localhost:{}",
                ports.consensus_of(member_id)
            ),
        ),
        // No endpoint: the module puts `control-mode=manual` on the log
        // publication itself (`ConsensusModule.java:2145`).
        d("aeron.cluster.log.channel", "aeron:udp?term-length=512k"),
        // No endpoint: the egress channel is a template. The leader publishes
        // each session's events to the response channel the client named.
        d("aeron.cluster.egress.channel", "aeron:udp?term-length=128k"),
        d(
            "aeron.cluster.control.channel",
            "aeron:ipc?term-length=128k",
        ),
        d(
            "aeron.cluster.replication.channel",
            "aeron:udp?endpoint=localhost:0",
        ),
        d(
            "aeron.cluster.leader.heartbeat.timeout",
            LEADER_HEARTBEAT_TIMEOUT,
        ),
        // The ballot waits one election timeout. The reference's default is
        // 1 s; half of it is still far above the local round trip, and it is
        // part of the five-second budget.
        d("aeron.cluster.election.timeout", "250ms"),
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

fn container_args(
    config: &Config,
    ports: Ports,
    dirs: &DirSet,
    members: &str,
    member_id: usize,
) -> Vec<String> {
    let mut args = module_args(config, ports, dirs, members, member_id);
    args.push(d("deepmsg.cluster.service", "basic-auction"));
    args
}
