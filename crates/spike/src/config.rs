//! Where deepmsg lives, where the run directory lives, and the ports.
//!
//! Everything is environment-driven so the same binary can run on a developer
//! machine and on the bench machine:
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `DEEPBOOK_DEEPMSG_DIR` | `../deepmsg` | deepmsg checkout whose binaries are run |
//! | `DEEPMSG_DIR` | — | legacy fallback; never passed to a child |
//! | `DEEPMSG_PROFILE` | `release` | `target/<profile>` to take the binaries from |
//! | `SPIKE_BASE_PORT` | `25000` | first port of the block |
//! | `SPIKE_RUN_DIR` | `/dev/shm/deepbook-spike-<pid>` | aeron/archive/cluster/logs |
//! | `SPIKE_KEEP` | unset | keep the run directory after the run |

use std::path::{Path, PathBuf};

/// The configuration of one spike run.
#[derive(Clone, Debug)]
pub struct Config {
    /// The deepmsg checkout whose `target/<profile>/` holds the binaries.
    pub deepmsg_dir: PathBuf,
    /// `release` or `debug`.
    pub profile: String,
    /// The first port of the block.
    pub base_port: u16,
    /// The cluster id shared by the module and the container.
    pub cluster_id: i32,
    /// Ask the archive to fsync recordings and the catalog (gate 3).
    pub durable: bool,
    /// Keep the run directory after the run.
    pub keep: bool,
    /// The root of the run's aeron/archive/cluster/log directories.
    pub run_root: PathBuf,
}

impl Config {
    /// Read the environment, with `durable` chosen by the scenario.
    #[must_use]
    pub fn new(durable: bool) -> Self {
        let deepmsg_dir = std::env::var("DEEPBOOK_DEEPMSG_DIR")
            .or_else(|_| std::env::var("DEEPMSG_DIR"))
            .map_or_else(|_| PathBuf::from("../deepmsg"), PathBuf::from);
        let profile = std::env::var("DEEPMSG_PROFILE").unwrap_or_else(|_| "release".to_owned());
        let base_port = std::env::var("SPIKE_BASE_PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(25_000);
        let keep = std::env::var("SPIKE_KEEP").is_ok();
        let run_root = std::env::var("SPIKE_RUN_DIR").map_or_else(
            |_| {
                let base = if Path::new("/dev/shm").is_dir() {
                    PathBuf::from("/dev/shm")
                } else {
                    std::env::temp_dir()
                };
                base.join(format!("deepbook-spike-{}", std::process::id()))
            },
            PathBuf::from,
        );

        Self {
            deepmsg_dir,
            profile,
            base_port,
            cluster_id: 7,
            durable,
            keep,
            run_root,
        }
    }

    /// The path of a binary in the deepmsg build output.
    #[must_use]
    pub fn binary(&self, name: &str) -> PathBuf {
        self.deepmsg_dir
            .join("target")
            .join(&self.profile)
            .join(name)
    }

    /// Fail early, with the build command, when a binary is missing.
    ///
    /// # Errors
    ///
    /// The names of the binaries that are not there.
    pub fn check_binaries(&self) -> Result<(), String> {
        let mut missing = Vec::new();
        for name in [
            "deepmsg-archiving-media-driver",
            "deepmsg-cluster-node",
            "deepmsg-clustered-service-container",
            "deepmsg-cluster-tool",
        ] {
            let path = self.binary(name);
            if !path.is_file() {
                missing.push(path.display().to_string());
            }
        }

        if missing.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "deepmsg binaries are missing:\n  {}\n\
                 build them in {}:\n\
                 cargo build --{} -p deepmsg-archive --bin deepmsg-archiving-media-driver \\\n\
                 \x20 -p deepmsg-cluster --bin deepmsg-cluster-node \\\n\
                 \x20 -p deepmsg-cluster-service --bin deepmsg-clustered-service-container \\\n\
                 \x20 -p deepmsg-cluster-tool --bin deepmsg-cluster-tool",
                missing.join("\n  "),
                self.deepmsg_dir.display(),
                self.profile,
            ))
        }
    }
}

/// The persistent directories of one run. The aeron directory is per
/// generation: a restarted driver gets a fresh one, while the archive and
/// cluster directories are what recovery reads.
#[derive(Clone, Debug)]
pub struct Paths {
    /// The run's root.
    pub root: PathBuf,
    /// The archive directory (persistent across restarts).
    pub archive: PathBuf,
    /// The cluster directory: `recording.log`, node state, mark files.
    pub cluster: PathBuf,
    /// Child process logs.
    pub logs: PathBuf,
}

impl Paths {
    /// Create the run's directories, removing a previous run at the same path.
    ///
    /// # Errors
    ///
    /// A filesystem error.
    pub fn new(root: &Path) -> Result<Self, String> {
        if root.exists() {
            std::fs::remove_dir_all(root)
                .map_err(|error| format!("remove {}: {error}", root.display()))?;
        }
        let paths = Self {
            root: root.to_path_buf(),
            archive: root.join("archive"),
            cluster: root.join("cluster"),
            logs: root.join("logs"),
        };
        for directory in [&paths.archive, &paths.cluster, &paths.logs] {
            std::fs::create_dir_all(directory)
                .map_err(|error| format!("create {}: {error}", directory.display()))?;
        }
        Ok(paths)
    }

    /// The aeron directory for a driver generation.
    #[must_use]
    pub fn aeron(&self, generation: u32) -> PathBuf {
        self.root.join(format!("aeron-{generation}"))
    }
}

/// The port block. The offsets are the reference's test scheme, so the
/// channels look like the ones in deepmsg's own cluster tests.
#[derive(Clone, Copy, Debug)]
pub struct Ports {
    /// The first port.
    pub base: u16,
}

impl Ports {
    /// `base` is the ingress port.
    #[must_use]
    pub const fn new(base: u16) -> Self {
        Self { base }
    }

    #[must_use]
    pub const fn ingress(self) -> u16 {
        self.base
    }

    #[must_use]
    pub const fn consensus(self) -> u16 {
        self.base + 10
    }

    #[must_use]
    pub const fn log(self) -> u16 {
        self.base + 20
    }

    #[must_use]
    pub const fn client_egress(self) -> u16 {
        self.base + 30
    }

    #[must_use]
    pub const fn archive(self) -> u16 {
        self.base + 40
    }

    /// The ingress port of `member` in a multi-member cluster.
    #[must_use]
    pub const fn ingress_of(self, member: usize) -> u16 {
        self.base + member as u16
    }

    /// The consensus port of `member` in a multi-member cluster.
    #[must_use]
    pub const fn consensus_of(self, member: usize) -> u16 {
        self.base + 10 + member as u16
    }

    /// The log port of `member` in a multi-member cluster.
    #[must_use]
    pub const fn log_of(self, member: usize) -> u16 {
        self.base + 20 + member as u16
    }

    /// The archive control port of `member` in a multi-member cluster.
    #[must_use]
    pub const fn archive_of(self, member: usize) -> u16 {
        self.base + 40 + member as u16
    }

    /// The archive control port of the client's own driver, which is not a
    /// member and never dials or is dialled by the cluster.
    #[must_use]
    pub const fn client_archive(self) -> u16 {
        self.base + 43
    }

    /// `id,ingress,consensus,log,catchup,archive` for one member.
    #[must_use]
    pub fn member_string(self) -> String {
        self.members_string(1)
    }

    /// The member string of a `count`-member cluster, members `0..count`.
    ///
    /// Each member's ingress, consensus, log and archive endpoints are its own;
    /// the catchup endpoint is `:0` because it is resolved at run time
    /// (`ClusterMember.java:561-564`).
    #[must_use]
    pub fn members_string(self, count: usize) -> String {
        (0..count)
            .map(|member| {
                format!(
                    "{member},localhost:{},localhost:{},localhost:{},localhost:0,localhost:{}",
                    self.ingress_of(member),
                    self.consensus_of(member),
                    self.log_of(member),
                    self.archive_of(member)
                )
            })
            .collect::<Vec<_>>()
            .join("|")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_member_string_has_one_entry_per_member() {
        let ports = Ports::new(25_000);
        let members = ports.members_string(3);
        let entries: Vec<&str> = members.split('|').collect();
        assert_eq!(3, entries.len());
        assert_eq!(
            "0,localhost:25000,localhost:25010,localhost:25020,localhost:0,localhost:25040",
            entries[0]
        );
        assert_eq!(
            "2,localhost:25002,localhost:25012,localhost:25022,localhost:0,localhost:25042",
            entries[2]
        );
    }

    #[test]
    fn the_single_member_string_is_the_first_entry() {
        let ports = Ports::new(25_000);
        assert_eq!(ports.members_string(1), ports.member_string());
    }
}
