default:
    @just --list

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

test:
    cargo test --workspace

check: fmt-check clippy test

fix-conformance:
    ./tests/fix_conformance/run.sh

marketdata-loss:
    DEEPBOOK_DEEPMSG_DIR=../deepmsg cargo test -p deepbook-marketdata --test deepmsg_loss -- --nocapture

# M4c: driver + module + engine-container + fix-gateway, a snapshot, kill -9
# of the engine's host, recovery, and a continuation fill. Opt-in: it starts
# real processes and needs the deepmsg binaries.
engine-e2e:
    cargo build -p deepbook-engine-cluster --bin engine-container
    cargo test -p deepbook-gateway --test cluster_e2e -- --ignored --nocapture
