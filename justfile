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

# M5: one member + engine-container + fix-gateway, measured by fix-bench.
# `just bench --rate 20000 --samples 50000` forwards the script's options.
bench *args:
    ./deploy/bench.sh {{args}}

# M5 backend comparison: epoll, uring, uring, epoll (ABBA).
compare:
    ./deploy/bench.sh --compare --rate 5000 --samples 20000 --warmup 2000

# M5 dashboard: stack + admin + WebSocket UI, then keep serving.
demo:
    ./deploy/bench.sh --admin --serve --rate 2000 --samples 5000 --warmup 500

# Grafana + Prometheus for the admin's /metrics (see deploy/docker-compose.monitoring.yml).
monitoring:
    docker compose -f deploy/docker-compose.monitoring.yml up -d
