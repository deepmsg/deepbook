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
