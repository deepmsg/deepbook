#!/usr/bin/env bash
# Build and verify the deepmsg binaries that deepbook's process tests run.
#
#   scripts/deepmsg-deps.sh pin     print the revision Cargo.toml pins
#   scripts/deepmsg-deps.sh check   fail unless the binaries are from that revision
#   scripts/deepmsg-deps.sh build   build them and write the stamp `check` reads
#
# deepmsg is a git dependency pinned to a revision (docs/deps.md). The binaries
# the cluster e2e, the spike and `bench.sh` start come from a checkout, not from
# cargo, so nothing forces them to be the pinned revision. `build` writes
# `target/<profile>/deepbook-deps.txt` with the revision and the SHA-256 of each
# binary; `check` compares that stamp with the pin, which is what makes a local
# measurement reproducible.
#
# Environment:
#   DEEPBOOK_DEEPMSG_DIR   deepmsg checkout (default: <repo>/../deepmsg)
#   DEEPMSG_PROFILE        release|debug (default: release)
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DEEPMSG=${DEEPBOOK_DEEPMSG_DIR:-$ROOT/../deepmsg}
if [ -d "$DEEPMSG" ]; then
    DEEPMSG=$(cd "$DEEPMSG" && pwd)
fi
PROFILE=${DEEPMSG_PROFILE:-release}

# package:binary, in the order the documentation lists them.
BINARIES=(
    "deepmsg-archive:deepmsg-archiving-media-driver"
    "deepmsg-cluster:deepmsg-cluster-node"
    "deepmsg-cluster-service:deepmsg-clustered-service-container"
    "deepmsg-cluster-tool:deepmsg-cluster-tool"
)

die() {
    echo "deepmsg-deps: $*" >&2
    exit 1
}

usage() {
    sed -n '4,6p' "$0" | sed 's/^# \{0,1\}//'
}

# The revision every deepmsg dependency in the root manifest names.
read_pin() {
    local revs
    revs=$(sed -nE 's/^deepmsg-.*rev = "([0-9a-f]+)".*/\1/p' "$ROOT/Cargo.toml" | sort -u)
    if [ "$(printf '%s\n' "$revs" | grep -c .)" -ne 1 ]; then
        die "Cargo.toml does not pin every deepmsg dependency to one rev: [$revs]"
    fi
    printf '%s\n' "$revs"
}

profile_dir() {
    printf '%s\n' "$DEEPMSG/target/$PROFILE"
}

stamp_path() {
    printf '%s\n' "$(profile_dir)/deepbook-deps.txt"
}

case "$PROFILE" in
    release|debug) ;;
    *) die "DEEPMSG_PROFILE=$PROFILE is neither release nor debug" ;;
esac

# How to get the pinned binaries into $DEEPMSG: in place when the checkout is
# at the pin, otherwise in a worktree of it (a developer's own branch is not
# moved by this script).
build_hint() {
    local pin head want name pin_dir
    pin=$(read_pin)
    if git -C "$DEEPMSG" rev-parse --git-dir >/dev/null 2>&1 \
        && git -C "$DEEPMSG" cat-file -e "$pin^{commit}" 2>/dev/null; then
        head=$(git -C "$DEEPMSG" rev-parse HEAD)
        want=$(git -C "$DEEPMSG" rev-parse "$pin^{commit}")
        if [ "$head" != "$want" ]; then
            name=$(basename "$DEEPMSG")
            pin_dir="$(dirname "$DEEPMSG")/${name%-pin}-pin"
            cat >&2 <<EOF
  $DEEPMSG is at ${head:0:7}, not the pinned $pin. Build the pin in a worktree:

    git -C $DEEPMSG worktree add --detach $pin_dir $pin
    DEEPBOOK_DEEPMSG_DIR=$pin_dir just deps
EOF
            return
        fi
    fi
    echo "  run: just deps" >&2
}

check() {
    local pin dir stamp problems=0 entry bin recorded actual
    pin=$(read_pin)
    dir=$(profile_dir)
    stamp=$(stamp_path)

    if [ ! -d "$dir" ]; then
        echo "deepmsg-deps: no binaries at $dir" >&2
        build_hint
        exit 1
    fi
    if [ ! -f "$stamp" ]; then
        echo "deepmsg-deps: no build stamp at $stamp" >&2
        echo "  these binaries have no recorded revision" >&2
        problems=1
    else
        recorded=$(sed -n 's/^rev=//p' "$stamp")
        if [ "$recorded" != "$pin" ]; then
            echo "deepmsg-deps: the binaries were built from ${recorded:-an unknown revision}," \
                "Cargo.toml pins $pin" >&2
            problems=1
        fi
    fi

    for entry in "${BINARIES[@]}"; do
        bin=${entry##*:}
        if [ ! -f "$dir/$bin" ]; then
            echo "deepmsg-deps: missing $dir/$bin" >&2
            problems=1
            continue
        fi
        [ -f "$stamp" ] || continue
        recorded=$(sed -n "s/^$bin=//p" "$stamp")
        actual=$(sha256sum "$dir/$bin" | cut -d' ' -f1)
        if [ "$recorded" != "$actual" ]; then
            echo "deepmsg-deps: $dir/$bin does not match the stamp (rebuilt by hand?)" >&2
            problems=1
        fi
    done

    if [ "$problems" -ne 0 ]; then
        build_hint
        exit 1
    fi
    echo "deepmsg-deps: $dir is the pinned $pin ($PROFILE)"
}

build() {
    local pin head want flags=() args=() entry
    pin=$(read_pin)

    git -C "$DEEPMSG" rev-parse --git-dir >/dev/null 2>&1 \
        || die "$DEEPMSG is not a git checkout (set DEEPBOOK_DEEPMSG_DIR)"
    git -C "$DEEPMSG" cat-file -e "$pin^{commit}" 2>/dev/null \
        || die "the pinned revision $pin is not in $DEEPMSG (run: git -C $DEEPMSG fetch origin)"

    head=$(git -C "$DEEPMSG" rev-parse HEAD)
    want=$(git -C "$DEEPMSG" rev-parse "$pin^{commit}")
    if [ "$head" != "$want" ]; then
        echo "deepmsg-deps: the checkout is not at the pinned revision" >&2
        build_hint
        exit 1
    fi

    [ "$PROFILE" = release ] && flags+=(--release)
    for entry in "${BINARIES[@]}"; do
        args+=(-p "${entry%%:*}" --bin "${entry##*:}")
    done

    ( cd "$DEEPMSG" && cargo build --locked "${flags[@]}" "${args[@]}" )
    write_stamp "$pin"
    echo "deepmsg-deps: built ${#BINARIES[@]} binaries from $pin ($PROFILE) in $(profile_dir)"
}

write_stamp() {
    local pin=$1 dir stamp entry bin
    dir=$(profile_dir)
    stamp=$(stamp_path)
    {
        echo "# written by scripts/deepmsg-deps.sh from the revision Cargo.toml pins; do not edit"
        echo "rev=$pin"
        echo "profile=$PROFILE"
        echo "rustc=$(rustc --version)"
        for entry in "${BINARIES[@]}"; do
            bin=${entry##*:}
            echo "$bin=$(sha256sum "$dir/$bin" | cut -d' ' -f1)"
        done
    } > "$stamp"
}

case "${1:-}" in
    pin) read_pin ;;
    check) check ;;
    build) build ;;
    "" | -h | --help | help) usage ;;
    *) die "unknown command '$1' (pin, check, build)" ;;
esac
