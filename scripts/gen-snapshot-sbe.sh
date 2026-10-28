#!/usr/bin/env bash
# Regenerate the engine snapshot SBE codec (ADR 0005).
#
# Mirrors the generator setup deepmsg uses (deepmsg ADR-0004): sbe-tool 1.40.2
# with its Rust target, agrona 2.6.1 on the classpath, and FIX's sbe.xsd
# validating the schema. The generator is not a build dependency: this script
# is run explicitly and its output is checked in.
#
# The jars are taken from $SBE_TOOL_JAR and $AGRONA_JAR first, then from the
# Gradle module cache.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

SBE_TOOL_VERSION=1.40.2
AGRONA_VERSION=2.6.1
SCHEMA=schemas/deepbook-snapshot.xml
XSD=schemas/fpl/sbe.xsd
PACKAGE=deepbook_snapshot
DEST=crates/codec/snapshot
STAGE=target/sbe-snapshot

locate_jar() {
    local env_name=$1 artifact=$2 version=$3
    local from_env=${!env_name:-}
    if [[ -n "$from_env" ]]; then
        printf '%s\n' "$from_env"
        return
    fi
    local found
    found=$(find "${GRADLE_USER_HOME:-$HOME/.gradle}" -type f -name "$artifact-$version.jar" -print -quit 2>/dev/null)
    if [[ -z "$found" ]]; then
        echo "gen-snapshot-sbe: cannot find $artifact-$version.jar; set $env_name" >&2
        exit 1
    fi
    printf '%s\n' "$found"
}

command -v java >/dev/null || { echo "gen-snapshot-sbe: java not found" >&2; exit 1; }
[[ -f "$SCHEMA" ]] || { echo "gen-snapshot-sbe: $SCHEMA is missing" >&2; exit 1; }
[[ -f "$XSD" ]] || { echo "gen-snapshot-sbe: $XSD is missing" >&2; exit 1; }

SBE_TOOL_JAR=$(locate_jar SBE_TOOL_JAR sbe-tool "$SBE_TOOL_VERSION")
AGRONA_JAR=$(locate_jar AGRONA_JAR agrona "$AGRONA_VERSION")

rm -rf "$STAGE"
mkdir -p "$STAGE"
TRANSCRIPT=$(mktemp)
trap 'rm -f "$TRANSCRIPT"' EXIT

java -cp "$SBE_TOOL_JAR:$AGRONA_JAR" \
    -Dsbe.output.dir="$STAGE" \
    -Dsbe.target.language=Rust \
    -Dsbe.validation.xsd="$XSD" \
    -Dsbe.validation.stop.on.error=true \
    uk.co.real_logic.sbe.SbeTool "$SCHEMA" 2>&1 | tee "$TRANSCRIPT"

# sbe-tool reports XSD violations and then exits zero. Without this check a
# malformed schema produces codecs that quietly disagree with the wire.
if grep -q '\[Error\]' "$TRANSCRIPT"; then
    echo "gen-snapshot-sbe: the generator reported [Error]; the schema is malformed" >&2
    exit 1
fi

GENERATED="$STAGE/$PACKAGE/src"
[[ -d "$GENERATED" ]] || { echo "gen-snapshot-sbe: expected $GENERATED" >&2; exit 1; }

rm -rf "$DEST/src"
cp -R "$GENERATED" "$DEST/src"
cargo fmt -p deepbook-codec-snapshot
echo "gen-snapshot-sbe: wrote $DEST/src"
