#!/bin/bash
# Bump dex-connector across pairtrade + slow-mm in one shot.
#
# Usage:
#   scripts/bump.sh v4.2.42
#
# What it does (all local — no push):
#   1. Verifies no uncommitted changes in the three repos for the files it touches
#   2. Refuses if the tag already exists locally or on origin
#   3. Creates the tag on dex-connector HEAD
#   4. In pairtrade: sed-bumps `DEX_CONNECTOR_REF`, runs `cargo update -p
#      dex-connector`, commits
#   5. Same for slow-mm
#   6. Prints the push commands for the user to run
#
# Assumes sibling layout: ~/bot/{dex-connector,pairtrade,slow-mm}
#
# Tracked in bot-strategy#119.

set -euo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 vX.Y.Z"
    exit 1
fi

TAG="$1"
case "$TAG" in
    v*.*.*) ;;
    *) echo "Tag must look like vX.Y.Z (got: $TAG)"; exit 1;;
esac

DEX_DIR="${DEX_DIR:-$HOME/bot/dex-connector}"
PAIR_DIR="${PAIR_DIR:-$HOME/bot/pairtrade}"
MM_DIR="${MM_DIR:-$HOME/bot/slow-mm}"

for d in "$DEX_DIR" "$PAIR_DIR" "$MM_DIR"; do
    if [ ! -d "$d/.git" ]; then
        echo "Not a git repo: $d"
        exit 1
    fi
done

# Fail if tag exists locally or on origin for dex-connector.
cd "$DEX_DIR"
if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
    echo "Tag $TAG already exists locally in $DEX_DIR"
    exit 1
fi
if git ls-remote --exit-code --tags origin "$TAG" >/dev/null 2>&1; then
    echo "Tag $TAG already exists on origin in $DEX_DIR"
    exit 1
fi

# Guard: the files we are about to modify must be clean.
require_clean() {
    local dir="$1"; shift
    local files=("$@")
    cd "$dir"
    for f in "${files[@]}"; do
        if ! git diff --quiet -- "$f" || ! git diff --cached --quiet -- "$f"; then
            echo "Dirty tracked file in $dir: $f"
            exit 1
        fi
    done
}
require_clean "$DEX_DIR" Cargo.toml
require_clean "$PAIR_DIR" .github/workflows/ci.yml Cargo.lock
require_clean "$MM_DIR" .github/workflows/ci.yml Cargo.lock

echo "== dex-connector: bumping Cargo.toml version + tagging $TAG =="
cd "$DEX_DIR"
# Strip leading 'v' for the Cargo.toml version string.
VERSION="${TAG#v}"
if ! grep -qE '^version = "[0-9]+\.[0-9]+\.[0-9]+"' Cargo.toml; then
    echo "Could not find top-level version line in $DEX_DIR/Cargo.toml"
    exit 1
fi
sed -i -E "0,/^version = \"[0-9]+\.[0-9]+\.[0-9]+\"/s||version = \"$VERSION\"|" Cargo.toml
git add Cargo.toml
git commit -m "Bump dex-connector to $TAG"
git tag "$TAG" -m "$TAG"

bump_downstream() {
    local dir="$1"
    local name
    name="$(basename "$dir")"
    echo "== $name: bumping CI ref + Cargo.lock =="
    cd "$dir"

    local ci=".github/workflows/ci.yml"
    if ! grep -qE '^  DEX_CONNECTOR_REF: v[0-9]+\.[0-9]+\.[0-9]+' "$ci"; then
        echo "Could not find DEX_CONNECTOR_REF line in $dir/$ci"
        exit 1
    fi
    sed -i -E "s|^  DEX_CONNECTOR_REF: v[0-9]+\.[0-9]+\.[0-9]+|  DEX_CONNECTOR_REF: $TAG|" "$ci"

    cargo update -p dex-connector

    git add "$ci" Cargo.lock
    git commit -m "Bump dex-connector to $TAG"
}
bump_downstream "$PAIR_DIR"
bump_downstream "$MM_DIR"

cat <<EOM

Done locally. To roll out, push in this order:

  cd $DEX_DIR   && git push origin \$(git rev-parse --abbrev-ref HEAD) && git push origin $TAG
  cd $PAIR_DIR  && git push origin \$(git rev-parse --abbrev-ref HEAD)
  cd $MM_DIR    && git push origin \$(git rev-parse --abbrev-ref HEAD)

The dex-connector tag must land first so downstream CI can resolve it.
EOM
