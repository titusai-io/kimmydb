#!/usr/bin/env bash
#
# Run every example against a fresh node, and fail if any of them does.
#
# An example nobody runs rots into a document that used to be true. These are
# the answer to "what does using KimmyDB look like", so they are executed
# rather than admired.
set -euo pipefail

cd "$(dirname "$0")/.."
root=$(pwd)
work=$(mktemp -d)
trap 'kill "${node_pid:-}" 2>/dev/null || true; rm -rf "$work"' EXIT

port=$((20000 + RANDOM % 20000))
export KIMMY_URL="http://127.0.0.1:$port"
export KIMMY_ROOT_PASSWORD=example-password

cat >"$work/kimmy.toml" <<TOML
[server]
bind = "127.0.0.1:$port"
mcp = false

[storage]
data_dir = "$work/data"

[auth]
jwt_secret = "a-secret-long-enough-for-the-examples"
TOML

binary="${KIMMYD_BINARY:-$root/target/release/kimmyd}"
[ -x "$binary" ] || binary="$root/target/debug/kimmyd"
[ -x "$binary" ] || { echo "no kimmyd; run \`cargo build --release\` first" >&2; exit 1; }

"$binary" --config "$work/kimmy.toml" >"$work/node.log" 2>&1 &
node_pid=$!

for _ in $(seq 1 300); do
    if curl -sf "$KIMMY_URL/healthz" >/dev/null; then break; fi
    sleep 0.1
done
curl -sf "$KIMMY_URL/healthz" >/dev/null || { cat "$work/node.log"; exit 1; }

# Each example runs against the *same* node, which is deliberate: the second
# and third exercise the "already stocked" path, so re-runnability is checked
# rather than assumed.
echo "=== rust"
cargo run --quiet --example shelf -p kimmy-client

echo
echo "=== python"
(cd clients/python && uv run --quiet python "$root/examples/shelf.py")

echo
echo "=== go"
(cd clients/go && go run "$root/examples/shelf.go")

echo
echo "all three examples ran against $KIMMY_URL"
