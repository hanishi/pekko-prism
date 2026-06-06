#!/usr/bin/env bash
#
# Launch the prism reverse proxy with normal shell quoting (unlike
# `sbt "runMain …"`, this handles flags containing spaces and quotes).
#
# Usage:
#   ./run-proxy.sh <bindPort> <originBaseUrl> [--rewrite a=b] [--text] [--insert-after '<body>=…'] …
#
# Example (brand the R2 asset host + inject a banner against the live site):
#   ./run-proxy.sh 8080 http://publisher.programmer.llc \
#     --rewrite pub-7ab486148c8740dbb2cc31c5072eb91c.r2.dev=cdn.publisher.programmer.llc \
#     --insert-after '<body>=<div style="background:#fee;padding:8px">via prism</div>'
#
# Then open http://localhost:8080/ in a browser (or `curl`). Press Ctrl-C to stop.

set -euo pipefail
cd "$(dirname "$0")"

echo "resolving classpath (compiles if needed)…" >&2
CP="$(sbt -batch --error 'export Runtime/fullClasspath' | tail -1)"

exec java -cp "$CP" prism.ReverseProxy "$@"
