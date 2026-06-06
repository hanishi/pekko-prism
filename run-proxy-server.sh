#!/usr/bin/env bash
#
# Launch the config-driven prism reverse proxy.
#
# Usage:
#   ./run-proxy-server.sh [config-file]
#
# With no argument it uses the defaults in application.conf. With a file it
# layers that file on top (so you only specify what you change). Ctrl-C / SIGTERM
# drains in-flight requests before exiting.

set -euo pipefail
cd "$(dirname "$0")"

echo "resolving classpath (compiles if needed)…" >&2
CP="$(sbt -batch --error 'export Runtime/fullClasspath' | tail -1)"

exec java -cp "$CP" prism.ProxyServer "$@"
