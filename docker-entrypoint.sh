#!/usr/bin/env bash
#
# Dual-purpose entrypoint for the Candybox image: the same image runs either the storage node
# (server) or the `candybox` client CLI, depending on the first argument.
#
#   docker run … zetaplusae/candybox                       # storage node, default example config
#   docker run … zetaplusae/candybox server [config-path]  # storage node (optional config path)
#   docker run … zetaplusae/candybox candybox <args…>      # client CLI (honors CANDYBOX_SERVER / -s)
#   docker run … zetaplusae/candybox <anything-else>       # run it verbatim (escape hatch)
#
# Server config defaults to the shipped example; CANDYBOX_* env vars override individual keys.
#
set -euo pipefail

DEFAULT_CONF="${CANDYBOX_CONF_DIR:-/opt/candybox/conf}/candybox.properties.example"

case "${1:-server}" in
  server)
    shift || true
    [[ "$#" -eq 0 ]] && set -- "$DEFAULT_CONF"
    exec candybox-server "$@"
    ;;
  candybox | candybox-server)
    # Explicit binary name: run it as given (e.g. `candybox list-boxes`).
    exec "$@"
    ;;
  *)
    # Backward compatibility: a bare path to an existing file means "server with this config"
    # (older images had `candybox-server` as the entrypoint and took the config path directly).
    if [[ -f "$1" ]]; then
      exec candybox-server "$@"
    fi
    exec "$@"
    ;;
esac
