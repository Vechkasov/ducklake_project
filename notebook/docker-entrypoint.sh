#!/usr/bin/env bash
set -euo pipefail

mkdir -p "${HOME}/.duckdb" "${HOME}/.duckdb/extensions"

python /usr/local/bin/ducklake-bootstrap.py || echo "[bootstrap] skipped or failed"

TOKEN_OPT="--ServerApp.token=${JUPYTER_TOKEN:-duck}"
ROOT_OPT="--ServerApp.root_dir=${JUPYTER_WORKDIR:-/home/jupyter/work}"

exec "$@" ${TOKEN_OPT} ${ROOT_OPT}
