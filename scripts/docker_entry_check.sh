#!/usr/bin/env sh
# 无 Docker 环境时的等价冒烟：只用 app/ + samples/ 启动容器入口（python -m app），
# 访问首页与恢复页后在临时目录起一份可用的服务。用于确认镜像 CMD 可用。
set -eu
cd "$(dirname "$0")/.."
DATA_DIR="${DATA_DIR:-$(mktemp -d)}"
PORT="${PORT:-5099}"
export DATA_DIR PORT
python3 -m app >/tmp/geosync-entry-check.log 2>&1 &
pid=$!
trap 'kill "$pid" 2>/dev/null || true; wait "$pid" 2>/dev/null || true' EXIT
i=0
while [ "$i" -lt 60 ]; do
  if curl -fsS "http://127.0.0.1:$PORT/api/health" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1)); sleep 0.25
done
curl -fsS "http://127.0.0.1:$PORT/api/health"
echo
curl -fsS "http://127.0.0.1:$PORT/restore" >/dev/null
echo "容器入口检查通过（$PORT）"
