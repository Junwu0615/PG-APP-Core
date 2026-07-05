#!/bin/bash

echo "==== 正在檢查觀測管道狀態 ===="
echo ""

# 1. 檢查 Prometheus (等待 9090 埠回應)
curl -s http://localhost:9090/-/healthy > /dev/null
if [ $? -eq 0 ]; then echo "✅  Prometheus [9090]: Online"; else echo "❌  Prometheus [9090]: Offline"; fi

# 2. 檢查 Loki (檢查 API 版本路徑)
curl -s http://localhost:3100/loki/api/v1/status/buildinfo > /dev/null
if [ $? -eq 0 ]; then echo "✅  Loki [3100]: Online"; else echo "❌  Loki [3100]: Offline"; fi

# 3. 檢查 Tempo (gRPC 埠口探測)
nc -z localhost 4317 > /dev/null
if [ $? -eq 0 ]; then echo "✅  Tempo [4317]: Online"; else echo "❌  Tempo [4317]: Offline"; fi

echo ""
echo "========== 檢查完畢 =========="