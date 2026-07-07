#!/bin/bash

echo "==== 正在檢查觀測管道狀態 ===="
echo ""

# 1. 檢查 Prometheus (等待 9090 埠回應)
curl -s http://localhost:9090/-/healthy > /dev/null
if [ $? -eq 0 ]; then echo "✅  Prometheus [9090]: Online"; else echo "❌  Prometheus [9090]: Offline"; fi

# 2. 檢查 Loki (檢查 API 版本路徑)
curl -s http://localhost:3100/loki/api/v1/status/buildinfo > /dev/null
if [ $? -eq 0 ]; then echo "✅  Loki [3100]: Online"; else echo "❌  Loki [3100]: Offline"; fi

# 3. Tempo (檢查 UI)
curl -s http://localhost:3100/status > /dev/null && echo "✅ Tempo UI [3100]: Online" || echo "❌ Tempo UI [3100]: Offline"

# 4. Tempo (檢查 gRPC 接收埠 => Python 發送資料位置)
nc -z localhost 4317 > /dev/null && echo "✅ Tempo OTLP [4317]: Online" || echo "❌ Tempo OTLP [4317]: Offline"


echo ""
echo "========== 檢查完畢 =========="