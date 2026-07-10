#!/bin/bash

echo "======= 正在建立與 k3s 叢集的觀測隧道 ======="
echo ""

# 使用 & 符號將其放入背景執行
kubectl port-forward svc/tempo-homelab-test 3100:3100 -n observability-homelab-test > /dev/null 2>&1 &
echo "✅  Tempo UI (3100) 已連線"
kubectl port-forward svc/tempo-homelab-test 4317:4317 -n observability-homelab-test > /dev/null 2>&1 &
echo "✅  Tempo gRPC (4317) 已連線"

kubectl port-forward svc/loki-gateway 3100:80 -n observability-homelab-test > /dev/null 2>&1 &
echo "✅  Loki (3100) 已連線"

kubectl port-forward svc/prometheus-operated 9090:9090 -n observability-homelab-test > /dev/null 2>&1 &
echo "✅  Prometheus (9090) 已連線"

echo ""
echo "=== 隧道全開！ 請啟動 FastAPI ( api.py ) ==="