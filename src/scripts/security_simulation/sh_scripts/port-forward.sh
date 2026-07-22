#!/bin/bash

echo "======= 正在建立與 k3s 叢集的密鑰隧道 ======="
echo ""

# 使用 & 符號將其放入背景執行
kubectl port-forward svc/vault-homelab-test 8200:8200 -n security-homelab-test > /dev/null 2>&1 &
echo "✅  Vault (8200) 已連線"

echo ""
echo "=== 隧道全開！ 請啟動 Python ( monitor_roles.py | request_secret.py ) ==="