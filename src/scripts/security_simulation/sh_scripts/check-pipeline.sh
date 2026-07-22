#!/bin/bash

VAULT_ADDR=${VAULT_ADDR:-"http://127.0.0.1:8200"}

echo "==== 正在檢查 Vault 狀態 ($VAULT_ADDR) ===="
echo ""

# 1. 檢查 Liveness (容器是否存活，回傳 200 即代表活著)
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$VAULT_ADDR/v1/sys/health")
if [ "$HTTP_CODE" -eq 200 ] || [ "$HTTP_CODE" -eq 429 ] || [ "$HTTP_CODE" -eq 503 ]; then
    echo "✅ Vault Liveness [API]: Online (HTTP $HTTP_CODE)"
else
    echo "❌ Vault Liveness [API]: Offline (HTTP $HTTP_CODE)"
    exit 1
fi

# 2. 透過 Vault Status API 檢查內部封鎖與初始化狀態
STATUS_JSON=$(curl -s "$VAULT_ADDR/v1/sys/health")

# 解析 JSON (假設系統有 jq，若無可改用 grep)
if command -v jq &> /dev/null; then
    INITIALIZED=$(echo "$STATUS_JSON" | jq -r '.initialized')
    SEALED=$(echo "$STATUS_JSON" | jq -r '.sealed')
    HA_ENABLED=$(echo "$STATUS_JSON" | jq -r '.ha_enabled')

    if [ "$INITIALIZED" = "true" ]; then
        echo "✅ Vault Initialization: Initialized"
    else
        echo "❌ Vault Initialization: Not Initialized (需要執行 vault operator init)"
    fi

    if [ "$SEALED" = "false" ]; then
        echo "✅ Vault Seal Status: Unsealed (正常運作中)"
    else
        echo "⚠️  Vault Seal Status: SEALED (已上鎖，需要執行 vault operator unseal)"
    fi

    echo "ℹ️  HA Enabled: $HA_ENABLED"
else
    # 簡易替代方案 (如果環境沒有 jq)
    echo "ℹ️  Raw Health Response: $STATUS_JSON"
fi

echo ""
echo "========== 檢查完畢 =========="