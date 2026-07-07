#!/bin/bash
echo "🚀 開始負載測試 (流量: 約 2 RPS)..."

while true; do
  # 隨機選擇 customer_id，讓 Tempo 產生不同的 Trace 路徑
  CUSTOMER_ID=$((1 + RANDOM % 10))

  # 靜默執行，只在出錯時顯示內容，避免終端機被洗版
  curl -s -X 'POST' \
    "http://localhost:8000/orders/?item_name=Laptop&amount=999.99&customer_id=$CUSTOMER_ID" \
    -H 'accept: application/json' -d '' > /dev/null

  # 隨機延遲 (0.3s - 0.7s)，模擬真實使用者間隔，避免機器感太重
  sleep $(awk -v min=0.3 -v max=0.7 'BEGIN{srand(); print min+rand()*(max-min)}')
done