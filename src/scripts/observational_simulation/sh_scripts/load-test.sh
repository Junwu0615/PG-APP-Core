while true; do
  curl -X 'POST' \
    'http://localhost:8000/orders/?item_name=Laptop&amount=999.99&customer_id=101' \
    -H 'accept: application/json' -d ''
  sleep 0.5
done