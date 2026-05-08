SINK_MACH_STATUS_LOGS = {
    'topic': 'inst.status-logs',
    'content': """
{
  "type": "record",
  "name": "oltp.machine_status_logs",
  "fields": [
    {"name": "machine_id", "type": "int"},
    {"name": "status", "type": ["null", "string"], "default": null},
    {
      "name": "event_time", 
      "type": ["null", {
        "type": "long",
        "logicalType": "timestamp-millis" 
      }],
      "default": null
    }
  ]
}
""",
}


SINK_PROD_ORDERS = {
    'topic': 'inst.prod-orders',
    'content': """
{
  "type": "record",
  "name": "oltp.production_orders",
  "fields": [
    {"name": "order_id", "type": "int"},
    {"name": "product_id", "type": ["null", "int"], "default": null},
    {"name": "quantity", "type": ["null", "int"], "default": null},
    {
      "name": "start_at", 
      "type": ["null", {
        "type": "long",
        "logicalType": "timestamp-millis" 
      }],
      "default": null
    },
    {
      "name": "end_at", 
      "type": ["null", {
        "type": "long",
        "logicalType": "timestamp-millis" 
      }],
      "default": null
    },
    {
      "name": "created_at", 
      "type": ["null", {
        "type": "long",
        "logicalType": "timestamp-millis" 
      }],
      "default": null
    }
  ]
}
""",
}


SINK_PROD_RECORDS = {
    'topic': 'inst.prod-records',
    'content': """
{
  "type": "record",
  "name": "oltp.production_records",
  "fields": [
    {"name": "order_id", "type": ["null", "int"], "default": null},
    {"name": "machine_id", "type": ["null", "int"], "default": null},
    {"name": "product_id", "type": ["null", "int"], "default": null},
    {"name": "quantity", "type": ["null", "int"], "default": null},
    {
      "name": "event_time", 
      "type": ["null", {
        "type": "long",
        "logicalType": "timestamp-millis" 
      }],
      "default": null
    }
  ]
}
""",
}