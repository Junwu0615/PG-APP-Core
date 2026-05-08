<a href='https://github.com/Junwu0615/Platform Genesis'><img alt='GitHub Views' src='https://views.whatilearened.today/views/github/Junwu0615/Platform Genesis.svg'>

## *⭐ PG-APP-Core ⭐*

<br>

### *A.　Roadmap*

<details>
<summary><b><i>　Project Tree </i></b></summary>
<ul>

```bash
tree -I 'venv|.git|__pycache__|docs|logs|assets|kafka_data'
tree -d -I 'venv|.git|__pycache__|docs|logs|assets|kafka_data'

.
├── README.md
└── src
    ├── __init__.py
    ├── core
    │   ├── __init__.py
    │   ├── models
    │   │   ├── __init__.py
    │   │   ├── simulator.py
    │   │   └── sink_format.py
    │   ├── v1
    │   │   ├── __init__.py
    │   │   ├── factory_config.yaml
    │   │   ├── init_factory_data.py
    │   │   └── simulate_factory_stream.py
    │   └── v2
    │       ├── __init__.py
    │       ├── api
    │       │   └── __init__.py
    │       ├── cp
    │       │   ├── __init__.py
    │       │   └── main.py
    │       ├── factory_config.yaml
    │       ├── inst
    │       │   ├── __init__.py
    │       │   └── main.py
    │       └── scripts
    │           ├── __init__.py
    │           ├── create_topic.py
    │           ├── init.py
    │           └── topics_config.json
    └── scripts
        ├── __init__.py
        ├── generic_benchmark
        │   ├── dashboard_benchmark.sql
        │   └── olap_benchmark.sql
        └── sql
            ├── auto_partition.py
            ├── delete_data.py
            └── drop_table.py
```

</ul>
</details>

<br>