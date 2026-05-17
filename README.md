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
├── LICENSE
├── README.md
├── requirements.txt
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

### *B.　Command Platform*
```
python3 src/core/v2/cp/main.py
```

<br>

### *C.　Instance*
```
python3 src/core/v2/inst/main.py
```

<br>

### *D.1.　[ Manual ] Before Push Code*
```bash
# <語法格式版本>
black --version
# <語法格式檢查>
black src/

# 期望輸出
# All done! ✨ 🍰 ✨
# ?? files left unchanged.
```

<br>

### *⭐ D.2.　[ Auto - Global Var ] Just Push Code*
```bash
# 全域設定 ( 一次性 )
    # 1. 透過 Ubuntu 系統套件管理員安裝 pipx
    sudo apt update && sudo apt install -y pipx
    
    # 2. pipx 自動配置環境變數路徑
    pipx ensurepath
    
    # 3. 用 pipx 安裝 pre-commit
    pipx install pre-commit

# ⭐ 當前專案的 Git 與 pre-commit 工具正式綁定
pre-commit install
```

<br><br><br>