import os, json, yaml, pytest


def test_validate_all_config_files():
    """
    通用測試：自動掃描專案內所有 JSON 和 YAML 設定檔，確保語法解析正常。
    """
    project_root = os.path.join(os.path.dirname(__file__), "..")

    for root, dirs, files in os.walk(project_root):
        # 跳過虛擬環境與快取資料夾，避免掃描到不必要的檔案
        if any(p in root for p in ['tests', '.venv', 'venv', '.cache', 'docker-compose', 'k8s']):
            continue

        for file in files:
            file_path = os.path.join(root, file)

            # 1. 驗證 JSON
            if file.endswith('.json'):
                with open(file_path, 'r', encoding='utf-8') as f:
                    try:
                        json.load(f)
                    except json.JSONDecodeError as e:
                        pytest.fail(f"JSON 格式損壞: {file_path}\n錯誤訊息: {e}")

            # 2. 驗證 YAML
            elif file.endswith(('.yaml', '.yml')):
                with open(file_path, 'r', encoding='utf-8') as f:
                    try:
                        yaml.safe_load(f)
                    except yaml.YAMLError as e:
                        pytest.fail(f"YAML 格式損壞: {file_path}\n錯誤訊息: {e}")