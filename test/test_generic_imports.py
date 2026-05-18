import os, pkgutil, pytest, importlib


def test_import_all_modules():
    """
    通用測試：自動找出專案內所有的 Python 模組並嘗試 import，
    確保沒有任何一個檔案因為內部 import 寫錯而無法被調用
    """
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    # 自動尋找模組
    for _, module_name, is_pkg in pkgutil.walk_packages(path=[project_root]):
        # 跳過測試資料夾本身與虛擬環境
        if module_name.startswith(('tests', '.venv', 'venv', '.cache', 'docker-compose', 'k8s')):
            continue

        try:
            # 動態 import 該模組
            importlib.import_module(module_name)

        except Exception as e:
            pytest.fail(f"模組庫中的檔案 [{module_name}] 無法被正常 import！\n錯誤原因: {e}")