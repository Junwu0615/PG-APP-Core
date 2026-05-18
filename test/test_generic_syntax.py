import os, pytest, compileall


def test_compile_all_python_files():
    """
    通用測試：強制編譯專案內所有 Python 檔案，確保沒有任何低級語法錯誤。
    """
    project_root = os.path.join(os.path.dirname(__file__), "..")

    # compile_dir 如果成功編譯所有檔案會回傳 True，有任一檔案語法出錯會回傳 False
    success = compileall.compile_dir(project_root, force=True, quiet=True)

    assert success, "專案中有 Python 檔案存在語法錯誤，無法順利編譯！"