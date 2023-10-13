import os


def dir_check(path: str):
    """Create ::path if not exist"""
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Created : {path}")
