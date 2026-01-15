import os

IGNORE_DIRS = {".git", "venv", "__pycache__"}

def list_all_files(project_root):
    for root, dirs, files in os.walk(project_root):
        dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
        for file in files:
            print(os.path.join(root, file))

if __name__ == "__main__":
    list_all_files(os.getcwd())