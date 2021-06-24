from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

DATA_DIR = BASE_DIR / 'data'
CONFIGS_DIR = BASE_DIR / 'configs'
TASKS_DIR = DATA_DIR / 'task_data'
