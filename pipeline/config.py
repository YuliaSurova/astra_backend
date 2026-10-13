"""Конфигурация пайплайна: пути к данным и строка подключения к БД."""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data_set")))
DB_URL = os.getenv("DATABASE_URL", f"sqlite:///{BASE_DIR / 'astra.db'}")
