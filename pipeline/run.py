"""Точка входа в пайплайн.
Примеры запуска:
    python -m pipeline.run                      # SQLite по умолчанию
    python -m pipeline.run --reset              # пересоздать таблицы
    python -m pipeline.run --db-url postgresql+psycopg2://user:pass@localhost:5432/astra
"""
import argparse
import logging
import sys
from pathlib import Path
from .config import DATA_DIR, DB_URL
from .load import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Пайплайн загрузки датасета стройматериалов в БД"
    )
    parser.add_argument("--db-url", default=DB_URL, help="Строка подключения SQLAlchemy")
    parser.add_argument("--data-dir", default=str(DATA_DIR), help="Папка с CSV-файлами")
    parser.add_argument("--reset", action="store_true", help="Пересоздать таблицы перед загрузкой")
    parser.add_argument("--verbose", action="store_true", help="Подробный вывод")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
    )

    try:
        run_pipeline(db_url=args.db_url, data_dir=Path(args.data_dir), reset=args.reset)
    except Exception as exc: 
        logging.error("Пайплайн завершился с ошибкой: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()