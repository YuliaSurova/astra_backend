import io
import logging
import time
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.engine import Engine
from .config import DATA_DIR, DB_URL
from .db import create_schema

logger = logging.getLogger(__name__)

TRANSACTIONS_FILE ="building_materials_transactions.csv"
MACRO_FILE="macro_drivers_weekly.csv"
DICTIONARY_FILE= "data_dictionary_materials.csv"
EXOGENOUS_COLUMNS= ["housing_starts_index", "lumber_price_index", "mortgage_rate"]
TRANSACTION_COLUMNS = {
    "date", "year", "month", "week_of_year", "region", "product_category",
    "sku", "channel", "customer_type", "units", "unit_price", "revenue",
}
MACRO_COLUMNS= {
    "week", "housing_starts_index", "lumber_price_index", "mortgage_rate", "season_factor",
}
DICTIONARY_COLUMNS = {"column", "type", "description"}

def _check_columns(df: pd.DataFrame, required: set, source: str) -> None:
    missing = required -set(df.columns)
    if missing:
        raise ValueError(f"В файле {source} отсутствуют столбцы: {sorted(missing)}")
def read_transactions(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    _check_columns(df, TRANSACTION_COLUMNS, path.name)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    return df

def read_macro(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    _check_columns(df, MACRO_COLUMNS, path.name)
    df["week"] = pd.to_datetime(df["week"]).dt.date
    return df

def read_dictionary(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    _check_columns(df, DICTIONARY_COLUMNS, path.name)
    return df.rename(columns={"column": "column_name"})

def validate_fk(transactions: pd.DataFrame, macro: pd.DataFrame) -> None:
    macro_weeks = set(macro["week"])
    missing = sorted(set(transactions["date"]) - macro_weeks)
    if missing:
        raise ValueError(
            f"Нарушение целостности: {len(missing)} недель транзакций нет в "
            f"macro_drivers_weekly. Примеры: {missing[:5]}"
        )
    logger.info(
        "Проверка целостности пройдена: все даты транзакций есть в macro_drivers_weekly"
    )

def _records_in_chunks(df: pd.DataFrame, chunksize: int):
    for i in range(0, len(df), chunksize):
        chunk = df.iloc[i : i + chunksize]
        yield chunk.where(pd.notnull(chunk), None).to_dict(orient="records")

def _copy_insert(engine: Engine, table_name: str, df: pd.DataFrame) -> int:
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    columns = ", ".join(df.columns)
    with engine.raw_connection() as conn:
        with conn.cursor() as cur:
            cur.copy_expert(f"COPY {table_name} ({columns}) FROM STDIN WITH CSV", buf)
        conn.commit()
    return len(df)

def _executemany_insert(engine: Engine, table_name: str, df: pd.DataFrame, chunksize: int) -> int:
    columns = ", ".join(df.columns)
    placeholders = ", ".join(f":{col}" for col in df.columns)
    stmt = text(f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})")
    total = 0
    with engine.begin() as conn:
        for records in _records_in_chunks(df, chunksize):
            conn.execute(stmt, records)
            total += len(records)
    return total

def bulk_insert(engine: Engine, table_name: str, df: pd.DataFrame, chunksize: int = 10_000) -> int:
    if engine.dialect.name == "postgresql":
        return _copy_insert(engine, table_name, df)
    return _executemany_insert(engine, table_name, df, chunksize)


def _enable_sqlite_foreign_keys(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    @event.listens_for(engine, "connect")
    def _set_pragma(dbapi_connection, _connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def _table_has_rows(engine: Engine, table_name: str) -> bool:
    if not inspect(engine).has_table(table_name):
        return False
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
    return count > 0


def run_pipeline(db_url: str = DB_URL, data_dir: Path = DATA_DIR, reset: bool = False) -> None:
    started = time.perf_counter()
    engine = create_engine(db_url)
    _enable_sqlite_foreign_keys(engine)

    logger.info("Чтение CSV-файлов из %s", data_dir)
    transactions = read_transactions(data_dir / TRANSACTIONS_FILE)
    macro = read_macro(data_dir / MACRO_FILE)
    dictionary = read_dictionary(data_dir / DICTIONARY_FILE)
    logger.info(
        "Прочитано: транзакций=%d, макро-недель=%d, записей словаря=%d",
        len(transactions), len(macro), len(dictionary),
    )

    validate_fk(transactions, macro)
    transactions = transactions.drop(columns=EXOGENOUS_COLUMNS)

    if not reset and _table_has_rows(engine,"macro_drivers_weekly"):
        raise RuntimeError(
            "Таблицы уже содержат данные. Запустите пайплайн с флагом --reset, "
            "чтобы пересоздать схему и загрузить данные заново."
        )

    logger.info("Создание схемы БД (reset=%s)", reset)
    create_schema(engine, reset=reset)
    logger.info("Загрузка macro_drivers_weekly ...")
    n_macro = bulk_insert(engine, "macro_drivers_weekly", macro)
    logger.info("Загружено строк: %d", n_macro) 

    logger.info("Загрузка building_materials_transactions ...")
    n_tx = bulk_insert(engine, "building_materials_transactions", transactions)
    logger.info("Загружено строк: %d", n_tx)

    logger.info("Загрузка data_dictionary ...")
    n_dict = bulk_insert(engine, "data_dictionary", dictionary)
    logger.info("Загружено строк: %d", n_dict)

    with engine.connect() as conn:
        total_tx = conn.execute(
            text("SELECT COUNT(*) FROM building_materials_transactions")
        ).scalar() 
        total_macro = conn.execute(text("SELECT COUNT(*) FROM macro_drivers_weekly")).scalar()
        joined = conn.execute(
            text(
                "SELECT COUNT(*) FROM building_materials_transactions t "
                "JOIN macro_drivers_weekly m ON t.date = m.week"
            )
        ).scalar()

    elapsed = time.perf_counter() - started
    logger.info(
        "Готово за %.2f с. Итог: транзакций=%d, макро-недель=%d, "
        "строк после JOIN=%d (должно совпадать с числом транзакций)",
        elapsed, total_tx, total_macro, joined,
    )