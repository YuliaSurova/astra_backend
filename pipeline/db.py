from sqlalchemy import (
    BigInteger,
    Column,
    Date, 
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,  
    Text,
)
BIGINT_PK =BigInteger().with_variant(Integer, "sqlite")

metadata = MetaData()

macro_drivers_weekly = Table(
    "macro_drivers_weekly",
    metadata,
    Column("week",  Date, primary_key=True, comment = "Неделя (понедельник)"),
    Column("housing_starts_index", Float, comment = "Индекс жилищного строительства"),
    Column("lumber_price_index", Float, comment ="Индекс цен на пиломатериалы"),
    Column("mortgage_rate",Float, comment = "Ипотечная ставка, %"),
    Column("season_factor",Float, comment= "Сезонный коэффициент"),
)
building_materials_transactions = Table(
    "building_materials_transactions",
    metadata,
    Column("id", BIGINT_PK, primary_key=True, autoincrement=True, comment="Суррогатный ключ"),
    Column(
        "date",
        Date,
        ForeignKey("macro_drivers_weekly.week"),
        nullable=False, 
        comment="Неделя транзакции (понедельник)", 
    ),
    Column("year",  Integer),
    Column("month", Integer),
    Column("week_of_year", Integer),
    Column("region", String(64)),
    Column("product_category", String(64)),
    Column("sku",String(32)),
    Column("channel", String(32)),
    Column("customer_type", String(32)),
    Column("units",Integer),
    Column("unit_price", Float),
    Column("revenue", Float),
)
data_dictionary = Table(
    "data_dictionary",
    metadata,
    Column("column_name", String(64), primary_key=True),
    Column("type", String(32)),
    Column("description",Text),
)

def create_schema(engine, reset: bool = False) -> None:
    if reset:
        metadata.drop_all(engine)
    metadata.create_all(engine)