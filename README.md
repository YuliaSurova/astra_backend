# astra_backend

Бэкенд-проект с пайплайном загрузки датасета продаж стройматериалов в базу данных.

## Общая структура проекта

```
astra_backend/
├── data_set/                                # исходные CSV-файлы
│   ├── building_materials_transactions.csv  # фактовая таблица транзакций (340 тыс. строк)
│   ├── data_dictionary_materials.csv        # словарь данных (метаданные)
│   └── macro_drivers_weekly.csv             # недельные макроэкономические показатели
├── pipeline/                                # Python-пайплайн загрузки в БД
│   ├── __init__.py                          # пакет
│   ├── config.py                            # пути и строка подключения к БД
│   ├── db.py                                # схема БД (SQLAlchemy Core)
│   ├── load.py                              # логика загрузки (чтение, валидация, вставка)
│   └── run.py                               # точка входа (CLI)
├── requirements.txt                         # зависимости проекта
├── .gitignore                               # исключённые из Git файлы
└── TASK.md                                  # обоснование архитектурных решений
```

### Назначение модулей

| Модуль | Назначение |
|---|---|
| `pipeline/config.py` | Конфигурация: пути к данным и строка подключения к БД (через переменные окружения) |
| `pipeline/db.py` | Схема БД: три таблицы (справочник макроэкономики, фактовая таблица транзакций, словарь данных) |
| `pipeline/load.py` | Логика пайплайна: чтение CSV, проверка целостности, нормализация, загрузка в БД |
| `pipeline/run.py` | Точка входа: разбор аргументов командной строки и запуск пайплайна |

## Порядок действий для развертывания

### 1. Клонирование проекта

```bash
git clone <url-репозитория>
cd astra_backend
```
### 2. Создание виртуального окружения

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 4. Запуск пайплайна

Точка входа в пайплайн:

```bash
python -m pipeline.run                      # SQLite по умолчанию
python -m pipeline.run --reset              # пересоздать таблицы
python -m pipeline.run --db-url postgresql+psycopg2://user:pass@localhost:5432/astra
```

По умолчанию данные загружаются в SQLite-файл `astra.db` (сервер не нужен). Для PostgreSQL достаточно указать строку подключения через `--db-url` или переменную окружения `DATABASE_URL`.

### 5. Проверка результата

После запуска в корне проекта появится файл `astra.db`. Проверить данные:

```bash
python -c "
import sqlite3
conn = sqlite3.connect('astra.db')
print('Транзакций:', conn.execute('SELECT COUNT(*) FROM building_materials_transactions').fetchone()[0])
print('Макро-недель:', conn.execute('SELECT COUNT(*) FROM macro_drivers_weekly').fetchone()[0])
"
```

Ожидаемый результат: `Транзакций: 340000`, `Макро-недель: 313`.

## Схема БД

| Таблица | Роль | Ключ |
|---|---|---|
| `macro_drivers_weekly` | измерение: недельные макро-показатели + сезонный коэффициент | `week` (PK) |
| `building_materials_transactions` | фактовая таблица транзакций | `id` (PK), `date` → `macro_drivers_weekly.week` (FK) |
| `data_dictionary` | словарь данных (метаданные) | `column_name` (PK) |

Экзогенные макро-столбцы из CSV транзакций при загрузке выносятся в справочник `macro_drivers_weekly` (нормализация, устранение дублирования).

## Примечания

- База `astra.db` создаётся автоматически при запуске и не хранится в Git (в `.gitignore`);
- Для работы на другом компьютере достаточно склонировать проект, установить зависимости и запустить пайплайн;
- Обоснование архитектурных решений — в файле [`TASK.md`](TASK.md).