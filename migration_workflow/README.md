# Migration Workflow

`migration_workflow/run_migration.py` поднимает локальный временный TypeDB, импортирует старую БД, создаёт новую БД по новой схеме, запускает пользовательский Python-скрипт миграции и после успеха экспортирует новую БД обратно рядом с новой схемой.

## Что нужно локально

- установленный бинарник `typedb` в `PATH`
- Python-окружение с зависимостями `IB-graph-server/requirements.txt`

## Аргументы

- `--old-dump-dir` — папка со старым дампом, где лежат файлы `schema` и `data`
- `--old-templates` — папка со старыми TypeQL template-ами и `specification.json`
- `--new-schema` — папка с новым файлом `schema` или прямой путь к нему
- `--new-templates` — папка с новыми TypeQL template-ами и `specification.json`
- `--migration-script` — Python-скрипт с функцией `migrate(old_client, new_client)`

Дополнительно можно задать:

- `--investigation-name`
- `--typedb-bin`
- `--workspace-dir`
- `--keep-workspace`

## Контракт migration-script

Скрипт должен экспортировать функцию:

```python
def migrate(old_client, new_client) -> bool | None:
    ...
```

- `old_client` — `TypeDBClient`, подключённый к импортированной старой БД `old_DB`
- `new_client` — `TypeDBClient`, подключённый к новой БД `new_DB`, где уже применена новая схема и создано расследование
- `True` или `None` считаются успехом
- `False` считается ошибкой
- непойманное исключение тоже считается ошибкой

Примеры лежат в `migration_workflow/migration_example/`.

- `example_copy_migration.py` — минимальный общий пример
- `copy_data_db_to_db_copy.py` — пример 1-в-1 копии дампа
- `migrate_v0_1_to_v0_2_publish_all.py` — пример schema migration в `v0.2`

## Пример запуска

```bash
python migration_workflow/run_migration.py \
  --old-dump-dir ./legacy_dump \
  --old-templates ./db/v0.1 \
  --new-schema ./db/v0.2 \
  --new-templates ./db/v0.2 \
  --migration-script ./migration_workflow/migration_example/example_copy_migration.py
```

После успешной миграции экспорт новой БД будет записан в папку новой схемы:

- схема — в файл `schema` (или в тот путь, который был передан через `--new-schema`, если это был файл)
- данные — в файл `data`
