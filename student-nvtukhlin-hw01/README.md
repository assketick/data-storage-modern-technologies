# ДЗ1 — nvtukhlin — Wikimedia Pageviews → Lakehouse

Источник, вопрос, измерения и выводы — в [`report.md`](report.md). Поля и типы — в [`schema.md`](schema.md).

## Что делает пайплайн

Пайплайн переводит два часа официального дампа Wikimedia Pageviews (`en.wikipedia`,
2015-05-01, часы 01:00 и 02:00 UTC) в воспроизводимую Iceberg-таблицу, читаемую и Spark, и Trino,
плюс федеративный SQL с небольшим справочником в Trino `memory` catalog.

## Предварительно

Нужен уже поднятый локальный стенд из `lecture_01_student/infra/`, бакеты `raw`/`datalake` в MinIO, каталоги
`lakehouse`/`memory` в Trino.

## Секреты

Скрипты `01_upload_raw.py`/`02_prepare_csv.py` не хранят пароль MinIO в коде — читают его из
переменных окружения `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY`. Перед запуском скопируйте
[`.env.example`](.env.example) в `infra/.env` и подставьте реальные значения из `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` вашего локального `infra/docker-compose.yml`

## Как воспроизвести с чистого стенда

1. Скачать два файла-источника:
   ```bash
   curl -O https://dumps.wikimedia.org/other/pageviews/2015/2015-05/pageviews-20150501-010000.gz
   curl -O https://dumps.wikimedia.org/other/pageviews/2015/2015-05/pageviews-20150501-020000.gz
   ```
2. Положить оба файла в `infra/data/hw01/`, а три
   скрипта этой папки (`01_upload_raw.py`, `02_prepare_csv.py`, `pipeline.py`) — в
   `infra/scripts/hw01/`.
3. Из `infra/` по очереди, проверяя вывод каждой команды:
   ```bash
   docker compose exec spark spark-submit /scripts/hw01/01_upload_raw.py
   docker compose exec spark spark-submit /scripts/hw01/02_prepare_csv.py
   docker compose exec spark spark-submit /scripts/hw01/pipeline.py
   ```
4. SQL-часть (Trino), из `infra/`:
   ```bash
   docker compose exec -T trino trino < queries.sql
   ```
   файл `queries.sql` этой папки — копия `infra/trino/scripts/hw01_queries.sql`.

## Ожидаемые контрольные числа

- Raw: 2 объекта `.gz` по ~42 МиБ каждый в `s3://raw/nvtukhlin/wikipageviews/...`.
- Staging CSV: 2,600,099 строк (1,294,434 + 1,305,665), 105.38 МиБ.
- DQ: 0 отклонённых строк (источник уже агрегирован MediaWiki).
- Parquet: 32.87 МиБ (сжатие ×3.21 к CSV).
- Iceberg `lakehouse.nvtukhlin.wikipageviews`: после 1-й записи 1,294,434 строк, после 2-й —
  2,600,099; 2 новых snapshot'а (`overwrite`, `append`).
- Namespace-агрегация в Spark и Trino совпадает: `(article)` — 2,322,536 страниц, 11,451,033
  просмотров (топ).
- Федеративный JOIN: `before_join = after_join = 2,600,099`, `unmatched = 103,759`.

## Структура

```
nvtukhlin-hw01/
├── README.md            # этот файл
├── report.md            # источник, DQ, измерения, Iceberg, федерация, вывод
├── schema.md             # поля, типы, смысл, nullable
├── pipeline.py           # явная схема -> DQ -> Parquet -> Iceberg (2 порции)
├── 01_upload_raw.py      # заливка неизменённых .gz в raw
├── 02_prepare_csv.py     # раскодирование не-CSV источника в табличный CSV (staging)
├── queries.sql           # проверочные, аналитический и федеративный SQL (Trino)
└── evidence/
    ├── compose-ps.txt
    ├── object-listing.txt
    ├── spark-result.txt
    └── trino-result.txt
```

## Ограничения решения

Однодневный/двухчасовой срез не подходит для суточных или недельных выводов о поведении
читателей — это демонстрация pipeline, а не продуктовая аналитика. Подробнее — раздел
«Известные ограничения» в `report.md`.
