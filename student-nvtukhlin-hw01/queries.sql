
-- 1. Что доступно через подключение.
SHOW CATALOGS;
SHOW TABLES IN lakehouse.nvtukhlin;

-- 2. Читаем подготовленные данные.
SELECT count(*) AS all_rows FROM lakehouse.nvtukhlin.wikipageviews;
SELECT page_title, namespace, count_views, event_hour
FROM lakehouse.nvtukhlin.wikipageviews
ORDER BY count_views DESC
LIMIT 10;

-- 3. Тот же вопрос, что в Spark (pipeline.py): просмотры по namespace.
-- Ожидание из Spark: (article) 2,322,536 страниц, 11,451,033 просмотров
SELECT namespace, count(*) AS pages, sum(count_views) AS total_views
FROM lakehouse.nvtukhlin.wikipageviews
GROUP BY namespace
ORDER BY total_views DESC;

-- 4. История таблицы: два snapshot'а — CTAS первого часа и INSERT второго.
SELECT snapshot_id, committed_at, operation,
       CAST(summary['total-records'] AS bigint) AS total_records
FROM lakehouse.nvtukhlin."wikipageviews$snapshots"
ORDER BY committed_at;

-- 5. Небольшой справочник в memory catalog: группировка namespace по смыслу
-- контента. Покрывает не все namespace — это намеренно, чтобы показать
-- обработку несопоставленных строк.
DROP TABLE IF EXISTS memory.default.namespace_groups;
CREATE TABLE memory.default.namespace_groups AS
SELECT * FROM (VALUES
    ('(article)', 'Основной контент'),
    ('Category',  'Категоризация'),
    ('File',      'Медиафайлы'),
    ('Talk',      'Обсуждения')
) AS t(namespace, content_group);
SELECT * FROM memory.default.namespace_groups ORDER BY namespace;

-- 6. Федеративный JOIN: Iceberg (lakehouse, физически в MinIO) + memory
-- (справочник, физически в памяти процесса Trino). Trino сам вычисляет план,
-- читает файлы Parquet Iceberg-таблицы и join'ит их со справочником без
-- переноса данных в отдельное хранилище — это и есть федерация.
SELECT coalesce(g.content_group, 'Не сопоставлен') AS content_group,
       count(*) AS pages,
       sum(w.count_views) AS total_views
FROM lakehouse.nvtukhlin.wikipageviews w
LEFT JOIN memory.default.namespace_groups g ON w.namespace = g.namespace
GROUP BY coalesce(g.content_group, 'Не сопоставлен')
ORDER BY total_views DESC;

-- 7. Проверки: ключи справочника уникальны (должно быть 0 строк).
SELECT namespace, count(*) AS key_count
FROM memory.default.namespace_groups
GROUP BY namespace
HAVING count(*) > 1;

-- Сохранность строк: JOIN не должен терять и не должен размножать строки.
SELECT (SELECT count(*) FROM lakehouse.nvtukhlin.wikipageviews) AS before_join,
       count(*) AS after_join,
       count_if(g.namespace IS NULL) AS unmatched
FROM lakehouse.nvtukhlin.wikipageviews w
LEFT JOIN memory.default.namespace_groups g ON w.namespace = g.namespace;
