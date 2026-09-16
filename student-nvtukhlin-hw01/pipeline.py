import time

from pyspark.sql import SparkSession, functions as F, types as T

CSV_PATH = "s3a://datalake/nvtukhlin/wikipageviews/staging_csv/wikipageviews.csv"
PARQUET_PATH = "s3a://datalake/nvtukhlin/wikipageviews/parquet"
TABLE = "lakehouse.nvtukhlin.wikipageviews"

KNOWN_NAMESPACES = {
    "Media", "Special", "Talk", "User", "User_talk", "Wikipedia", "Wikipedia_talk",
    "File", "File_talk", "MediaWiki", "MediaWiki_talk", "Template", "Template_talk",
    "Help", "Help_talk", "Category", "Category_talk", "Portal", "Portal_talk",
    "Draft", "Draft_talk", "Module", "Module_talk", "Book", "TimedText", "TimedText_talk",
}

CSV_SCHEMA = T.StructType([
    T.StructField("domain_code", T.StringType()),
    T.StructField("page_title", T.StringType()),
    T.StructField("count_views", T.IntegerType()),
    T.StructField("total_response_size", T.IntegerType()),
    T.StructField("event_date", T.DateType()),
    T.StructField("event_hour", T.IntegerType()),
])

QUERY = """
    SELECT namespace, count(*) AS pages, sum(count_views) AS total_views
    FROM {view}
    GROUP BY namespace
    ORDER BY total_views DESC
"""


def dir_size_mb(spark, path):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI.create(path), conf)
    summary = fs.getContentSummary(jvm.org.apache.hadoop.fs.Path(path))
    return summary.getLength() / 1024 / 1024


def file_size_mb(spark, path):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI.create(path), conf)
    status = fs.getFileStatus(jvm.org.apache.hadoop.fs.Path(path))
    return status.getLen() / 1024 / 1024


def with_namespace(df):
    prefix = F.regexp_extract(F.col("page_title"), r"^([A-Za-z_]+):", 1)
    is_known = prefix.isin(list(KNOWN_NAMESPACES))
    return df.withColumn("namespace", F.when(is_known, prefix).otherwise(F.lit("(article)")))


def validate(df):
    metrics = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("page_title").isNull() | (F.trim(F.col("page_title")) == ""), 1).otherwise(0))
         .alias("null_page_title"),
        F.sum(F.when(F.col("count_views").isNull() | (F.col("count_views") < 0), 1).otherwise(0))
         .alias("bad_count_views"),
        F.sum(F.when(F.col("domain_code") != "en", 1).otherwise(0)).alias("bad_domain"),
        F.sum(F.when(F.col("event_date").isNull() | ~F.col("event_hour").isin(1, 2), 1).otherwise(0))
         .alias("bad_time"),
    ).first()
    if metrics.rows == 0:
        raise ValueError("CSV пуст: staging-файл нужно пересобрать 02_prepare_csv.py")
    null_share = metrics.null_page_title / metrics.rows
    rejected = metrics.null_page_title + metrics.bad_count_views + metrics.bad_domain + metrics.bad_time
    print(f"Проверки: rows={metrics.rows:,}; доля NULL page_title={null_share:.6f}; "
          f"невозможные count_views={metrics.bad_count_views}; domain_code!=en={metrics.bad_domain}; "
          f"неверные event_date/event_hour={metrics.bad_time}")
    if rejected == 0:
        print("Отклонённых строк нет: воспроизводимая проверка с нулевым результатом "
              "(источник уже агрегирован MediaWiki, грязных строк не ожидается).")
    accepted = df.filter(
        F.col("page_title").isNotNull() & (F.trim(F.col("page_title")) != "")
        & F.col("count_views").isNotNull() & (F.col("count_views") >= 0)
        & (F.col("domain_code") == "en")
        & F.col("event_date").isNotNull() & F.col("event_hour").isin(1, 2)
    )
    accepted_count = accepted.count()
    if accepted_count + rejected != metrics.rows:
        raise ValueError("raw != accepted + rejected")
    print(f"raw={metrics.rows:,} = accepted={accepted_count:,} + rejected={rejected:,}")
    return accepted, accepted_count


def main():
    spark = (SparkSession.builder.appName("wikipageviews-pipeline")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        print("== CSV: явная схема 6 полей ==")
        df = (spark.read.schema(CSV_SCHEMA)
              .option("header", True).option("enforceSchema", False)
              .option("mode", "FAILFAST").option("nullValue", "")
              .option("escape", '"')
              .option("dateFormat", "yyyy-MM-dd").csv(CSV_PATH))
        df.printSchema()
        df = with_namespace(df)
        accepted, count = validate(df)

        print("\n== Parquet: partitionBy(event_hour) — две непересекающиеся порции ==")
        (accepted.write.mode("overwrite").option("compression", "snappy")
         .partitionBy("event_hour").parquet(PARQUET_PATH))
        parquet_df = spark.read.parquet(PARQUET_PATH)
        csv_types = {f.name: f.dataType for f in accepted.schema.fields}
        parquet_types = {f.name: f.dataType for f in parquet_df.schema.fields}
        if csv_types != parquet_types:
            raise ValueError("Имена или типы полей изменились после записи Parquet")
        print("Имена и типы всех полей CSV = Parquet (порядок колонок не сравниваем)")
        parquet_count = parquet_df.count()
        if parquet_count != count:
            raise ValueError(f"Количество строк изменилось: accepted={count}, Parquet={parquet_count}")
        print(f"Строки accepted = Parquet: {count:,}")
        csv_mb = file_size_mb(spark, CSV_PATH)
        pq_mb = dir_size_mb(spark, PARQUET_PATH)
        print(f"Размер staging CSV: {csv_mb:.2f} МиБ; Parquet: {pq_mb:.2f} МиБ")
        if pq_mb > 0:
            print(f"Отношение размеров CSV/Parquet: {csv_mb / pq_mb:.2f}")

        accepted.createOrReplaceTempView("wiki_csv")
        parquet_df.createOrReplaceTempView("wiki_parquet")
        print("\n== Один запрос: просмотры по namespace ==")
        print(QUERY.format(view="wiki_csv"))
        print("По 3 измерения каждого формата; первый замер отдельно.")
        results = {}
        for trial in range(1, 4):
            formats = ("csv", "parquet") if trial % 2 else ("parquet", "csv")
            for fmt in formats:
                started = time.perf_counter()
                result = spark.sql(QUERY.format(view=f"wiki_{fmt}")).collect()
                elapsed = time.perf_counter() - started
                key = tuple((r.namespace, r.pages, r.total_views) for r in result)
                if results and key != next(iter(results.values())):
                    raise ValueError(f"Результат запроса изменился: {fmt}")
                results[fmt] = key
                label = "первый замер" if trial == 1 else f"повтор {trial}"
                print(f"{fmt.upper()}, {label}: {elapsed:.3f} с; топ строка: {result[0].asDict()}")
        print("Результаты CSV = Parquet во всех замерах.")
        print("Время зависит от кэшей, прогрева и среды; преимущество формата по скорости не гарантируется.")

        print("\n== Iceberg: две непересекающиеся порции по event_hour ==")
        spark.sql("CREATE DATABASE IF NOT EXISTS lakehouse.nvtukhlin")

        def snapshot_ids():
            return {r.snapshot_id for r in spark.sql(f"SELECT snapshot_id FROM {TABLE}.snapshots").collect()}

        previous_ids = snapshot_ids() if spark.catalog.tableExists(TABLE) else set()

        print("Запись 1: CTAS event_hour = 1")
        spark.sql(f"""
            CREATE OR REPLACE TABLE {TABLE}
            USING iceberg
            PARTITIONED BY (event_hour)
            AS SELECT * FROM parquet.`{PARQUET_PATH}` WHERE event_hour = 1
        """)
        first_count = spark.table(TABLE).count()
        after_first_ids = snapshot_ids()
        print(f"После записи 1: {first_count:,} строк; новые snapshots: {sorted(after_first_ids - previous_ids)}")

        print("Запись 2: INSERT event_hour = 2")
        spark.sql(f"""
            INSERT INTO {TABLE}
            SELECT * FROM parquet.`{PARQUET_PATH}` WHERE event_hour = 2
        """)
        final = spark.table(TABLE).count()
        after_second_ids = snapshot_ids()
        print(f"После записи 2: {final:,} строк; новые snapshots: {sorted(after_second_ids - after_first_ids)}")
        if final != count:
            raise ValueError(f"Итог Iceberg {final} != accepted {count}")
        print("Проверка: сумма двух порций Iceberg = accepted.")

        print("\n== История snapshots ==")
        spark.sql(f"""
            SELECT snapshot_id, committed_at, operation, summary['total-records'] AS total_records
            FROM {TABLE}.snapshots ORDER BY committed_at
        """).show(100, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
