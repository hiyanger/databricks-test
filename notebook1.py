# Databricks notebook source
# MAGIC %python
from pyspark.sql.functions import col

# 1. ファイルパス定義
cur_path    = "dbfs:/Volumes/test/default/test/test_cur_sample.csv"
master_path = "dbfs:/Volumes/test/default/test/redshift_node_master.csv"

# 2. CSV 読み込み
cur_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(cur_path)

master_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(master_path)

# 3. inner join（キー：lineItem/ResourceId）
joined_df = cur_df.join(
    master_df,
    cur_df["lineItem/ResourceId"] == master_df["lineItem/ResourceId"],
    "inner"
).drop(master_df["lineItem/ResourceId"])  # 重複カラムを削除

# 4. 結果プレビュー
display(joined_df)

# 5. Delta テーブルとして出力（Unity Catalog に登録）
output_table = "default.joined_cur_master"  # <catalog>.<schema>.<table>
joined_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(output_table)

# 6. 保存後の動作確認（Python API 版）
spark.sql(f"SELECT * FROM {output_table} LIMIT 10").show()
