import os
import sys
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, sum as _sum
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    spark = SparkSession.builder.appName("WeeklyAggregation").getOrCreate()

    date_str = sys.argv[1]
    input_dir = sys.argv[3]
    output_tmp_dir = sys.argv[5]
    output_dir = sys.argv[7]

    os.makedirs(output_tmp_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)

    dt = datetime.strptime(date_str, '%Y-%m-%d')

    start_date = dt - timedelta(days=7)
    dates = [start_date + timedelta(n) for n in range(7)]
    date_strings = [date.strftime('%Y-%m-%d') for date in dates]

    existing_parquet_files = set([
        os.path.splitext(f)[0] for f in os.listdir(output_tmp_dir)
        if f.endswith('.parquet')
    ])
    missing_files = set(date_strings) - existing_parquet_files

    for current_date in missing_files:
        input_filepath = os.path.join(input_dir, f"{current_date}.csv")
        output_tmp_filepath = os.path.join(output_tmp_dir, f"{current_date}.parquet")

        df = spark.read.csv(input_filepath, header=False)

        df = df.withColumnRenamed("_c0", "email") \
               .withColumnRenamed("_c1", "action") \
               .withColumnRenamed("_c2", "dt")

        df = df.withColumn("dt", to_date(col("dt"), "yyyy-MM-dd")) \
               .filter(col("dt") == lit(current_date))

        agg_df = df.groupBy("email").pivot("action", ["CREATE", "READ", "UPDATE", "DELETE"]).count().na.fill(0)

        for action in ["CREATE", "READ", "UPDATE", "DELETE"]:
            if action in agg_df.columns:
                agg_df = agg_df.withColumnRenamed(action, f"{action.lower()}_count")
            else:
                agg_df = agg_df.withColumn(f"{action.lower()}_count", lit(0).cast(IntegerType()))

        agg_df.write.parquet(output_tmp_filepath, mode='overwrite')

    existing_agg_files = [
        os.path.join(output_tmp_dir, f"{date_str}.parquet") for date_str in date_strings
        if os.path.exists(os.path.join(output_tmp_dir, f"{date_str}.parquet"))
    ]

    weekly_df = spark.read.parquet(*existing_agg_files)

    weekly_agg_df = weekly_df.groupBy("email").agg(
        _sum("create_count").cast(IntegerType()).alias("create_count"),
        _sum("read_count").cast(IntegerType()).alias("read_count"),
        _sum("update_count").cast(IntegerType()).alias("update_count"),
        _sum("delete_count").cast(IntegerType()).alias("delete_count")
    )

    output_filename = os.path.join(output_dir, f"{date_str}.csv")

    weekly_agg_df.write.csv(output_filename, header=True, mode='overwrite')

    spark.stop()

    logging.info(f"Final aggregated data written to {output_filename}")