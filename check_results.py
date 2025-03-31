from pyspark.sql import SparkSession

def check_results():
    spark = (SparkSession.builder
             .appName("CheckResults")
             .getOrCreate())


    tables = [
        "dim_date",
        "dim_customer",
        "dim_artist",
        "dim_album",
        "dim_genre",
        "dim_media_type",
        "dim_track",
        "fact_sales",
        "fact_sales_extended"
    ]


    for table in tables:
        print(f"\n{table}:")
        df = spark.read.parquet(f"output/{table}")
        df.show(5)


    spark.stop()

if __name__ == "__main__":
    check_results()
