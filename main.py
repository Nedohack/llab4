import os
from src.spark import start_spark
from src.tasks import (create_dim_date, create_dim_customer, create_dim_artist,
                      create_dim_album, create_dim_genre, create_dim_media_type,
                      create_dim_track, create_fact_sales, create_fact_sales_extended,
                      save_to_parquet)

def main():
    spark, _ = start_spark(
        number_cores=2,
        memory_gb=1
    )

    sqlite_path = os.path.abspath("input/chinook.sqlite")
    jdbc_url = f"jdbc:sqlite:{sqlite_path}"

    dim_date = create_dim_date(spark, jdbc_url)
    dim_customer = create_dim_customer(spark, jdbc_url)
    dim_artist = create_dim_artist(spark, jdbc_url)
    dim_album = create_dim_album(spark, jdbc_url, dim_artist)
    dim_genre = create_dim_genre(spark, jdbc_url)
    dim_media_type = create_dim_media_type(spark, jdbc_url)
    dim_track = create_dim_track(spark, jdbc_url, dim_album, dim_genre, dim_media_type)

    fact_sales = create_fact_sales(spark, jdbc_url, dim_date, dim_customer, dim_track)
    fact_sales_extended = create_fact_sales_extended(spark, jdbc_url, dim_date, dim_customer, dim_track)

    save_to_parquet(dim_date, "output/dim_date")
    save_to_parquet(dim_customer, "output/dim_customer")
    save_to_parquet(dim_artist, "output/dim_artist")
    save_to_parquet(dim_album, "output/dim_album")
    save_to_parquet(dim_genre, "output/dim_genre")
    save_to_parquet(dim_media_type, "output/dim_media_type")
    save_to_parquet(dim_track, "output/dim_track")
    save_to_parquet(fact_sales, "output/fact_sales")
    save_to_parquet(fact_sales_extended, "output/fact_sales_extended")

    spark.stop()

if __name__ == "__main__":
    main()