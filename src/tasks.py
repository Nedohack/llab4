from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, year, month, dayofmonth, quarter, to_date


def load_sqlite_table(spark: SparkSession, jdbc_url: str, table_name: str) -> DataFrame:
    """
    Завантаження таблиці з SQLite бази даних у DataFrame.

    :param spark: SparkSession.
    :param jdbc_url: JDBC URL для підключення до SQLite.
    :param table_name: Назва таблиці.
    :return: DataFrame із даними таблиці.
    """
    return (spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", table_name)
	        .option("driver", "org.sqlite.JDBC")
            .load())


def create_dim_date(spark: SparkSession, jdbc_url: str) -> DataFrame:
    """
    Створення таблиці dim_date на основі даних із таблиці Invoice.

    :param spark: SparkSession.
    :param jdbc_url: JDBC URL для підключення до SQLite.
    :return: DataFrame для dim_date.
    """
    invoice_df = load_sqlite_table(spark, jdbc_url, "Invoice")
    dim_date = (invoice_df
                .select("InvoiceDate")
                .distinct()
                .withColumn("date_id", to_date(col("InvoiceDate")))
                .withColumn("year", year(col("InvoiceDate")))
                .withColumn("month", month(col("InvoiceDate")))
                .withColumn("day", dayofmonth(col("InvoiceDate")))
                .withColumn("quarter", quarter(col("InvoiceDate")))
                .drop("InvoiceDate"))
    return dim_date


def create_dim_customer(spark: SparkSession, jdbc_url: str) -> DataFrame:
    """
    Створення таблиці dim_customer на основі даних із таблиці Customer.

    :param spark: SparkSession.
    :param jdbc_url: JDBC URL для підключення до SQLite.
    :return: DataFrame для dim_customer.
    """
    customer_df = load_sqlite_table(spark, jdbc_url, "Customer")
    dim_customer = (customer_df
                    .select(
        col("CustomerId").alias("customer_id"),
        col("FirstName").cast("string").alias("first_name"),
        col("LastName").cast("string").alias("last_name"),
        col("Company").cast("string").alias("company"),
        col("Address").cast("string").alias("address"),
        col("City").cast("string").alias("city"),
        col("State").cast("string").alias("state"),
        col("Country").cast("string").alias("country"),
        col("PostalCode").cast("string").alias("postal_code"),
        col("Phone").cast("string").alias("phone"),
        col("Email").cast("string").alias("email")
    )
                    .withColumn("full_name", col("first_name") + " " + col("last_name"))
                    .drop("first_name", "last_name"))
    return dim_customer


def create_dim_artist(spark: SparkSession, jdbc_url: str) -> DataFrame:
    """
    Створення таблиці dim_artist на основі даних із таблиці Artist.

    :param spark: SparkSession.
    :param jdbc_url: JDBC URL для підключення до SQLite.
    :return: DataFrame для dim_artist.
    """
    artist_df = load_sqlite_table(spark, jdbc_url, "Artist")
    dim_artist = artist_df.select(
        col("ArtistId").alias("artist_id"),
        col("Name").cast("string").alias("artist_name")
    )
    return dim_artist


def create_dim_album(spark: SparkSession, jdbc_url: str, dim_artist: DataFrame) -> DataFrame:
    """
    Створення таблиці dim_album на основі даних із таблиці Album.

    :param spark: SparkSession.
    :param jdbc_url: JDBC URL для підключення до SQLite.
    :param dim_artist: DataFrame із таблиці dim_artist.
    :return: DataFrame для dim_album.
    """
    album_df = load_sqlite_table(spark, jdbc_url, "Album")
    dim_album = (album_df
    .join(dim_artist, album_df.ArtistId == dim_artist.artist_id, "inner")
    .select(
        col("AlbumId").alias("album_id"),
        col("Title").cast("string").alias("album_title"),
        col("artist_id"),
        col("artist_name")
    ))
    return dim_album


def create_dim_genre(spark: SparkSession, jdbc_url: str) -> DataFrame:
    """
    Створення таблиці dim_genre на основі даних із таблиці Genre.

    :param spark: SparkSession.
    :param jdbc_url: JDBC URL для підключення до SQLite.
    :return: DataFrame для dim_genre.
    """
    genre_df = load_sqlite_table(spark, jdbc_url, "Genre")
    dim_genre = genre_df.select(
        col("GenreId").alias("genre_id"),
        col("Name").cast("string").alias("genre_name")
    )
    return dim_genre


def create_dim_media_type(spark: SparkSession, jdbc_url: str) -> DataFrame:
    """
    Створення таблиці dim_media_type на основі даних із таблиці MediaType.

    :param spark: SparkSession.
    :param jdbc_url: JDBC URL для підключення до SQLite.
    :return: DataFrame для dim_media_type.
    """
    media_type_df = load_sqlite_table(spark, jdbc_url, "MediaType")
    dim_media_type = media_type_df.select(
        col("MediaTypeId").alias("media_type_id"),
        col("Name").cast("string").alias("media_type_name")
    )
    return dim_media_type


def create_dim_track(spark: SparkSession, jdbc_url: str, dim_album: DataFrame, dim_genre: DataFrame,
                     dim_media_type: DataFrame) -> DataFrame:
    """
    Створення таблиці dim_track на основі даних із таблиці Track.

    :param spark: SparkSession.
    :param jdbc_url: JDBC URL для підключення до SQLite.
    :param dim_album: DataFrame із таблиці dim_album.
    :param dim_genre: DataFrame із таблиці dim_genre.
    :param dim_media_type: DataFrame із таблиці dim_media_type.
    :return: DataFrame для dim_track.
    """
    track_df = load_sqlite_table(spark, jdbc_url, "Track")
    dim_track = (track_df
    .join(dim_album, track_df.AlbumId == dim_album.album_id, "inner")
    .join(dim_genre, track_df.GenreId == dim_genre.genre_id, "inner")
    .join(dim_media_type, track_df.MediaTypeId == dim_media_type.media_type_id, "inner")
    .select(
        col("TrackId").alias("track_id"),
        col("Name").cast("string").alias("track_name"),
        col("album_id"),
        col("album_title"),
        col("artist_id"),
        col("artist_name"),
        col("genre_id"),
        col("genre_name"),
        col("media_type_id"),
        col("media_type_name"),
        col("Composer").cast("string").alias("composer"),
        col("Milliseconds").cast("integer").alias("duration_ms"),
        col("Bytes").cast("integer").alias("size_bytes"),
        col("UnitPrice").cast("double").alias("unit_price")
    ))
    return dim_track


def create_fact_sales(spark: SparkSession, jdbc_url: str, dim_date: DataFrame, dim_customer: DataFrame,
                      dim_track: DataFrame) -> DataFrame:
    """
    Створення таблиці fact_sales на основі даних із таблиць Invoice і InvoiceLine.

    :param spark: SparkSession.
    :param jdbc_url: JDBC URL для підключення до SQLite.
    :param dim_date: DataFrame із таблиці dim_date.
    :param dim_customer: DataFrame із таблиці dim_customer.
    :param dim_track: DataFrame із таблиці dim_track.
    :return: DataFrame для fact_sales.
    """
    invoice_df = load_sqlite_table(spark, jdbc_url, "Invoice")
    invoice_item_df = load_sqlite_table(spark, jdbc_url, "InvoiceLine")
    fact_sales = (invoice_item_df
    .join(invoice_df, invoice_item_df.InvoiceId == invoice_df.InvoiceId, "inner")
    .join(dim_date, to_date(invoice_df.InvoiceDate) == dim_date.date_id, "inner")
    .join(dim_customer, invoice_df.CustomerId == dim_customer.customer_id, "inner")
    .join(dim_track, invoice_item_df.TrackId == dim_track.track_id, "inner")
    .select(
        col("InvoiceLineId").alias("sales_id"),
        col("date_id"),
        col("customer_id"),
        col("track_id"),
        col("Quantity").cast("integer").alias("quantity"),
        col("UnitPrice").cast("double").alias("unit_price"),
        (col("UnitPrice") * col("Quantity")).cast("double").alias("total_price")
    ))
    return fact_sales


def create_fact_sales_extended(spark: SparkSession, jdbc_url: str, dim_date: DataFrame, dim_customer: DataFrame,
                               dim_track: DataFrame) -> DataFrame:
    """
    Створення таблиці fact_sales_extended на основі даних із таблиць Invoice і InvoiceLine.

    :param spark: SparkSession.
    :param jdbc_url: JDBC URL для підключення до SQLite.
    :param dim_date: DataFrame із таблиці dim_date.
    :param dim_customer: DataFrame із таблиці dim_customer.
    :param dim_track: DataFrame із таблиці dim_track.
    :return: DataFrame для fact_sales_extended.
    """
    invoice_df = load_sqlite_table(spark, jdbc_url, "Invoice")
    invoice_item_df = load_sqlite_table(spark, jdbc_url, "InvoiceLine")
    fact_sales_extended = (invoice_item_df
    .join(invoice_df, invoice_item_df.InvoiceId == invoice_df.InvoiceId, "inner")
    .join(dim_date, to_date(invoice_df.InvoiceDate) == dim_date.date_id, "inner")
    .join(dim_customer, invoice_df.CustomerId == dim_customer.customer_id, "inner")
    .join(dim_track, invoice_item_df.TrackId == dim_track.track_id, "inner")
    .select(
        col("InvoiceLineId").alias("sales_id"),
        col("date_id"),
        col("year"),
        col("month"),
        col("day"),
        col("quarter"),
        col("customer_id"),
        col("full_name"),
        col("track_id"),
        col("track_name"),
        col("album_id"),
        col("album_title"),
        col("artist_id"),
        col("artist_name"),
        col("genre_id"),
        col("genre_name"),
        col("media_type_id"),
        col("media_type_name"),
        col("Quantity").cast("integer").alias("quantity"),
        col("UnitPrice").cast("double").alias("unit_price"),
        (col("UnitPrice") * col("Quantity")).cast("double").alias("total_price")
    ))
    return fact_sales_extended


def save_to_parquet(df: DataFrame, output_path: str) -> None:
    """
    Збереження DataFrame у форматі Parquet.

    :param df: DataFrame для збереження.
    :param output_path: Шлях для збереження.
    """
    df.write.mode("overwrite").parquet(output_path)
