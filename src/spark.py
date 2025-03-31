from pyspark.sql import SparkSession
import os


def start_spark(number_cores: int = 2, memory_gb: int = 1, spark_config: dict = None):
    """
    Ініціалізація SparkSession для роботи з SQLite.

    :param number_cores: Кількість ядер для Spark.
    :param memory_gb: Обсяг пам’яті для Spark (у GB).
    :param spark_config: Додаткові конфігурації для Spark.
    :return: SparkSession та SparkContext.
    """
    print("Starting Spark initialization...")

    if spark_config is None:
        jar_path = "/Users/yaroslavsemenov/Desktop/ЛАБИ/lab3/lab4/sqlite-jdbc-3.49.1.0.jar"
        print(f"Checking if JAR file exists at: {jar_path}")
        if not os.path.exists(jar_path):
            raise FileNotFoundError(f"JAR file not found at: {jar_path}")
        spark_config = {
            "spark.jars": jar_path
        }
        print(f"Using spark_config: {spark_config}")

    # Ініціалізація SparkSession
    try:
        print("Building SparkSession...")
        spark = (SparkSession.builder
                 .appName("ChinookETL")
                 .config("spark.driver.memory", f"{memory_gb}g")
                 .config("spark.executor.memory", f"{memory_gb}g")
                 .config("spark.executor.cores", str(number_cores))
                 .config("spark.default.parallelism", str(number_cores * 2))
                 .config("spark.sql.shuffle.partitions", str(number_cores * 2))
                 .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
                 .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
                 .config("spark.sql.session.timeZone", "UTC")
                 .config("spark.sql.legacy.timeParserPolicy", "LEGACY"))

        print("Applying additional configurations...")
        for key, value in spark_config.items():
            print(f"Setting {key} = {value}")
            spark = spark.config(key, value)

        print("Creating SparkSession...")
        spark = spark.getOrCreate()
        print("SparkSession created successfully.")

        sc = spark.sparkContext
        print("SparkContext obtained successfully.")
        return spark, sc
    except Exception as e:
        print(f"Error initializing SparkSession: {e}")
        raise