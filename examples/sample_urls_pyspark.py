
from pyspark.sql.functions import expr, col
from pyspark.sql import SparkSession
import fire


def domain_count(spark : SparkSession, url_list: str, output_folder: str):
    df = spark.read.parquet(url_list)
    result_df = df.withColumn("domain", expr("parse_url(url, 'HOST')")) \
        .withColumn("domain", expr("lower(coalesce(substring_index(domain, 'www.', -1), domain))")) \
        .groupBy("domain").count().orderBy(col("count").desc())
    result_df.repartition(1).write.parquet(output_folder, mode="overwrite", compression="snappy")
    return


          
if __name__ == "__main__":
    """
    Usage: sample_urls_large_pyspark --url_list=[url_list] --output_folder=[output_folder]
    Example: sample_urls_large_pyspark --url_list="s3a://my-content/video/one_million.parquet" --output_folder="s3a://my-content/video/urls/video_platform_dataset-stats"

    Counts the number of videos per domain in a dataset of urls and save the result to a parquet file
    """
    spark = SparkSession.builder.appName("ExtremeWeather").getOrCreate()
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
    fire.Fire(domain_count)