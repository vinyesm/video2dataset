from video2dataset import video2dataset
import shutil
import os
from pyspark.sql import SparkSession  # pylint: disable=import-outside-toplevel

from pyspark import SparkConf, SparkContext

def create_spark_session():
    # this must be a path that is available on all worker nodes
    pex_file = "/home/ubuntu/video2dataset/video2dataset.pex"

    os.environ['PYSPARK_PYTHON'] = pex_file
    spark = (
        SparkSession.builder
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2") \
        .config("spark.submit.deployMode", "client") \
        # .config("spark.files", pex_file)\
        .config("spark.task.maxFailures", "100") \
        .config("spark.driver.memory", "16G") \
        .master("local[16]") \
        .appName("spark-stats")\
        .getOrCreate()
    )
    return spark

output_dir = "s3://my-content/video/urls/video_platform_dataset-sample/one_million_dataset_v23"


spark = create_spark_session()

url_list = "s3://my-content/video/urls/video_platform_dataset-sample/one_million"

video2dataset(
        url_list=url_list,
        output_folder=output_dir,
        input_format="parquet",
        url_col="url",
        enable_wandb=True,
    wandb_project="video-download",
    tmp_dir="./tmp"
)