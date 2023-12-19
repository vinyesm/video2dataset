spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.2 \
  --master local[*] \
  --deploy-mode client \
  sample_urls_pyspark.py --url_list="s3a://my-content/video/urls/one_million.parquet" --output_folder="s3a://my-content/video/urls/video_platform_dataset-sample/one_million/"
