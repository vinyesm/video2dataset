spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.2 \
  --master local[*] \
  --deploy-mode client \
  sample_urls_pyspark.py