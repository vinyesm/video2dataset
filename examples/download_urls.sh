#!/bin/bash

# https://wandb.ai/authorize
WANDB_API_KEY=""

video2dataset --url_list="s3://my-content/video/urls/video_platform_dataset-sample/one_million/" --url_col="url" --output_folder="s3://my-content/video/urls/video_platform_dataset-sample/one_million_dataset_v17" --input_format="parquet" --enable_wandb="True" --wandb_project="video-download"  --tmp_dir="./tmp"
