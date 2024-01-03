""" This script is used to analyze dataset directory containing downloaded videos
and generate a csv file with the success rate per domain.

First, download videos using video2dataset script:
video2dataset --url_list="urls.parquet" --url_col="url" --output_folder="dataset" --input_format="parquet"

"""

import os
import json
import pandas as pd
from urllib.parse import urlparse
import boto3


# def list_directories(directory_path):
#     directories = [d for d in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, d))]
#     return directories


def parse_json_file(s3, file_path):
    # with open(file_path, "r") as file:
    #     data = json.load(file)
    #     return {"url": data.get("url", ""), "key": data.get("key", ""), "status": data.get("status", "")}
    bucket, prefix = file_path[len("s3a://"):].split("/", 1)
    response = s3.get_object(Bucket=bucket, Key=prefix)
    json_data = response['Body'].read().decode('utf-8')
    # Parse the JSON data (modify this based on your actual parsing logic)
    parsed_data = json.loads(json_data)
    return {"url": parsed_data.get("url", ""), "key": parsed_data.get("key", ""), "status": parsed_data.get("status", "")}




def process_directory(s3, directory_path):
    """Process a sub-directories containing json files of downloaded videos and return a dataframe with the data"""
    data_list = []
    # for directory in list_directories(directory_path):
    #     for root, _, files in os.walk(directory):
    #         for file in files:
    #             if file.endswith(".json") and "stats" not in file:
    #                 file_path = os.path.join(root, file)
    #                 json_data = parse_json_file(file_path)
    #                 data_list.append(json_data)
    bucket, prefix = input_dir[len("s3a://"):].split("/", 1)
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
    files = [obj["Key"] for page in pages for obj in page['Contents']]
    # files2 = [f for f in files if f.endswith(".json") and "stats" not in f]
    count = 0
    #TODO: this is too slow, read files in parallel
    for file in files:
        if file.endswith(".json") and "stats" not in file:
            count +=1
            if count%1000==0:
                print(count)
            file_path = os.path.join(f"s3a://{bucket}", file)
            json_data = parse_json_file(s3, file_path)
            data_list.append(json_data)
    print("{count} json files")
    return pd.DataFrame(data_list)


def normalize_domain(domain):
    domain = domain.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def extract_domain(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    return normalize_domain(domain)


def success_download_rate_per_domain(s3, dataset_directory_path: str, output_filename: str):
    """Compute the success download rate per domain and save it to a csv file"""
    df = process_directory(s3, dataset_directory_path)
    df["domain"] = df["url"].apply(extract_domain)

    # group df by domain and count success and failure
    df2 = df.groupby(["domain", "status"])["url"].count().reset_index()

    # pivot table to have success and failure as columns
    df2 = df2.pivot(index="domain", columns="status", values="url").reset_index()
    df2 = df2.fillna(0)

    # add success rate column
    df2["success_rate"] = df2["success"] / (df2["success"] + df2["failed_to_download"])

    # save to csv
    df2.to_csv(output_filename, index=False)


if __name__ == "__main__":
    input_dir = "s3a://my-content/video/urls/video_platform_dataset-sample/one_million_dataset_v15/"
    out_filename = "s3a://my-content/video/urls/video_platform_dataset-sample/one_million_dataset_v15_analysis/success_rate.csv"
    s3 = boto3.client('s3')
    success_download_rate_per_domain(s3, input_dir, out_filename)
