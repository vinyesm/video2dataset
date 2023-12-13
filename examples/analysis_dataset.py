""" This script is used to analyze dataset directory containing downloaded videos
and generate a csv file with the success rate per domain.

First, download videos using video2dataset script:
video2dataset --url_list="urls.parquet" --url_col="url" --output_folder="dataset" --input_format="parquet"

"""

import os
import json
import pandas as pd
from urllib.parse import urlparse


def list_directories(directory_path):
    directories = [d for d in os.listdir(directory_path) if os.path.isdir(os.path.join(directory_path, d))]
    return directories


def parse_json_file(file_path):
    with open(file_path, "r") as file:
        data = json.load(file)
        return {"url": data.get("url", ""), "key": data.get("key", ""), "status": data.get("status", "")}


def process_directory(directory_path):
    """Process a sub-directories containing json files of downloaded videos and return a dataframe with the data"""
    data_list = []
    print(list_directories(directory_path))
    for directory in list_directories(directory_path):
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(".json"):
                    file_path = os.path.join(root, file)
                    json_data = parse_json_file(file_path)
                    data_list.append(json_data)
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


def success_download_rate_per_domain(dataset_directory_path: str, output_filename: str):
    """Compute the success download rate per domain and save it to a csv file"""
    df = process_directory(dataset_directory_path)
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
    input_dir = "../dataset"
    out_filename = "success_rate.csv"
    success_download_rate_per_domain(input_dir, out_filename)
