# Owner: Emircan
# Branch: emircan/infra-kafka
# Purpose: Download Chicago Crime records from Socrata SODA API, save to data/raw/
# Input:  SODA API https://data.cityofchicago.org/resource/ijzp-q8t2.json
# Output: data/raw/chicago_crimes_sample.csv

import argparse
import os
import time
import requests
import pandas as pd

DATASET_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
TARGET_ROWS = 100_000
OUTPUT_PATH = "data/raw/chicago_crimes_sample.csv"
COLUMNS = "id,case_number,date,block,iucr,primary_type,description,location_description,arrest,domestic,beat,district,ward,community_area,fbi_code,x_coordinate,y_coordinate,year,updated_on,latitude,longitude"

def fetch_chicago_crimes(limit: int, output_path: str, batch_size: int = 50000):

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    all_records = []

    offset = 0

    print(f"[INFO] Download started. Target records: {limit}")

    print(f"[INFO] Output path: {output_path}")

    while offset < limit:

        current_limit = min(batch_size, limit - offset)

        params = {
            "$limit":  current_limit,
            "$offset": offset,
            "$order":  "date DESC",
            "$select": COLUMNS,
        }

        print(f"[INFO] Fetching records {offset} - {offset + current_limit}")

        response = requests.get(DATASET_URL, params=params, timeout=60)

        if response.status_code != 200:

            raise Exception(

                f"API request failed. Status code: {response.status_code}, "

                f"Response: {response.text[:500]}"

            )

        batch = response.json()

        if not batch:

            print("[WARN] No more records returned from API.")

            break

        all_records.extend(batch)

        offset += current_limit

        print(f"[INFO] Total downloaded so far: {len(all_records)}")

        time.sleep(0.2)

    if not all_records:

        raise Exception("No data downloaded.")

    df = pd.DataFrame(all_records)

    print("[INFO] DataFrame created.")

    print(f"[INFO] Shape: {df.shape}")

    print(f"[INFO] Columns: {list(df.columns)}")

    df.to_csv(output_path, index=False)

    print(f"[SUCCESS] CSV saved to: {output_path}")

    print(f"[SUCCESS] Final row count: {len(df)}")

    print(f"[SUCCESS] Final column count: {len(df.columns)}")

def main():

    parser = argparse.ArgumentParser(

        description="Download Chicago Crime data sample from Chicago Open Data API."

    )

    parser.add_argument(

        "--limit",

        type=int,

        default=10000,

        help="Number of records to download. Example: 10000 or 100000"

    )

    parser.add_argument(

        "--output",

        type=str,

        default="data/raw/chicago_crimes_sample.csv",

        help="Output CSV file path."

    )

    parser.add_argument(

        "--batch-size",

        type=int,

        default=50000,

        help="Number of records per API request."

    )

    args = parser.parse_args()

    fetch_chicago_crimes(

        limit=args.limit,

        output_path=args.output,

        batch_size=args.batch_size

    )

if __name__ == "__main__":

    main()

# TODO: implement paginated download using $limit and $offset params
