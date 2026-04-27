# Owner: Emircan
# Branch: emircan/infra-kafka
# Purpose: Download Chicago Crime records from Socrata SODA API, save to data/raw/
# Input:  SODA API https://data.cityofchicago.org/resource/ijzp-q8t2.json
# Output: data/raw/chicago_crimes_sample.csv

import requests
import pandas as pd
import time

BASE_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
TARGET_ROWS = 100_000
OUTPUT_PATH = "data/raw/chicago_crimes_sample.csv"
COLUMNS = "id,case_number,date,block,iucr,primary_type,description,location_description,arrest,domestic,beat,district,ward,community_area,latitude,longitude,updated_on"

# TODO: implement paginated download using $limit and $offset params
