import os
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DATASETS = {
    "yellow": "yellow_tripdata",
    "hvfhv": "fhvhv_tripdata"
}

YEARS = range(2019, 2024)   # 2019–2023
MONTHS = range(1, 13)

BASE_DIR = "../data/raw"

for dataset, prefix in DATASETS.items():
    for year in YEARS:
        year_dir = os.path.join(BASE_DIR, dataset, str(year))
        os.makedirs(year_dir, exist_ok=True)

        for month in MONTHS:
            month_str = f"{month:02d}"
            filename = f"{prefix}_{year}-{month_str}.parquet"
            url = f"{BASE_URL}/{filename}"
            filepath = os.path.join(year_dir, filename)

            if os.path.exists(filepath):
                print(f"Already exists: {filename}")
                continue

            print(f"Downloading: {filename}")
            try:
                response = requests.get(url, timeout=30)
                if response.status_code == 200:
                    with open(filepath, "wb") as f:
                        f.write(response.content)
                else:
                    print(f"Not available: {filename}")
            except Exception as e:
                print(f"Error downloading {filename}: {e}")

