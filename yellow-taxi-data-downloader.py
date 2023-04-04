import requests
import os

START_YR = 2015
END_YR = 2021

MAIN_DIR = 'parquet_files'
if not os.path.exists(MAIN_DIR):
    os.mkdir(MAIN_DIR)

total_byte_size = 0
for year in range(START_YR, END_YR+1):
    dir = f'{MAIN_DIR}/{year}'
    if not os.path.exists(dir):
        os.mkdir(dir)
        print("Directory '% s' created" % dir)
    for month in range(1, 13):
        month = f'{month:02d}'
        file_str = f'yellow_tripdata_{year}-{month}.parquet'
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{file_str}'
        r = requests.get(url, allow_redirects=True)
        open(f'{dir}/{file_str}', 'wb').write(r.content)
        file_size = os.path.getsize(f'{dir}/{file_str}')
        print(f'File downloaded: {file_str}')
        print(f'  File size: {file_size}')
        total_byte_size += file_size

size_gb = total_byte_size / (1024 * 1024 * 1024)
print(f'\nDataset total size: {round(size_gb, 2)}GB')
