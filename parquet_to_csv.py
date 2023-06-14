import pandas as pd
import os

pd.set_option('display.max_columns', None)

MAIN_DIR = 'parquet_files'

START_YR = 2020
END_YR = 2020

CSV_DIR = 'csv_files_small'
if not os.path.exists(CSV_DIR):
    os.mkdir(CSV_DIR)

BLOCK_SIZE = 1000
total_byte_size = 0

START_MONTH = 6
END_MONTH = 6

for year in range(START_YR, END_YR + 1):
    
    for month in range(START_MONTH, END_MONTH + 1):
        month_num = f'{month:02}'
        parq_file = f"{MAIN_DIR}/{year}/yellow_tripdata_{year}-{month_num}.parquet"
        df = pd.read_parquet(parq_file)
        total_rows = len(df.index)
        print(f"File: {parq_file}")
        print(f"Total rows to split: {total_rows}")

        csv_year_loc = f"./{CSV_DIR}/{year}/"
        file_path = f"{csv_year_loc}yellow_tripdata_{year}-{month_num}-"

        if not os.path.exists(csv_year_loc):
            os.mkdir(csv_year_loc)

        # counter = 0
        # for num in range(0, total_rows, BLOCK_SIZE):
        #     print(f"{counter} Splitting rows: {num} - {num + BLOCK_SIZE}")
        #     rows = df[num:num+BLOCK_SIZE]
        #     full_path = f"{file_path}{counter}.csv"
        #     rows.to_csv(full_path)
        #     counter += 1

        counter = 0
        for num in range(0, BLOCK_SIZE * 2, BLOCK_SIZE):
            print(f"{counter} Splitting rows: {num} - {num + BLOCK_SIZE}")
            rows = df[num:num + BLOCK_SIZE]
            full_path = f"{file_path}{counter}.csv"
            rows.to_csv(full_path)
            counter += 1
