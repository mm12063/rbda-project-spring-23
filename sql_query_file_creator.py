import os

MAIN_DIR = 'sql_files'
if not os.path.exists(MAIN_DIR):
    os.mkdir(MAIN_DIR)

output_file = f"./{MAIN_DIR}/seasons.sql"

YEAR_START = 2015
YEAR_END = 2021

with open(output_file, "a") as file:
    file.write("\n")
    for year in range(YEAR_START, YEAR_END+1):
        command = f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = 3 OR pu_month = 4 OR pu_month = 5;"
        file.write(command)
        command = f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = 6 OR pu_month = 7 OR pu_month = 8;"
        file.write(command)
        command = f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = 9 OR pu_month = 10 OR pu_month = 11;"
        file.write(command)
        command = f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = 12 OR pu_month = 1 OR pu_month = 2;"
        file.write(command)
file.close()


