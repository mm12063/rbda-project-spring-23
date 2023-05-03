import os

# presto --catalog hive --schema default --file sql_files/XXXX.sql > sql_output/XXXX-output.txt

MAIN_DIR = 'sql_files'
if not os.path.exists(MAIN_DIR):
    os.mkdir(MAIN_DIR)

YEAR_START = 2015
YEAR_END = 2021
MONTH_START = 1
MONTH_END = 12

# output_file = f"./{MAIN_DIR}/seasons.sql"
# if os.path.exists(output_file):
#     os.remove(output_file)
#
# with open(output_file, "a") as file:
#     for year in range(YEAR_START, YEAR_END+1):
#         command = f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = 3 OR pu_month = 4 OR pu_month = 5; \n"
#         command += f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = 6 OR pu_month = 7 OR pu_month = 8; \n"
#         command += f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = 9 OR pu_month = 10 OR pu_month = 11; \n"
#         command += f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = 12 OR pu_month = 1 OR pu_month = 2; \n"
#         file.write(command)
# file.close()


FILE_NAME = "rush_hour"
output_file = f"./{MAIN_DIR}/{FILE_NAME}.sql"
if os.path.exists(output_file):
    os.remove(output_file)

with open(output_file, "a") as file:
    for year in range(YEAR_START, YEAR_END+1):
        for month in range(MONTH_START, MONTH_END+1):
            command = f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = {month} AND htp_am = 1 OR htp_pm = 1; \n"
            file.write(command)
file.close()

print(f"presto --catalog hive --schema default --file sql_files/{FILE_NAME}.sql > sql_output/{FILE_NAME}_output.txt")



