import os

# presto --catalog hive --schema default --file sql_files/XXXX.sql > sql_output/XXXX-output.txt

MAIN_DIR = 'sql_files'
if not os.path.exists(MAIN_DIR):
    os.mkdir(MAIN_DIR)

# YEAR_START = 2015
# YEAR_END = 2021
# MONTH_START = 1
# MONTH_END = 12
#
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


# FILE_NAME = "rush_hour"
# output_file = f"./{MAIN_DIR}/{FILE_NAME}.sql"
# if os.path.exists(output_file):
#     os.remove(output_file)
#
# with open(output_file, "a") as file:
#     for year in range(YEAR_START, YEAR_END+1):
#         for month in range(MONTH_START, MONTH_END+1):
#             command = f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = {month} AND htp_am = 1 OR htp_pm = 1; \n"
#             file.write(command)
# file.close()


# FILE_NAME = "covid_waves"

# YEAR_START = 2020
# YEAR_END = 2020
# MONTH_START = 3
# MONTH_END = 5

# Covid waves
# March to May 20
# Nov to May 21
#
# output_file = f"./{MAIN_DIR}/{FILE_NAME}.sql"
# if os.path.exists(output_file):
#     os.remove(output_file)
#
# with open(output_file, "a") as file:
#     for year in range(YEAR_START, YEAR_END+1):
#         for month in range(MONTH_START, MONTH_END+1):
#             command = f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = {month}; \n"
#             file.write(command)
#
#     command = f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = 2020  AND pu_month = 11; \n"
#     command += f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = 2020  AND pu_month = 12; \n"
#     command += f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = 2021  AND pu_month = 1; \n"
#     command += f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = 2021  AND pu_month = 2; \n"
#     command += f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = 2021  AND pu_month = 3; \n"
#     command += f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = 2021  AND pu_month = 4; \n"
#     command += f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = 2021  AND pu_month = 5; \n"
#     file.write(command)
#
# file.close()



# FILE_NAME = "taxi_zones"
#
# YEAR_START = 2015
# YEAR_END = 2021
# MONTH_START = 1
# MONTH_END = 12
#
# NUM_TAXI_ZONES = 263
#
# output_file = f"./{MAIN_DIR}/{FILE_NAME}.sql"
# if os.path.exists(output_file):
#     os.remove(output_file)
#
# with open(output_file, "a") as file:
#     for zone_id in range(1, NUM_TAXI_ZONES + 1):
#         for year in range(YEAR_START, YEAR_END+1):
#             for month in range(MONTH_START, MONTH_END+1):
#                 command = f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = {month} AND pu_loc_id = {zone_id}; \n"
#                 file.write(command)
#
# file.close()
#
#


# YEAR_START = 2015
# YEAR_END = 2021
# MONTH_START = 1
# MONTH_END = 12
#
# FILE_NAME = "trips_per_month"
# output_file = f"./{MAIN_DIR}/{FILE_NAME}.sql"
# if os.path.exists(output_file):
#     os.remove(output_file)
#
# with open(output_file, "a") as file:
#     for year in range(YEAR_START, YEAR_END+1):
#         for month in range(MONTH_START, MONTH_END+1):
#             command = f"SELECT COUNT(*) FROM yellow_taxi WHERE pu_year = {year} AND pu_month = {month}; \n"
#             file.write(command)
# file.close()


YEAR_START = 2015
YEAR_END = 2021
MONTH_START = 1
MONTH_END = 12

FILE_NAME = "cost_avg_per_month"
output_file = f"./{MAIN_DIR}/{FILE_NAME}.sql"
if os.path.exists(output_file):
    os.remove(output_file)

with open(output_file, "a") as file:
    for year in range(YEAR_START, YEAR_END+1):
        command = f"SELECT AVG(cost) FROM yellow_taxi WHERE pu_year = {year}; \n"
        file.write(command)
file.close()

print(f"presto --catalog hive --schema default --file sql_files/{FILE_NAME}.sql > sql_output/{FILE_NAME}_output.txt")



