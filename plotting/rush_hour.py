import matplotlib.pyplot as plt
import pandas as pd

YEAR_START = 2015
YEAR_END = 2020

months = [f"{month}/1/{year}" for year in range(YEAR_START, YEAR_END + 1) for month in range(1, 13)]
months = pd.to_datetime(months)

df = pd.read_csv('sql_output/trips_outside_rush_hour_am_pm_output.txt', header=None)
outside_rush = [row[0] for _, row in df.iterrows()]

df = pd.read_csv('sql_output/rush_hour_output.txt', header=None)
during_rush_hour = [row[0] for _, row in df.iterrows()]

fig, ax = plt.subplots(figsize=(8, 6))
ax.plot(months, outside_rush, label="Outside Rush Hours")
ax.plot(months, during_rush_hour, label="During Rush Hours")
plt.legend(loc="center right")

plt.title('Trips during and outside rush hour')
plt.savefig('plotting/output_plots/during_and_outside_rush_hour.png')
plt.show()



df = pd.read_csv('sql_output/rush_hour_am_pm_output.txt', header=None)
am = df.iloc[0::2, :]
pm = df.iloc[1::2, :]

fig, ax = plt.subplots(figsize=(8, 6))
ax.plot(months, am, label="AM Rush Hour")
ax.plot(months, pm, label="PM Rush Hour")
plt.legend(loc="upper right")

plt.title('Comparing AM/PM Rush hours')
plt.savefig('plotting/output_plots/am_pm_rush_hours.png')
plt.show()

