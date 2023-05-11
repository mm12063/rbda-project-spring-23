import matplotlib.pyplot as plt
import pandas as pd

YEAR_START = 2015
YEAR_END = 2021

COLORS = ['#f0f921', '#fdca26', '#fb9f3a', '#ed7953', '#ed7953', '#bd3786', '#9c179e', '#9c179e', '#7201a8', '#46039f',
          '#0d0887']

for year in range(YEAR_START, YEAR_END + 1):
    df = pd.read_csv(f"pu_top_10_w_year/{year}_top10/part-r-00000", header=None)

    trips = [row[0].split(":")[0].strip() for _, row in df.iterrows()]
    distances = [row[0].split(":")[1].strip() for _, row in df.iterrows()]

    trips = [trip.replace("__", "/\n").replace("_", " ") for trip in trips]

    fig, ax = plt.subplots(figsize=(15, 9))
    ax.pie(distances, colors=COLORS, labels=trips, labeldistance=1.1, textprops={'fontsize': 22},
           wedgeprops = { 'linewidth' : 3, 'edgecolor' : 'white' })
    plt.savefig(f"plotting/output_plots/pu_locations/top_10_pu_{year}.png")
    plt.show()
