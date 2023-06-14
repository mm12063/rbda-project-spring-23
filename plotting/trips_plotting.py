import matplotlib.pyplot as plt
import pandas as pd

colors = ['#f0f921', '#fdca26', '#fb9f3a', '#ed7953', '#ed7953', '#bd3786', '#9c179e', '#9c179e', '#7201a8', '#46039f', '#0d0887']

df = pd.read_csv('top_10_trips_per_year/2021_top10/part-r-00000', header=None)
trips = [row[0].split(":")[0].strip() for _, row in df.iterrows()]
distances = [row[0].split(":")[1].strip() for _, row in df.iterrows()]
trips = [trip.replace("___", " -> ").replace("_", " ") for trip in trips]

fig, ax = plt.subplots(figsize=(30, 15))
ax.pie(distances, colors=colors, labels=trips, labeldistance=1.1, textprops={'fontsize': 24}, wedgeprops = { 'linewidth' : 3, 'edgecolor' : 'white' })

plt.savefig('plotting/output_plots/top_10_trips_2021.png')
plt.show()