import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

barWidth = 0.25
fig = plt.subplots(figsize=(25, 15))

df = pd.read_csv('plotting/input_data/seasons_output_yellow_taxi.txt', header=None)
yellow_taxi = [row[0] for _, row in df.iterrows()]

df = pd.read_csv('plotting/input_data/seasons_output_subway.txt', header=None)
subway = [row[0] for _, row in df.iterrows()]


lyft = [x * 0.3 for x in subway] #TODO update with real data


# Set position of bar on X axis
br1 = np.arange(len(yellow_taxi))
br2 = [x + barWidth for x in br1]
br3 = [x + barWidth for x in br2]

# # Make the plot
plt.bar(br1, yellow_taxi, color='yellow', width=barWidth,
        edgecolor='grey', label='Yellow Taxi')
plt.bar(br2, subway, color='navy', width=barWidth,
        edgecolor='grey', label='Subway')
plt.bar(br3, lyft, color='turquoise', width=barWidth,
        edgecolor='grey', label='Uber/Lyft')


plt.xticks(rotation=45, fontsize=19)
plt.xlabel('Seasons', fontweight='bold', fontsize=22)
plt.ylabel('Passengers', fontweight='bold', fontsize=22)
plt.xticks([r + barWidth for r in range(len(yellow_taxi))],
           ['Spr 15', 'Sum 15', 'Aut 15', 'Win 15',
            'Spr 16', 'Sum 16', 'Aut 16', 'Win 16',
            'Spr 17', 'Sum 17', 'Aut 17', 'Win 17',
            'Spr 18', 'Sum 18', 'Aut 18', 'Win 18',
            'Spr 19', 'Sum 19', 'Aut 19', 'Win 19',
            'Spr 20', 'Sum 20', 'Aut 20', 'Win 20',
            'Spr 21', 'Sum 21', 'Aut 21', 'Win 21',
            ])

plt.legend(fontsize="22")
plt.savefig('plotting/output_plots/seasons.png')

plt.show()

