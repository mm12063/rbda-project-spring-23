import matplotlib.pyplot as plt
import pandas as pd

df = pd.read_csv('sql_output/cost_avg_per_year_2_output.txt', header=None)
avg_costs_per_year = [row[0] for _, row in df.iterrows()]
months = ["J","F","M","A","M","J","J","A","S","O","N","D"]*7
years = [year for year in range(2015,2021+1)]

df = pd.read_csv('sql_output/dist_avg_per_year_2_output.txt', header=None)
avg_dist_per_year = [row[0] for _, row in df.iterrows()]


fig, ax1 = plt.subplots()

color = 'tab:red'
ax1.set_xlabel('Years')
ax1.set_ylabel('Avg Cost $', color='blue')
ax1.plot(years, avg_costs_per_year, color='blue')
ax1.tick_params(axis='y', labelcolor='blue')

ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis

color = 'tab:blue'
ax2.set_ylabel('Avg Dist miles', color='grey')  # we already handled the x-label with ax1
ax2.plot(years, avg_dist_per_year, color='grey')
ax2.tick_params(axis='y', labelcolor='grey')

# fig.tight_layout()  # otherwise the right y-label is slightly clipped
plt.title('Avg Cost vs Distance per year')
plt.savefig('plotting/output_plots/avg_cost_dist_per_year.png')
plt.show()
