import pandas as pd
# For quick and dirty overview of the data

dist_dic = {}
for i in range(0, 125):
    idx = f'{i:03}'
    df = pd.read_csv(f"/Users/mitch/Desktop/NYU/classes/rbda/hadoop_setup4/output_yellow_taxi/part-m-00{idx}")

    for index, row in df.iterrows():
        pass_count = row['pu_loc_id']
        if pass_count not in dist_dic:
            dist_dic[pass_count] = 1
        else:
            dist_dic[pass_count] = dist_dic[pass_count] + 1
        if pass_count == "0" or pass_count == "0.0":
            print("0 found")

print(dist_dic)
