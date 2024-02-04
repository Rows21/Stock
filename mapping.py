import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

restable = pd.read_csv('ResTable.tsv', sep='\t')
data = restable.pivot(index='date', columns='Labels', values='Values')
sns.heatmap(data=data)
plt.show()