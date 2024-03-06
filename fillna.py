import pandas as pd
import numpy as np
from tqdm import tqdm

pre_close = pd.read_csv('pre_close.csv').iloc[:,1:]
na_mask = pre_close.isna()
for columns in tqdm(pre_close.columns):
    na_ind = [i for i in range(len(na_mask)) if na_mask[columns][i] == True]
    na_ind.reverse()
    for ind in na_ind:
        if ind +1 == len(pre_close):
            pre_close[columns][ind] = 0
        else:
            pre_close[columns][ind] = pre_close[columns][ind + 1]

pre_close.to_csv('pre_close_adj.csv')