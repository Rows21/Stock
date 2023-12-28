import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import tushare as ts
ts.set_token('2839da8e840b42925531966efa5ae652898578ede7471c43a1e7239b')
pro = ts.pro_api()
print(pro)
#s=我给你的股票代码csv

import pandas as pd
import time

# 假设df0、df1和df2是DataFrame，分别表示涨停数据、跌停数据和炸版数据

# 创建一个空列表，用于存储每天的数据
dataframes = []

# 获取从2020年到今天的每天数据，逐个添加到列表中
start_date = pd.to_datetime('2023-12-01')
end_date = pd.to_datetime('now')
date_range = pd.date_range(start=start_date, end=end_date, freq='D')  # 生成日期范围
for date in date_range:
    print(date)
    date_str = date.strftime('%Y%m%d')  # 将日期转换为字符串形式
    
    # 进行每分钟请求次数限制
    requests_count = 0
    
    df0 = pro.limit_list_d(trade_date=date_str, limit_type='U', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
    requests_count += 1
    time.sleep(0.5)  # 每次请求后延迟1秒
    
    df1 = pro.limit_list_d(trade_date=date_str, limit_type='D', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
    requests_count += 1
    time.sleep(0.5)  # 每次请求后延迟1秒
    
    df2 = pro.limit_list_d(trade_date=date_str, limit_type='Z', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
    requests_count += 1
    
    # 检查是否达到每分钟请求次数限制
    if requests_count == 3:
        time.sleep(60)  # 如果达到每分钟请求次数限制，延迟60秒
    
    combined_df = pd.concat([df0, df1, df2])
    dataframes.append(combined_df)

# 将所有DataFrame合并为一个大的DataFrame
merged_df = pd.concat(dataframes)

# 打印合并后的DataFrame
print(merged_df)
merged_df.to_csv('api.csv')