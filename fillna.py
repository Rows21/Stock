import pandas as pd
import numpy as np
from tqdm import tqdm
import tushare as ts
from datetime import datetime, timedelta

token = 'c336245e66e2882632285493a7d0ebc23a2fbb7392b74e4b3855a222'
ts.set_token(token)
pro = ts.pro_api()

def pre_close(date_list):
    #df_close = pd.read_csv(path).iloc[:,1:]
    #df_close_adj = pd.read_csv(path_adj).iloc[:,1:]

    # 拉取第一天的数据集
    day0 = pro.daily(trade_date=date_list[0])
    day0 = day0.loc[:, ~day0.columns.str.contains('BJ')]
    # 创建原始表
    df_close = pd.DataFrame([[date_list[0]]+day0['close'].tolist()], columns=['trade_date']+day0['ts_code'].tolist())
    df_high = pd.DataFrame([[date_list[0]]+day0['high'].tolist()], columns=['trade_date']+day0['ts_code'].tolist())
    df_low = pd.DataFrame([[date_list[0]]+day0['low'].tolist()], columns=['trade_date']+day0['ts_code'].tolist())

    for i, date in enumerate(date_list[1:]):
        print(date)
        #print('执行复权值更新:')
        adj = pro.query('adj_factor',  trade_date=date)
        adj_pre = pro.query('adj_factor',  trade_date=date_list[i])
        feature = pd.merge(adj, adj_pre, on='ts_code', how='outer')
        feature = feature[~feature['ts_code'].str.contains('BJ')]
        filtered_feature = feature[feature['adj_factor_x'] - feature['adj_factor_y'] != 0]
            
        if filtered_feature.empty != True:
            print(filtered_feature['ts_code'].tolist())
            for tss in filtered_feature['ts_code'].tolist():
                if tss in df_close.columns:
                    adj_pre = filtered_feature[filtered_feature['ts_code'] == tss]['adj_factor_y']
                    adj_aft = filtered_feature[filtered_feature['ts_code'] == tss]['adj_factor_x']
                    df_close[tss] = df_close[tss] * (adj_pre / adj_aft).iloc[0]
                    df_high[tss] = df_high[tss] * (adj_pre / adj_aft).iloc[0]
                    df_low[tss] = df_low[tss] * (adj_pre / adj_aft).iloc[0]
                else:
                    df_close[tss] = pd.Series([np.nan] * len(df_close))
                    df_high[tss] = pd.Series([np.nan] * len(df_high))
                    df_low[tss] = pd.Series([np.nan] * len(df_low))
            
        # 在第一行插入新行
        daily = pro.daily(trade_date=date)
        daily = daily.loc[:, ~daily.columns.str.contains('BJ')]
        ts_today = daily['ts_code']
        new_row = pd.Series([np.nan] * len(df_close.columns), index=df_close.columns)
        df_close = pd.concat([pd.DataFrame([new_row]), df_close]).reset_index(drop=True)
        df_close.loc[0,'trade_date'] = int(date)
        df_high = pd.concat([pd.DataFrame([new_row]), df_high]).reset_index(drop=True)
        df_high.loc[0,'trade_date'] = int(date)
        df_low = pd.concat([pd.DataFrame([new_row]), df_low]).reset_index(drop=True)
        df_low.loc[0,'trade_date'] = int(date)

            # 不变的前复权价
            # new
            #elements_to_exclude = filtered_feature['ts_code'].to_list()
        #print('执行前复权更新:')
        for tsnew in ts_today:
            if tsnew not in df_close.columns:
                df_new = ts.pro_bar(ts_code=tsnew, adj='qfq', start_date=date_list[0], end_date=date)[['trade_date', 'close', 'high', 'low']]
                df_new['trade_date'] = df_new['trade_date'].astype(float)
                close_new = df_new[['trade_date', 'close']]
                close_new.columns = ['trade_date', tsnew]
                df_close = pd.merge(df_close, close_new, on='trade_date', how='left')
                high_new = df_new[['trade_date', 'high']]
                high_new.columns = ['trade_date', tsnew]
                df_high = pd.merge(df_high, high_new, on='trade_date', how='left')
                low_new = df_new[['trade_date', 'low']]
                low_new.columns = ['trade_date', tsnew]
                df_low = pd.merge(df_low, low_new, on='trade_date', how='left')
            else:
                df_close.loc[0,tsnew] = daily[daily['ts_code'] == tsnew]['close'].iloc[0]
                df_high.loc[0,tsnew] = daily[daily['ts_code'] == tsnew]['high'].iloc[0]
                df_low.loc[0,tsnew] = daily[daily['ts_code'] == tsnew]['low'].iloc[0]

    df_close.to_csv('pre_close_new.csv')
    df_high.to_csv('pre_high_new.csv')
    df_low.to_csv('pre_low_new.csv')
'''
p_close = pd.read_csv('pre_close.csv').iloc[:,1:]
na_mask = p_close.isna()
for columns in tqdm(p_close.columns):
    na_ind = [i for i in range(len(na_mask)) if na_mask[columns][i] == True]
    na_ind.reverse()
    for ind in na_ind:
        if ind +1 == len(p_close):
            p_close[columns][ind] = 0
        else:
            p_close[columns][ind] = p_close[columns][ind + 1]

p_close.to_csv('pre_close_adj.csv')
'''
if __name__ == '__main__':

    # 获取今天的日期
    time = datetime.now()
    formatted_time = time.strftime("%Y%m%d")

    # token
    token = 'c336245e66e2882632285493a7d0ebc23a2fbb7392b74e4b3855a222'
    ts.set_token(token)
    pro = ts.pro_api()

    # 检查数据
    # 获取去年的1月1日日期并格式化
    # last_year_date = datetime(time.year - 1, 1, 1).date()
    last_year_date = pd.to_datetime('2017-01-01')
    formatted_date = last_year_date.strftime("%Y%m%d")
    
    # 提取交易日历
    date_df = pro.trade_cal(exchange='SZSE', start_date=formatted_date, end_date=formatted_time)
    opendate_df = date_df[date_df['is_open'] == 1]

    # 提取当日交易数据
    today_df = pro.limit_list_d(trade_date=formatted_time, limit_type='U', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
    # 检查是否为空 如果为空 把上个交易日设置为当日
    if today_df.empty:
        formatted_time = opendate_df['cal_date'].iloc[0]
        # 获取去年的第一个交易日
        #last_year_date = datetime(int(formatted_time[:4]) - 1, 1, 1).date()
        #formatted_date = last_year_date.strftime("%Y%m%d")
        
        # 提取交易日历
        date_df = pro.trade_cal(exchange='SZSE', start_date=formatted_date, end_date=formatted_time)
        opendate_df = date_df[date_df['is_open'] == 1]

    # 生成交易日历
    date_list = list(opendate_df['pretrade_date'])
    date_list.reverse()
    pre_close(date_list)