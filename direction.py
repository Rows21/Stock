import pandas as pd
import numpy as np
from collections import Counter
class Direction():
    def __init__(self) -> None:
        # 创建历史数据空表
        self.df_upratio = pd.DataFrame(None, columns = ['date', '>100', '80-100', '60-80', '50-60', '40-50', '30-40', '20-30','all','bar','sump','w_sump'])
        self.short = pd.DataFrame(None, columns = ['date', '1', '2', '3', '4', '5', '6', '7', '8', '>9', '>7','semo','bar'])
        
    
    def get_hist(self, date_list, df_close, df_close_adj, label):
        
        date_list.reverse()
        date_list = date_list[date_list.index('20200103'):] # start from 2020

        for i, date in enumerate(date_list[20:]):
            
            
            date_20 = date_list[i]
            # 拉取1-20天的交易数据
            #today = pro.daily(trade_date=date)
            day_2 = []
            ind_today = int(df_close[df_close['trade_date'] == int(date)].index.values)
            ind_pre = int(df_close[df_close['trade_date'] == int(date_20)].index.values)
            df_20 = df_close_adj.iloc[ind_today:(ind_pre+1),]

            rps_df = None            
            df_20.iloc[0] = df_close[df_close['trade_date'] == int(date)]

            # 涨跌幅
            for day in [2,3,5,10,20]:
                chgi = df_20.iloc[0,1:]/df_20.iloc[day,1:]
                if rps_df is None:
                    rps_df = pd.DataFrame(chgi.index, columns = ['证券代码'])
                rps_df['chg' + str(day)] = chgi.tolist()
            
            rps_df = rps_df.dropna()

            # RPS
            for day in [2,3,5,10,20]:
                ranki = rps_df['chg' + str(day)].rank(ascending=False)
                rps_df['rps'+str(day)] = (1-ranki/ranki.shape[0])*ranki
            
            mainrps_params = [0.05, 0.05, 0.1, 0.15, 0.65]
            shortrps_params = [0.2, 0.5, 0.3]
            main_rps = [sum(rps_df.iloc[i,-5:] * mainrps_params) for i in range(rps_df.shape[0])]
            short_rps = [sum(rps_df.iloc[i,-5:-2] * shortrps_params) for i in range(rps_df.shape[0])]
            rps_df['main'], rps_df['short'] = main_rps, short_rps

            rps_df: pd.DataFrame = pd.merge(rps_df,label,on='证券代码',how='left')
            #rps_df = rps_df.dropna()
            main_top_300, short_top_300 = rps_df.nlargest(300, 'main'), rps_df.nlargest(300, 'short')
            
            # 主线上榜
            counter11 = Counter(main_top_300['一阶1']) + Counter(main_top_300['一阶2'])
            counter21 = Counter(main_top_300['模糊1']) + Counter(main_top_300['模糊2'])
            counter_main = counter11 + Counter({key: value * 0.3 for key, value in counter21.items()})
            del counter_main[0]
            counter_dict = dict(counter_main)
            style_main = pd.DataFrame.from_dict(counter_dict, orient='index', columns=['count'])

            counter12 = Counter(main_top_300['二阶1']) + Counter(main_top_300['二阶2']) + Counter(main_top_300['二阶3'])
            counter22 = Counter(main_top_300['模糊1.1']) + Counter(main_top_300['模糊2.1'])
            counter_main = counter12 + Counter({key: value * 0.3 for key, value in counter22.items()})
            del counter_main[0]
            counter_dict = dict(counter_main)
            field_main = pd.DataFrame.from_dict(counter_dict, orient='index', columns=['count'])

            # 短期上榜
            counter11 = Counter(short_top_300['一阶1']) + Counter(short_top_300['一阶2'])
            counter21 = Counter(short_top_300['模糊1']) + Counter(short_top_300['模糊2'])
            counter_main = counter11 + Counter({key: value * 0.3 for key, value in counter21.items()})
            del counter_main[0]
            counter_dict = dict(counter_main)
            style_short = pd.DataFrame.from_dict(counter_dict, orient='index', columns=['count'])

            counter12 = Counter(short_top_300['二阶1']) + Counter(short_top_300['二阶2']) + Counter(short_top_300['二阶3'])
            counter22 = Counter(short_top_300['模糊1.1']) + Counter(short_top_300['模糊2.1'])
            counter_main = counter12 + Counter({key: value * 0.3 for key, value in counter22.items()})
            del counter_main[0]
            counter_dict = dict(counter_main)
            field_short = pd.DataFrame.from_dict(counter_dict, orient='index', columns=['count'])

            # 强度和比例强度
            for i, df in enumerate([style_main, field_main, style_short, field_short]):
                temp = df['count'].tolist()
                df['vol'] = (temp - np.min(temp)) * (np.max(temp) - np.min(temp))
                df['vol_ratio'] #= 





if __name__ == '__main__':
    import tushare as ts
    # 获取今天的日期
    time = pd.to_datetime('2024-03-04')
    formatted_time = time.strftime("%Y%m%d")

    # token
    token = 'c336245e66e2882632285493a7d0ebc23a2fbb7392b74e4b3855a222'
    ts.set_token(token)
    pro = ts.pro_api()

    # 检查数据
    # 获取去年的1月1日日期并格式化
    # last_year_date = datetime(time.year - 1, 1, 1).date()
    last_year_date = pd.to_datetime('2018-01-01')
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

    # 历史数据生成
    df_close = pd.read_csv('./pre_close.csv').iloc[:,1:]
    df_close_adj = pd.read_csv('./pre_close_adj.csv').iloc[:,1:]

    label = pd.read_excel('RPS_label.xlsx', sheet_name='A股数据库20240206')
    direc = Direction() 
    direc_hist = direc.get_hist(date_list, df_close, df_close_adj,label)