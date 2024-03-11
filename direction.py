import pandas as pd
import numpy as np
from collections import Counter
from tqdm import tqdm

class Direction():
    def __init__(self) -> None:
        # 创建历史数据空表
        self.df_upratio = pd.DataFrame(None, columns = ['date', '>100', '80-100', '60-80', '50-60', '40-50', '30-40', '20-30','all','bar','sump','w_sump'])
        self.short = pd.DataFrame(None, columns = ['date', '1', '2', '3', '4', '5', '6', '7', '8', '>9', '>7','semo','bar'])
        
    
    def get_hist(self, date_list, df_close, df_close_adj, label, daily=False):
        
        date_list.reverse()
        date_list = date_list[date_list.index('20240112'):] # start from 2020

        style_ts = None
        field_ts = None

        progress_bar = tqdm(total=len(date_list[20:]), ncols=200)
        # total
        counter11 = Counter(label['一阶1']) + Counter(label['一阶2'])
        counter21 = Counter(label['模糊1']) + Counter(label['模糊2'])
        counter_main = counter11 + Counter({key: value * 0.3 for key, value in counter21.items()})
        del counter_main[0]
        counter_dict = dict(counter_main)
        data = [[key, value] for key, value in counter_dict.items()]
        style_all = pd.DataFrame(data, columns=['Key', 'Value'])
        if style_ts is None:
            style_ts = pd.DataFrame(columns = ['date'] + style_all['Key'].tolist())

        counter12 = Counter(label['二阶1']) + Counter(label['二阶2']) + Counter(label['二阶3'])
        counter22 = Counter(label['模糊1.1']) + Counter(label['模糊2.1'])
        counter_main = counter12 + Counter({key: value * 0.3 for key, value in counter22.items()})
        del counter_main[0]
        counter_dict = dict(counter_main)
        data = [[key, value] for key, value in counter_dict.items()]
        field_all = pd.DataFrame(data, columns=['Key', 'Value'])
        if field_ts is None:
            field_ts = pd.DataFrame(columns = ['date'] + field_all['Key'].tolist())

        last_rank = []
        disp = pd.DataFrame(columns=['date', 'style', 'field'])

        for i, date in enumerate(date_list[20:]):
            
            date_20 = date_list[i]
            # 拉取1-20天的交易数据
            #today = pro.daily(trade_date=date)
            day_2 = []
            ind_today = int(df_close[df_close['trade_date'] == int(date)].index.values)
            ind_pre = int(df_close[df_close['trade_date'] == int(date_20)].index.values)
            df_20: pd.DataFrame = df_close_adj.iloc[ind_today:(ind_pre+1),].reset_index(drop=True)

            rps_df = None            
            df_20.iloc[0,:] = df_close[df_close['trade_date'] == int(date)].values

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
            data = [[key, value] for key, value in counter_dict.items()]
            style_main = pd.DataFrame(data, columns=['Key', 'count'])

            counter12 = Counter(main_top_300['二阶1']) + Counter(main_top_300['二阶2']) + Counter(main_top_300['二阶3'])
            counter22 = Counter(main_top_300['模糊1.1']) + Counter(main_top_300['模糊2.1'])
            counter_main = counter12 + Counter({key: value * 0.3 for key, value in counter22.items()})
            del counter_main[0]
            counter_dict = dict(counter_main)
            data = [[key, value] for key, value in counter_dict.items()]
            field_main = pd.DataFrame(data, columns=['Key', 'count'])

            # 短期上榜
            counter11 = Counter(short_top_300['一阶1']) + Counter(short_top_300['一阶2'])
            counter21 = Counter(short_top_300['模糊1']) + Counter(short_top_300['模糊2'])
            counter_main = counter11 + Counter({key: value * 0.3 for key, value in counter21.items()})
            del counter_main[0]
            counter_dict = dict(counter_main)
            data = [[key, value] for key, value in counter_dict.items()]
            style_short = pd.DataFrame(data, columns=['Key', 'count'])

            counter12 = Counter(short_top_300['二阶1']) + Counter(short_top_300['二阶2']) + Counter(short_top_300['二阶3'])
            counter22 = Counter(short_top_300['模糊1.1']) + Counter(short_top_300['模糊2.1'])
            counter_main = counter12 + Counter({key: value * 0.3 for key, value in counter22.items()})
            del counter_main[0]
            counter_dict = dict(counter_main)
            data = [[key, value] for key, value in counter_dict.items()]
            field_short = pd.DataFrame(data, columns=['Key', 'count'])

            # 强度和比例强度 
            #rank_dif = []
            # 定义分位点列表
            quantiles = [0, 0.4, 0.6, 0.8, 1]
            for k, df in enumerate([style_main, field_main, style_short, field_short]):
                if k % 2 == 0:
                    lab = style_all
                else:
                    lab = field_all

                df = pd.merge(df,lab,how='outer',on='Key')
                df['vol'] = (df['count'] - np.min(df['count'])) / (np.max(df['count']) - np.min(df['count']))
                df['vol_ratio'] = (df['count']/df['Value'] - np.min(df['count']/df['Value'])) / (np.max(df['count']/df['Value']) - np.min(df['count']/df['Value']))

                # OUT5-8
                df['prior'] = df['vol'] * 0.7 + df['vol_ratio'] * 0.3
                # OUT9-12
                df['prior_rank'] = df['prior'].rank(ascending=False)

                # OUT13-16
                if i > 0:
                    df['rank_dif'] = df['prior_rank'] - last_rank[k]['prior_rank']
                
                # 使用 cut() 方法根据分位点划分数据，并生成新的一列 'B'
                df['position'] = pd.cut(df['prior'], bins=df['prior'].quantile(quantiles), labels=['4', '3', '2', '1'])

                # 保存上一日排名值
                if i == 0:
                    last_rank.append(df)
                else:
                    last_rank[k] = df
                

            # 风格离散度
            dispersion1 = sum(last_rank[0]['prior'].dropna())
            # 行业离散度
            dispersion2 = sum(last_rank[1].nsmallest(20, 'prior_rank')['prior'])
            # 汇总
            disp.loc[i] = [date,dispersion1,dispersion2]
            
            if i == 0:
                style_ts.loc[i] = [date] + [0] * len(style_all)
                field_ts.loc[i] = [date] + [0] * len(field_all)
            else:
                # 风格档位 
                style_ts.loc[i] = [date] + last_rank[0]['position'].tolist()
                # 行业档位
                field_ts.loc[i] = [date] + last_rank[1]['position'].tolist()
                

            progress_bar.set_description(f"date=: {date}")
            progress_bar.set_postfix({'Iter': i+1})

        if daily:
            return style_ts, field_ts, disp, last_rank[0], last_rank[2]
        else:
            return style_ts, field_ts, disp

        

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
    style_ts, field_ts, disp = direc.get_hist(date_list, df_close, df_close_adj,label)
    style_ts.to_csv('style.csv')
    field_ts.to_csv('field.csv')
    disp.to_csv('dispersion.csv')