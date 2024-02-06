import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import tushare as ts
from tqdm import tqdm

def algo2():
    return out1, out2

def all_stock(token):
    ts.set_token(token)
    pro = ts.pro_api()
    data_sh = pro.stock_basic(exchange='SSE', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    data_sz = pro.stock_basic(exchange='SZSE', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    all = pd.concat([data_sh['ts_code'], data_sz['ts_code']])
    #all_str = all.str.cat(sep=',')
    return all

class daily_in():
    def __init__(self, date, token) -> None:
        self.date_str = date
        ts.set_token(token)
        self.pro = ts.pro_api()
        cal = pro.trade_cal(exchange='SZSE', start_date=(pd.to_datetime(date) - timedelta(days=20)).strftime("%Y%m%d"), end_date=date)
        self.daily = self.pro.daily(trade_date=self.date_str)
        self.trade_cal = cal[cal['is_open'] == 1]['pretrade_date'].unique()[:10]

    def pre_close(self, path='./pre_close.csv'):
        
        ts_today = self.daily['ts_code']
        df_close = pd.read_csv(path).iloc[:,1:]
        ts_hist = df_close.columns
        adj = pro.query('adj_factor',  trade_date=self.date_str)
        adj_pre = pro.query('adj_factor',  trade_date=self.trade_cal[0])
        feature = pd.merge(adj, adj_pre, on='ts_code', how='outer')
        filtered_feature = feature[feature['adj_factor_x'] - feature['adj_factor_y'] != 0]

        # 不变的前复权价
        elements_to_exclude = filtered_feature['ts_code'].to_list()
        today = [self.date_str] + self.daily[~self.daily['ts_code'].isin(elements_to_exclude)]['close'].to_list()
        new = self.daily[self.daily['ts_code'].isin(elements_to_exclude)][['ts_code', 'close']]
        #complement = set(ts_today) - set(ts_hist)
        #if complement != 0:

        df_close.loc[len(df_close)] = new

        for tss in tqdm(elements_to_exclude):
            #print(tss)
            df_meta = ts.pro_bar(ts_code=tss, adj='qfq', start_date=df_close['trade_date'][0], end_date=self.date_str)[['trade_date', 'close']]
            df_meta.columns = ['trade_date', tss]

            #df_close_all['trade_date'] = df_meta['trade_date']

            df_close_all = pd.merge(df_close_all, df_meta, on='trade_date', how='outer')

    

    
class day20_in():
    def __init__(self, date, token):
        self.date_str = date 
        ts.set_token(token)
        pro = ts.pro_api()

        # 提取交易日历
        date_df = pro.trade_cal(exchange='SZSE', start_date='20200101', end_date=self.date_str)
        opendate_df = date_df[date_df['is_open'] == 1]

        # 取当前日期前20个交易日
        self.cal20_list = opendate_df['cal_date'][:19].tolist()
        self.pretrade20_list = opendate_df['pretrade_date'][:19].tolist()

    def pre_close20(self,ts_code):
        pre_close = pd.DataFrame(ts_code)
        #for i, date in enumerate(self.cal20_list):
        #    pre_close_meta = pro.daily(trade_date=date)
        #    perday_close = pd.merge(ts_code,pre_close_meta,on='ts_code')
        #    pre_close[self.pretrade20_list[i]] = perday_close['pre_close']
        pre_close_meta = pro.daily(trade_date=self.cal20_list[-1])
        pre20_close = pd.merge(ts_code,pre_close_meta,on='ts_code')

        today_meta = pro.daily(trade_date=self.cal20_list[0])
        today_close = pd.merge(ts_code,today_meta,on='ts_code')

        pre_close['difference'] = today_close['close']/pre20_close['pre_close'] -1
        up_df = pre_close[pre_close['difference'] > 0]

        count_all = (up_df['difference'] > 0.20).sum()

        # 筛选涨跌幅
        # 龙1-8
        sorted_df = up_df.sort_values('difference', ascending=False)[:8]
        # > 9%
        count_9 = (up_df['difference'] > 0.09).sum()
        # > 7%
        count_7 = (up_df['difference'] > 0.07).sum()

        return count_all

class limit_times():
    def __init__(self, date, token):
        
        self.date_str = date # 将日期转换为字符串形式
        ts.set_token(token)
        pro = ts.pro_api()

        # 提取交易日历
        date_df = pro.trade_cal(exchange='SZSE', start_date='20200101', end_date=self.date_str)
        opendate_df = date_df[date_df['is_open'] == 1]

        # 拉取当天交易数据
        # 涨版
        up_df = pro.limit_list_d(trade_date=self.date_str, limit_type='U', fields='ts_code,trade_date,industry,name,close,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')

        # 涨停数
        up_number = len(up_df)
        
        # 连板
        # 最大值
        limit_max = max(up_df['limit_times'])
        param_list = []
        for i in range(7):
            if i < 6:
                param_meta = up_df[up_df['limit_times'] == i+1]                
            else:
                param_meta = up_df[up_df['limit_times'] >= i+1]

            # 统计连板数
            param_list.append(len(param_meta))
            print(param_meta)
        
        # 跌
        down_df = pro.limit_list_d(trade_date=self.date_str, limit_type='D', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
        # 跌停数
        down_number = len(down_df)

        # 炸
        zha_df = pro.limit_list_d(trade_date=self.date_str, limit_type='Z', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
        # 炸板率
        zha_ratio = len(zha_df) / (up_number + len(zha_df))

        # 连板溢价
        # 拉取上个交易日数据
        pretrade_date = opendate_df['pretrade_date'][0]
        up_df_yes = pro.limit_list_d(trade_date=pretrade_date, limit_type='U', fields='ts_code,close')
        left_join = pd.merge(up_df_yes, up_df, on='ts_code', how='inner')
        # 涨幅均值
        mean_up = np.mean(left_join['close_y']/left_join['close_x'] - 1)

        
if __name__ == '__main__':
    # 获取今天的日期
    time = pd.to_datetime('2024-02-02')
    time = time.strftime('%Y%m%d') 
    token = 'c336245e66e2882632285493a7d0ebc23a2fbb7392b74e4b3855a222'

    ts.set_token(token)
    pro = ts.pro_api()
    stock_code = all_stock(token)

    # 前数据提取
    pre20 = daily_in(time, token)
    df_pre20 = pre20.pre_close()
    # 更新收盘价

    # 导入储存数据
    df = pd.read_csv('api.csv').iloc[:,1:]

    limit = limit_times(time, token)

    day = daily_in(time, stock_code, token=token)
    out1, out2 = day.algo1()
    
    print(out1,out2)