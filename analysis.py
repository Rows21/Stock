import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import tushare as ts

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
    def __init__(self, date, all_code, token) -> None:
        self.date_str = date
        self.code = all_code
        ts.set_token(token)
        pro = ts.pro_api()

        self.amount_df = pro.daily_info(trade_date=self.date_str, exchange='SZ,SH', fields='trade_date,ts_name,ts_code,com_count,amount')
        self.up_df = pro.limit_list_d(trade_date=self.date_str, limit_type='U', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
        
        daily_m = pro.daily(trade_date=self.date_str)
        self.all_m = pd.merge(all_code,daily_m,on='ts_code')
    
    def get_param1(self):
        return len(self.up_df)
    
    def get_param3(self):
        return len(self.up_df)

    def get_param56(self):
        param5 = np.median(self.up_df['pct_chg'])
        param6 = np.mean(self.up_df['pct_chg'])
        return param5, param6
    
    def get_param78(self):

        # 前百日最高最低值
        max100 = 100
        min100 = 0

        # 判断
        count_max = (self.all_m['close'] > self.all_m['pre_close']).sum()
        count_min = (self.all_m['close'] > self.all_m['pre_close']).sum()
        return count_max, count_min
    
    def get_param9(self):
        
        # 前20日收盘均价
        past20_ma = pd.DataFrame({'Column': [10] * 5096})

        # 判断
        count = (self.all_m['close'] > self.all_m['pre_close']).sum()

        return count
    
    def get_param10(self):
        return 1000
    
    def get_input1(self):
        in4 = self.amount_df['amount'][0] + self.amount_df['amount'][11]
        in5 = self.amount_df['com_count'][0] + self.amount_df['com_count'][11]
        return in4, in5

    def algo1(self):
        
        param1 = self.get_param1()
        param3 = self.get_param3()
        in4, in5 = self.get_input1()
        param5, param6 = self.get_param56()
        param7, param8 = self.get_param78()
        param9 = self.get_param9()
        param10 = self.get_param10()
        out1 = 0.00045 * in4 + 4.5 * param1 / (in5 * 0.5) + 4.5 * param3 / (in5 * 0.5) + 500 * (param5 + param6) + 0.041 * param7 + 0.01 * param8 + 6.1*param9/(in5*0.5) + param10
        out2 = np.median(out1)

        return out1, out2
    
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
    time = pd.to_datetime('2023-12-28')
    time = time.strftime('%Y%m%d') 
    token = 'abfd1859c8f279c5d5b90fd2966fd286845ad6106efac0bc10fbbf72'

    ts.set_token(token)
    pro = ts.pro_api()
    stock_code = all_stock(token)

    # 前数据提取
    pre20 = day20_in(time, token)
    df_pre20 = pre20.pre_close20(stock_code)

    # 导入储存数据
    df = pd.read_csv('api.csv').iloc[:,1:]

    limit = limit_times(time, token)

    day = daily_in(time, stock_code, token=token)
    out1, out2 = day.algo1()
    
    print(out1,out2)