import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import tushare as ts

ts.set_token('7da7a271ad4586a92f459e277d81b66b7f216818d5dfb17e5c103144')
pro = ts.pro_api()
print(pro)

def algo2():
    return out1, out2

def all_stock():
    data_sh = pro.stock_basic(exchange='SSE', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    data_sz = pro.stock_basic(exchange='SZSE', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    all = pd.concat([data_sh['ts_code'],data_sz['ts_code']])
    #all_str = all.str.cat(sep=',')
    return all

class daily_in():
    def __init__(self, date, all_code, token='7da7a271ad4586a92f459e277d81b66b7f216818d5dfb17e5c103144') -> None:
        self.date_str = date.strftime('%Y%m%d')  # 将日期转换为字符串形式
        ts.set_token(token)
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
    def __init__(self) -> None:
        pass
    
if __name__ == '__main__':
    time = pd.to_datetime('2023-11-28')
    token = '7da7a271ad4586a92f459e277d81b66b7f216818d5dfb17e5c103144'

    ts.set_token(token)
    pro = ts.pro_api()

    # 导入储存数据
    df = pd.read_csv('api.csv').iloc[:,1:]

    stock_code = all_stock()
    day = daily_in(time, stock_code, token=token)
    out1, out2 = day.algo1()
    
    print(out1,out2)