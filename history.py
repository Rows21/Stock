# 拉取历史数据

from analysis import *
from datetime import datetime


# 取当前年份-1年的首个交易日至今为历史数据

class History_M(daily_in):
    def __init__(self, date, all_code, token):
        super().__init__(date, all_code, token)
        print(self.all_m)
        a = 1


if __name__ == '__main__':

    # 获取今天的日期
    time = datetime.today().date()
    formatted_time = time.strftime("%Y%m%d")

    # token
    token = '9984e48de95326daee87a2fee7843133f8efd93b25a554db88b0a8ef'
    ts.set_token(token)
    pro = ts.pro_api()

    # 检查数据
    # 获取去年的1月1日日期并格式化
    last_year_date = datetime(time.year - 1, 1, 1).date()
    formatted_date = last_year_date.strftime("%Y%m%d")
    
    # 提取交易日历
    date_df = pro.trade_cal(exchange='SZSE', start_date=formatted_date, end_date=formatted_time)
    opendate_df = date_df[date_df['is_open'] == 1]

    # 提取当日交易数据
    today_df = pro.limit_list_d(trade_date=formatted_time, limit_type='U', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
    # 检查是否为空 如果为空 把上个交易日设置为当日
    if today_df.empty:
        formatted_time = opendate_df['pretrade_date'][0]
        # 获取去年的第一个交易日
        last_year_date = datetime(int(formatted_time[:4]) - 1, 1, 1).date()
        formatted_date = last_year_date.strftime("%Y%m%d")
        
        # 提取交易日历
        date_df = pro.trade_cal(exchange='SZSE', start_date=formatted_date, end_date=formatted_time)
        opendate_df = date_df[date_df['is_open'] == 1]

    stock_code = all_stock(token)

    # 前数据提取
    pre20 = History_M(formatted_time, stock_code, token=token)