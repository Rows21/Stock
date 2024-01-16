from history import *

if __name__ == '__main__':

    # 获取今天的日期
    time = pd.to_datetime('2024-01-02')
    formatted_time = time.strftime("%Y%m%d")

    # token
    token = 'abfd1859c8f279c5d5b90fd2966fd286845ad6106efac0bc10fbbf72'
    ts.set_token(token)
    pro = ts.pro_api()

    # 检查数据
    # 获取去年的1月1日日期并格式化
    # last_year_date = datetime(time.year - 1, 1, 1).date()
    last_year_date = pd.to_datetime('2023-12-01')
    formatted_date = last_year_date.strftime("%Y%m%d")

    # 提取交易日历
    date_df = pro.trade_cal(exchange='SZSE', start_date=formatted_date, end_date=formatted_time)
    opendate_df = date_df[date_df['is_open'] == 1]

    # 提取当日交易数据
    today_df = pro.limit_list_d(trade_date=formatted_time, limit_type='U',
                                fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
    # 检查是否为空 如果为空 把上个交易日设置为当日
    if today_df.empty:
        formatted_time = opendate_df['pretrade_date'][0]
        # 获取去年的第一个交易日
        last_year_date = datetime(int(formatted_time[:4]) - 1, 1, 1).date()
        formatted_date = last_year_date.strftime("%Y%m%d")

        # 提取交易日历
        date_df = pro.trade_cal(exchange='SZSE', start_date=formatted_date, end_date=formatted_time)
        opendate_df = date_df[date_df['is_open'] == 1]

    # 生成交易日历
    date_list = list(opendate_df['pretrade_date'])

    stock_code = all_stock(token)
    pre20 = History_M(formatted_time, stock_code, token=token)

    # 前数据提取
    # 市场情绪统计
    if os.path.exists('./pre_MM100.csv'):
        df_pre_MM100 = pd.read_csv('./pre_MM100.csv')
    else:
        pre20.get_100mm(date_list, all_stock)
