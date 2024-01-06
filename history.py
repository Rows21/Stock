# 拉取历史数据

from analysis import *
from datetime import datetime
import os

# 取当前年份-1年的首个交易日至今为历史数据

class History_M(daily_in):
    def __init__(self, date, all_code, token):
        super().__init__(date, all_code, token)
    
    def pre_close(self, date_list):
        df = pd.DataFrame(self.code)
        for date in date_list:
            print(date)
            daily_m = pro.daily(trade_date=date)
            df_today = daily_m[['ts_code','close']]
            df_today.columns = ['ts_code', date]
            df = pd.merge(df_today, df, on='ts_code')
            df[date] = daily_m['close']
        return df
    
    def get_100mm(self, date_list):
        
        start_date = date_list[-1]

        # 重新拉取交易日历 100 trade days
        cal = pro.trade_cal(exchange='SZSE', start_date=(pd.to_datetime(start_date) - timedelta(days=200)).strftime("%Y%m%d"), end_date=start_date)
        trade_cal = cal[cal['is_open'] == 1]['pretrade_date'].unique()[:10]

        df_100 = self.pre_close(trade_cal)
        df = pd.DataFrame(df_100.iloc[:,0])

        # 统计前百日极值
        df['max_value'] = df_100.iloc[:,1:].apply(max, axis=1)
        df['min_value'] = df_100.iloc[:,1:].apply(min, axis=1)

        # new DF 100
        df_extreme = None
        date_list.reverse()

        # 开始循环统计当日的百日新高和百日新低值
        for date in date_list:
            print(date)
            daily_m = pro.daily(trade_date=date)

            df_compare = pd.merge(df, daily_m, on='ts_code')
            count_max = (df_compare['close'] > df_compare['max_value']).sum()
            count_min = (df_compare['close'] < df_compare['min_value']).sum()
            
            # Save
            if df_extreme is None:
                df_extreme = pd.DataFrame([[date, count_max, count_min]], columns=['date', 'max', 'min'])
            else:
                new_row = pd.Series([date, count_max, count_min], index=df_extreme.columns)

                # 使用 loc() 方法将新行添加到 DataFrame
                df_extreme.loc[len(df_extreme)] = new_row

            # Update
            df_100 = df_100.drop(df_100.columns[-1], axis=1)
            df_today = daily_m[['ts_code','close']]
            df_today.columns = ['ts_code', date]
            df_100 = pd.merge(df_today, df_100, on='ts_code')

            df['max_value'] = df_100.iloc[:,1:].apply(max, axis=1)
            df['min_value'] = df_100.iloc[:,1:].apply(min, axis=1)
        
        self.df_100 = df_100

        return df_extreme
    
    def get_emo(self):
        
        return 1


class History_L(limit_times):
    def __init__(self, date, token):
        super().__init__(date, token)

    def get_hist(self, date_list):
        # 创建历史数据空表
        df_limit = None
        
        for date in date_list:
            # 拉取当天交易数据
            # 涨版
            up_df = pro.limit_list_d(trade_date=date, limit_type='U', fields='ts_code,trade_date,industry,name,close,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')

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
            down_df = pro.limit_list_d(trade_date=date, limit_type='D', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
            # 跌停数
            down_number = len(down_df)

            # 炸
            zha_df = pro.limit_list_d(trade_date=date, limit_type='Z', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
            # 炸板率
            zha_ratio = len(zha_df) / (up_number + len(zha_df))

            # 连板溢价
            # 拉取上个交易日数据
            pretrade_date = opendate_df['pretrade_date'][0]
            up_df_yes = pro.limit_list_d(trade_date=pretrade_date, limit_type='U', fields='ts_code,close')
            left_join = pd.merge(up_df_yes, up_df, on='ts_code', how='inner')
            # 涨幅均值
            mean_up = np.mean(left_join['close_y']/left_join['close_x'] - 1)

            # 历史当日数据统计

            # 汇总

        

        return df_limit
    
    def get_emo(self):
        
        return 1
    
class Historu_S():
    def __init__(self, token) -> None:
        ts.set_token(token)
        pro = ts.pro_api()

    def get_hist(self, date_list, ts_code):

        # 提取交易日历
        date_df = pro.trade_cal(exchange='SZSE', start_date='20200101', end_date=self.date_str)
        opendate_df = date_df[date_df['is_open'] == 1]

        # 取当前日期前20个交易日
        self.cal20_list = opendate_df['cal_date'][:19].tolist()
        self.pretrade20_list = opendate_df['pretrade_date'][:19].tolist()

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

if __name__ == '__main__':

    # 获取今天的日期
    time = pd.to_datetime('2024-01-02')
    formatted_time = time.strftime("%Y%m%d")

    # token
    token = '544dc6cd793837eed33f3ae9b42bceadc3d08c983a0cff0990019074'
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

    # 生成交易日历
    date_list = list(opendate_df['pretrade_date'])

    stock_code = all_stock(token)
    pre20 = History_M(formatted_time, stock_code, token=token)

    # 前数据提取
    # 收盘价
    # 检查是否有文件
    if os.path.exists('./pre_close_2year.csv'):
        df_pre_M = pd.read_csv('./pre_close_2year.csv')
    else:
        df_pre_M = pre20.pre_close(date_list=date_list)
        df_pre_M.to_csv('pre_close_2year.csv')
    
    # 市场情绪统计
    if os.path.exists('./pre_MM100.csv'):
        df_pre_MM100 = pd.read_csv('./pre_MM100.csv')
    else:
        df_pre_MM100 = pre20.get_100mm(date_list=date_list)
        df_pre_MM100.to_csv('pre_MM100.csv')

    