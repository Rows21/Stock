# 拉取历史数据
import pandas as pd

from analysis import *
from datetime import datetime
import os
from tqdm import tqdm

# 取当前年份-1年的首个交易日至今为历史数据

class History_M():
    def __init__(self, date, all_code, token):
        ts.set_token(token)
        self.pro = ts.pro_api()

        # 提取交易日历
        date_df = self.pro.trade_cal(exchange='SZSE', start_date='20200101', end_date=date)
        self.opendate_df = date_df[date_df['is_open'] == 1]

        # 创建历史数据空表
        self.df_upratio = pd.DataFrame(None, columns=['date', '成交量', '上涨数', '涨幅>2%', '涨幅中位', '涨幅均值', '新高', '新低', '>MA20', '客观指数'])
        self.code = all_code
    
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
    
    def get_100mm(self, date_list, ts_code):
        
        end_date = date_list[0]

        df_close_all = pd.DataFrame()
        for tss in ts_code:
            print(tss)
            df_meta = ts.pro_bar(ts_code=tss, adj='qfq', start_date='20200101', end_date=end_date)[['trade_date', 'close']]
            df_meta.columns = ['trade_date', tss]
            df_close_all['trade_date'] = df_meta['trade_date']

            df_close_all = pd.merge(df_close_all, df_meta, on='trade_date', how='outer')

        df_close_all.to_csv('pre_close.csv')

    def get_hist(self, date_list, preclose_df):

        date_list = date_list[::-1]
        for i, date in tqdm(enumerate(date_list), total=len(date_list)):
            # 在这里进行你的操作，例如打印索引和值
            # print(f"Value: {date}")

            daily_m = pro.daily(trade_date=date)
            # 除去北京
            daily_m = daily_m[~daily_m['ts_code'].str.contains('BJ')]
            daily_param = [None] * 9
            extracted_data = pd.DataFrame()

            # 成交额
            # 当日成交量 -
            daily_param[0] = daily_m['amount'].sum() /100000

            # 上涨数
            daily_param[1] = len(daily_m[daily_m['pct_chg'] > 0])

            # 涨幅均值
            daily_mean = np.mean(daily_m['pct_chg'])
            daily_param[4] = daily_mean

            # 涨幅中位
            daily_median = np.median(daily_m['pct_chg'])
            daily_param[3] = daily_median

            # 涨幅 > 2%
            daily_param[2] = len(daily_m[daily_m['pct_chg'] > 2.0])

            # 读入获取的百日新高新低值
            date_close = preclose_df[preclose_df['trade_date'] == int(date)]
            row_index = date_close.index[0]
            
            extracted_data.index = daily_m['ts_code']
            extracted_data['max'] = preclose_df[row_index : row_index + 100].max()[1:]
            extracted_data['min'] = preclose_df[row_index : row_index + 100].min()[1:]
            extracted_data['today'] = date_close.transpose()

            # 获取MA20
            extracted_data['MA20'] = preclose_df[row_index : row_index + 20].mean()[1:]

            # count 并保存
            count_max = (extracted_data['today'] >= extracted_data['max']).sum()
            daily_param[-4] = count_max
            count_min = (extracted_data['today'] <= extracted_data['min']).sum()
            daily_param[-3] = count_min
            # >MA20
            count_ma20 = (extracted_data['today'] >= extracted_data['MA20']).sum()
            daily_param[-2] = count_ma20

            # 客观指数
            if date == date_list[0]:
                index = 1000
            else:
                index = self.df_upratio['客观指数'][i-1] * (1 + 0.5 * (daily_median + daily_mean))
            
            daily_param[-1] = index
            # 历史当日数据统计
            new_row = pd.Series([date]+daily_param, index=self.df_upratio.columns)
            # 汇总
            self.df_upratio.loc[len(self.df_upratio)] = new_row

        return self.df_upratio


class History_L(limit_times):
    def __init__(self, date, token):
        super().__init__(date, token)


        # 创建历史数据空表
        self.df_limit = pd.DataFrame(None, columns = ['date', '成交量', 1,2,3,4,5,6,7,'7+', '涨停数', '跌停数', '炸板率', '连板高度', '连板股数', '连板溢价'])

    def get_hist(self, date_list):
        for i, date in enumerate(date_list):
            print(date)
            daily_param = [None] * 15

            # 拉取当天交易数据
            # 当日成交量
            amdf = pro.daily(trade_date=date)['amount'].sum() / 100000
            daily_param[0] = amdf

            # 涨版
            up_df = pro.limit_list_d(trade_date=date, limit_type='U', fields='ts_code,trade_date,industry,name,close,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')

            # 涨停数
            up_number = len(up_df)
            daily_param[9] = up_number

            # 连板股数
            daily_param[-2] = len(up_df[up_df['limit_times'] != 0])
            
            # 连板
            # 最大值, 连板高度
            limit_max = max(up_df['limit_times'])
            daily_param[-3] = limit_max
            
            for ii in range(8):
                if ii <= 6:
                    param_meta = up_df[up_df['limit_times'] == ii+1]
                else:
                    param_meta = up_df[up_df['limit_times'] >= ii+1]

                # 统计连板数
                daily_param[ii+1] = len(param_meta)
                #print(param_meta)
            
            # 跌
            down_df = pro.limit_list_d(trade_date=date, limit_type='D', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
            # 跌停数
            down_number = len(down_df)
            daily_param[10] = down_number

            # 炸
            zha_df = pro.limit_list_d(trade_date=date, limit_type='Z', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
            # 炸板率
            zha_ratio = len(zha_df) / (up_number + len(zha_df))
            daily_param[11] = zha_ratio

            # 连板溢价
            # 拉取上个交易日数据
            pretrade_date = date_list[i+1]
            up_df_yes = pro.limit_list_d(trade_date=pretrade_date, limit_type='U', fields='ts_code,close')
            left_join = pd.merge(up_df_yes, up_df, on='ts_code', how='inner')

            # 涨幅均值
            mean_up = np.mean(left_join['close_y']/left_join['close_x'] - 1)
            daily_param[-1] =mean_up

            # 历史当日数据统计
            new_row = pd.Series([date]+daily_param, index=self.df_limit.columns)
            # 汇总
            self.df_limit.loc[len(self.df_limit)] = new_row
        return self.df_limit
    
    def get_emo(self, hist_df = None, date_list = None):
        if hist_df is None:
            self.get_hist(date_list)
            hist_df = self.df_limit

        return 1

class History_S():
    def __init__(self, date, token):
        ts.set_token(token)
        self.pro = ts.pro_api()

        # 提取交易日历
        date_df = pro.trade_cal(exchange='SZSE', start_date='20200101', end_date=date)
        self.opendate_df = date_df[date_df['is_open'] == 1]

        # 创建历史数据空表
        self.df_upratio = pd.DataFrame(None, columns = ['date', '>100', '80-100', '60-80', '50-60', '40-50', '30-40', '20-30'])

    def get_hist(self, date_list, ts_code):
        aaa = ts.pro_bar(ts_code='000628.SZ', adj='qfq', start_date='20230924', end_date='20231228')
        #a = today_meta[today_meta['ts_code'] == '002459.SZ']
        today_meta = pro.daily(trade_date='20230425')
        b = today_meta[today_meta['ts_code'] == '002459.SZ']
        today_meta = pro.daily(trade_date='20230426')
        c = today_meta[today_meta['ts_code'] == '002459.SZ']

        for date in date_list:
            print(date)
            daily_param_1 = [None] * 7
            daily_param_2 = [None] * 8
            # 取当前日期前20个交易日
            cal20_list = self.opendate_df['cal_date'][:19].tolist()
            pretrade20_list = self.opendate_df['pretrade_date'][:19].tolist()

            # 拉取当天交易数据
            pre_close = pd.DataFrame(ts_code)

            pre_close_meta = pro.daily(trade_date=cal20_list[-1])
            pre20_close = pd.merge(ts_code,pre_close_meta,on='ts_code')

            today_meta = pro.daily(trade_date=cal20_list[0])
            today_close = pd.merge(ts_code,today_meta,on='ts_code')

            pre_close['difference'] = today_close['close']/pre20_close['pre_close'] - 1
            up_df = pre_close[pre_close['difference'] > 0]

            # >100
            daily_param_1[0] = len(up_df[up_df['difference'] >= 1.0])
            # 80-100
            daily_param_1[1] = len(up_df[(up_df['difference'] >= 0.80) & (up_df['difference'] < 1.0)])
            # 60-80
            daily_param_1[2] = len(up_df[(up_df['difference'] >= 0.60) & (up_df['difference'] < 0.80)])
            # 50-60
            daily_param_1[3] = len(up_df[(up_df['difference'] >= 0.50) & (up_df['difference'] < 0.60)])
            # 40-50
            daily_param_1[4] = len(up_df[(up_df['difference'] >= 0.40) & (up_df['difference'] < 0.50)])
            # 30-40
            daily_param_1[5] = len(up_df[(up_df['difference'] >= 0.30) & (up_df['difference'] < 0.40)])
            # 30-40
            daily_param_1[6] = len(up_df[(up_df['difference'] >= 0.20) & (up_df['difference'] < 0.30)])

            # 历史当日数据统计
            new_row = pd.Series([date]+daily_param_1, index=self.df_upratio.columns)
            # 汇总
            self.df_upratio.loc[len(self.df_upratio)] = new_row

            # 筛选涨跌幅
            # 龙1-8
            sorted_df = up_df.sort_values('difference', ascending=False)[:8]
            # > 9%
            count_9 = (up_df['difference'] > 0.09).sum()
            # > 7%
            count_7 = (up_df['difference'] > 0.07).sum()

            # 历史当日数据统计
            new_row = pd.Series([date]+daily_param_2, index=self.df_upratio.columns)
            # 汇总
            self.df_upratio.loc[len(self.df_upratio)] = new_row
        return self.df_upratio
    
    def get_emo(self, hist_df = None, date_list = None):
        if hist_df is None:
            self.get_hist(date_list)
            hist_df = self.df_limit

        return 1
    
if __name__ == '__main__':

    # 获取今天的日期
    time = pd.to_datetime('2024-01-02')
    formatted_time = time.strftime("%Y%m%d")

    # token
    token = 'c336245e66e2882632285493a7d0ebc23a2fbb7392b74e4b3855a222'
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

    # 前数据提取 - 收盘价
    if os.path.exists('./pre_close.csv'):
        df_preclose = pd.read_csv('./pre_close.csv')
    else:
        raise Exception('未找到历史数据文件！')
    
    hist_m = History_M(formatted_time, stock_code, token=token)
    df_L = hist_m.get_hist(date_list, df_preclose)
    
    df_L.to_csv('market_emo.csv')
    
    lianban = History_L(formatted_time, token=token)
    # 容错率
    short = History_S(formatted_time, token=token)
    df_duanxian = short.get_hist(date_list=date_list, ts_code=stock_code)



    df_lbemo = lianban.get_emo(df_lianban)

    # 收盘价
    # 检查是否有文件
    if os.path.exists('./pre_close_2year.csv'):
        df_pre_M = pd.read_csv('./pre_close_2year.csv')
    else:
        df_pre_M = pre20.pre_close(date_list=date_list)
        df_pre_M.to_csv('pre_close_2year.csv')
    
    