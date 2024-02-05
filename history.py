# 拉取历史数据
import pandas as pd

from analysis import *
from datetime import datetime
from tqdm import tqdm
import os

# 取当前年份-1年的首个交易日至今为历史数据

class History_M():
    def __init__(self, date, all_code, token):
        ts.set_token(token)
        self.pro = ts.pro_api()
        self.code = all_code

        # 提取交易日历
        date_df = self.pro.trade_cal(exchange='SZSE', start_date='20200101', end_date=date)
        self.opendate_df = date_df[date_df['is_open'] == 1]

        # 创建历史数据空表
        self.df_upratio = pd.DataFrame(None, columns=['date', 'amount', '上涨数', '涨幅>2%', '涨幅中位', '涨幅均值', '新高', '新低', '>MA20','index'])
    
    def pre_close(self, date_list):
        
        #start_date = date_list[-1]
        end_date = date_list[0]

        ts_code = self.pro.daily(trade_date=end_date)['ts_code']

        df_close_all = pd.DataFrame()
        df_close_all['trade_date'] = date_list
        for tss in tqdm(ts_code):
            #print(tss)
            df_meta = ts.pro_bar(ts_code=tss, adj='qfq', start_date=date_list[-1], end_date=date_list[0])[['trade_date', 'close']]
            df_meta.columns = ['trade_date', tss]

            #df_close_all['trade_date'] = df_meta['trade_date']

            df_close_all = pd.merge(df_close_all, df_meta, on='trade_date', how='outer')

        df_close_all.to_csv('pre_close.csv')

        '''
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
        
        # 开始循环统计当日的百日新高和百日新低值，以及其他数据
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
        '''

    def get_hist(self, date_list, df_pre):

        date_list.reverse()
        for i,date in tqdm(enumerate(date_list)):

            daily_m = pro.daily(trade_date=date)
            # 删除BJ字样
            daily_m = daily_m[~daily_m['ts_code'].str.contains('BJ')]
            daily_param = [None] * 8
            #print(date)

            # 成交额()
            amount = daily_m['amount'].sum() / 100000

            # 上涨数
            daily_m['difference'] = daily_m['close']/daily_m['pre_close'] -1
            daily_param[0] = len(daily_m[daily_m['difference'] > 0]) / len(daily_m) # /总股票数

            # 涨幅 > 2%
            daily_param[1] = len(daily_m[daily_m['difference'] > 0.02]) / len(daily_m) # /总股票数

            daily_param[2] = np.median(daily_m['difference']) * 100 # 中位
            daily_param[3] = np.mean(daily_m['difference']) * 100 # 均值

            # 读入获取的百日新高新低值
            index = np.where(df_pre['trade_date'] == int(date))[0][0]
            df_100 = df_pre.iloc[index:(index+100),1:]
            daily_param[4] = (df_100.iloc[0] == df_100.max()).sum() # 新高
            daily_param[5] = (df_100.iloc[0] == df_100.min()).sum() # 新低

            # >MA20
            df_20 = df_pre.iloc[index:(index+20),:]
            daily_param[6] = (df_20.iloc[0] >= df_20.mean()).sum() / len(daily_m) # /总股票数

            # 客观指数
            if i == 0:
                daily_param[7] = 1000 # 第一天1000
            else:
                daily_param[7] = self.df_upratio.iloc[i-1]['index'] * (1 + 0.5*(self.df_upratio.iloc[i-1]['涨幅中位'] + self.df_upratio.iloc[i-1]['涨幅均值'])/100)
            
            # 市场水温
            #heat = 0.00045 * amount + 4.5*daily_param[0]/(len(daily_m)*0.5) + 4.5*daily_param[1]/(len(daily_m)*0.5) + 500*(daily_param[2] + daily_param[3]) + 0.041*daily_param[4] + 0.01*daily_param[5] + 6.1*daily_param[6]/(len(daily_m)*0.5) + daily_param[7]
            # 历史当日数据统计
            new_row = pd.Series([date,amount]+daily_param, index=self.df_upratio.columns)
            # 汇总
            self.df_upratio.loc[len(self.df_upratio)] = new_row

        return self.df_upratio
    
    def get_timeseries(self, df_hist):
        date_list = df_hist['date'].to_list()
        market_heat = [None] * len(date_list)
        param_index = [None] * len(date_list)

        # 客观指数 +5天开始计算 mean(x_5days) - x ||| total 970 days
        for i, date in enumerate(date_list):
            if i > 4:
                param_index[i] = np.mean(df_hist['index'][i-5:i]) - df_hist['index'][i]
            else:
                param_index[i] = -9999999
        df_hist['param_index'] = param_index
        df_hist['rank'] = df_hist['param_index'].rank(ascending=False)
        max_rank = max(df_hist['rank'])
        df_hist['rank_param'] = [1-i/max_rank for i in df_hist['rank']]

        for i, date in enumerate(date_list):
            data = df_hist.iloc[i][1:9]
            params = [0.00045, 4.5/0.5, 4.5/0.15, 5, 5, 0.041, 0.01, 6.1/0.5]

            
            market_heat[i] = sum(data * params) + df_hist['rank_param'][i]
        df_hist['market_heat'] = market_heat
        
        #df_hist = df_hist.iloc[5:,:]
        df_daily_err = pd.DataFrame(columns=['2', '3', '5', '7', '10', '20'])
        # qing xu pian cha
        av2, av3, av5, av7, av10, av20 = [],[],[],[],[],[]
        w_emo = [None] * len(date_list)
        w1 = [None] * len(date_list)
        w2 = [None] * len(date_list)
        for i, date in enumerate(date_list):
            daily_err = [-9999] *6
            
            if i >= 2:
                av2.append(np.mean(df_hist['market_heat'][i-2:i]))
                daily_err[0] = df_hist['market_heat'][i] - np.median(np.mean(df_hist['market_heat'][i-2:i]))

            if i >= 3:
                av3.append(np.mean(df_hist['market_heat'][i-3:i]))
                daily_err[1] = df_hist['market_heat'][i] - np.median(np.mean(df_hist['market_heat'][i-3:i]))

            if i >= 5:
                av5.append((df_hist['market_heat'][i-5:i]))
                daily_err[2] = df_hist['market_heat'][i] - np.median(np.mean(df_hist['market_heat'][i-5:i]))
            
            if i >= 7:
                av7.append((df_hist['market_heat'][i-7:i]))
                daily_err[3] = df_hist['market_heat'][i] - np.median(np.mean(df_hist['market_heat'][i-7:i]))

            if i >= 10:
                av10.append((df_hist['market_heat'][i-10:i]))
                daily_err[4] = df_hist['market_heat'][i] - np.median(np.mean(df_hist['market_heat'][i-10:i]))

            if i >= 20:
                av20.append((df_hist['market_heat'][i-20:i]))
                daily_err[5] = df_hist['market_heat'][i] - np.median(np.mean(df_hist['market_heat'][i-20:i]))
            print(daily_err)
            df_daily_err.loc[i] = daily_err


            # weighted mean emo
            w_emo_param = [0.08, 0.12, 0.3, 0.18, 0.2, 0.12]
            w_emo[i] = sum([daily_err[j] * w_emo_param[j] for j in range(len(daily_err))])

            w_short = [0.55,0.45]
            w1[i] = daily_err[0] * w_short[0] + daily_err[1] * w_short[1]
            w2[i] = daily_err[-1] * w_short[0] + daily_err[-2] * w_short[1]
        
        df_hist['weighted_emo'] = w_emo
        df_hist['weighted_emo_R'] = df_hist['weighted_emo'].rank(ascending=False)
        df_daily_err.to_csv('emo_err.csv')

        df_hist['short'] = w1
        df_hist['short_R'] = 1 - df_hist['short'].rank(ascending=False)/df_hist['short']

        df_hist['long'] = w2
        df_hist['long_R'] = 1 - df_hist['long'].rank(ascending=False)/df_hist['long']

    
        # 定义分位点列表
        quantiles = [0, 0.1, 0.42, 0.58, 0.9, 1]

        # 使用 cut() 方法根据分位点划分数据，并生成新的一列 'B'
        df_hist['position'] = pd.cut(df_hist['weighted_emo'], bins=df_hist['weighted_emo'].quantile(quantiles), labels=['<10%', '10-42%', '42-58%', '58-90%', '>90%'])

        df_hist.iloc[20:,:].to_csv('long_final.csv')


        return df_hist.iloc[20:,:]


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
            if i == len(date_list)-1:
                pretrade_date = (pd.to_datetime(date) -pd.DateOffset(days=1)).strftime("%Y%m%d")
            else:
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
        self.opendate_df = date_df[date_df['is_open'] == 1].reset_index()

        # 创建历史数据空表
        self.df_upratio = pd.DataFrame(None, columns = ['date', 'sum', '>100', '80-100', '60-80', '50-60', '40-50', '30-40', '20-30'])
        self.short = pd.DataFrame(None, columns = ['date', '1', '2', '3', '4', '5', '6', '7', '8', '>9', '>7'])

    def get_hist(self, date_list, df_close):
        date_list.reverse()
        for date in date_list:
            print(date)
            daily_param_1 = [None] * 7
            daily_param_2 = [None] * 10
            # 取当前日期前20个交易日
            index = np.where(self.opendate_df['cal_date'] == date)[0][0]
            cal20 = self.opendate_df['cal_date'][index+20]

            # 拉取当天和20天前的交易数据
            today = pro.daily(trade_date=date)
            today = today[~today['ts_code'].str.contains('BJ')]
            
            pre = pro.daily(trade_date=cal20)
            pre = pre[~pre['ts_code'].str.contains('BJ')]
            up_df = pd.merge(today, pre, on='ts_code', how='left')
            # 删除BJ字样
            up_df['difference'] = up_df['close_x']/up_df['close_y'] -1


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
            new_row_1 = pd.Series([date,sum(daily_param_1)]+daily_param_1, index=self.df_upratio.columns)
            # 汇总
            self.df_upratio.loc[len(self.df_upratio)] = new_row_1

            # 筛选涨跌幅
            # 龙1-8
            daily_param_2[:8] = up_df.sort_values('difference', ascending=False)[:8]['difference'].to_list()
            # > 9%
            daily_param_2[-2] = (up_df['difference'] > 0.09).sum()
            # > 7%
            daily_param_2[-1] = (up_df['difference'] > 0.07).sum()

            # 历史当日数据统计
            new_row_2 = pd.Series([date]+daily_param_2, index=self.short.columns)
            # 汇总
            self.short.loc[len(self.short)] = new_row_2
        return self.df_upratio, self.short
    
    
if __name__ == '__main__':

    # 获取今天的日期
    time = pd.to_datetime('2024-02-03')
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

    stock_code = all_stock(token)
    
    # 收盘价-前数据提取
    # 市场情绪计算
    pre20 = History_M(formatted_time, stock_code, token=token)
    # 检查是否有文件
    if os.path.exists('./pre_close.csv'):
        df_pre_M = pd.read_csv('./pre_close.csv').iloc[:,1:]
    else:
        df_pre_M = pre20.pre_close(date_list=date_list)
        df_pre_M.to_csv('pre_close.csv')
    
    
    # 存储
    if os.path.exists('./longemo.csv'):
        df_M = pd.read_csv('./longemo.csv').iloc[:,1:]
    else:
        df_M = pre20.get_hist(date_list, df_pre_M)
        df_M.to_csv('longemo.csv')
    
    df_M = pre20.get_timeseries(df_M)

    # 连板计算
    lianban = History_L(formatted_time, token=token)
    if os.path.exists('./lianban.csv'):
        df_lianban = pd.read_csv('./lianban.csv').iloc[:,1:]
    else:
        df_lianban = lianban.get_hist(date_list)
        df_lianban.to_csv('lianban.csv')

    # 容错率
    short = History_S(formatted_time, token=token)
    
    if os.path.exists('./shortemo.csv'):
        df_short = pd.read_csv('./shortemo.csv').iloc[:,1:]
        df_rongcuo = pd.read_csv('./rongcuo.csv').iloc[:,1:]
    else:
        df_rongcuo, df_short = short.get_hist(date_list, df_pre_M)
        df_rongcuo.to_csv('shortemo.csv')
        df_short.to_csv('short.csv')


    
    