import numpy as np
import pandas as pd
from tqdm import tqdm

from datetime import datetime, timedelta
import tushare as ts
from direction import Direction

def all_stock(token):
    ts.set_token(token)
    pro = ts.pro_api()
    data_sh = pro.stock_basic(exchange='SSE', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    data_sz = pro.stock_basic(exchange='SZSE', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    all = pd.concat([data_sh['ts_code'], data_sz['ts_code']])
    #all_str = all.str.cat(sep=',')
    return all

class daily_in():
    def __init__(self, date, token, path_hist='temp/longemo.csv') -> None:
        self.date_str = date
        self.df_upratio = pd.read_csv(path_hist).iloc[:,1:]
        self.pas_param = True
        # check
        if int(date) in self.df_upratio['date'].tolist():
            self.pas_param = False
        print('是否执行长线表更新：'+str(self.pas_param))

        ts.set_token(token)
        self.pro = ts.pro_api()
        cal = pro.trade_cal(exchange='SZSE', start_date=(pd.to_datetime(date) - timedelta(days=20)).strftime("%Y%m%d"), end_date=date)
        if cal['is_open'][0] == 0:
            raise Exception('非交易日！')
        daily = self.pro.daily(trade_date=self.date_str)
        self.daily = daily[~daily['ts_code'].str.contains('BJ')]
        self.trade_cal = cal[cal['is_open'] == 1]['pretrade_date'].unique()[:10]
        
    def pre_close(self, path='temp/pre_close.csv', path_adj = 'temp/pre_close_adj.csv'):
        df_close = pd.read_csv(path).iloc[:,1:]
        df_close_adj = pd.read_csv(path_adj).iloc[:,1:]

        df_close = df_close.loc[:, ~df_close.columns.str.contains('BJ')]
        if float(self.date_str) in df_close['trade_date'].tolist():
            self.df_pre = df_close
        else:
            adj = pro.query('adj_factor',  trade_date=self.date_str)
            adj_pre = pro.query('adj_factor',  trade_date=self.trade_cal[0])
            feature = pd.merge(adj, adj_pre, on='ts_code', how='outer')
            feature = feature[~feature['ts_code'].str.contains('BJ')]
            filtered_feature = feature[feature['adj_factor_x'] - feature['adj_factor_y'] != 0]
            
            print('执行复权值更新:')
            if filtered_feature.empty != True:
                print(filtered_feature['ts_code'].tolist())
                for tss in filtered_feature['ts_code'].tolist():
                    if tss in df_close.columns:
                        adj_pre = filtered_feature[filtered_feature['ts_code'] == tss]['adj_factor_y']
                        adj_aft = filtered_feature[filtered_feature['ts_code'] == tss]['adj_factor_x']
                        df_close[tss] = df_close[tss] * adj_pre / adj_aft
                        df_close_adj[tss] = df_close[tss] * adj_pre / adj_aft
                    else:
                        df_close[tss] = pd.Series([np.nan] * len(df_close))
                        df_close_adj[tss] = pd.Series([np.nan] * len(df_close_adj))
            
            # 在第一行插入新行
            ts_today = self.daily['ts_code']
            new_row = pd.Series([np.nan] * len(df_close.columns), index=df_close.columns)
            df_close = pd.concat([pd.DataFrame([new_row]), df_close]).reset_index(drop=True)
            df_close['trade_date'][0] = int(self.date_str)
            df_close_adj = pd.concat([pd.DataFrame([new_row]), df_close_adj]).reset_index(drop=True)
            df_close_adj['trade_date'][0] = int(self.date_str)

            # 不变的前复权价
            # new
            #elements_to_exclude = filtered_feature['ts_code'].to_list()
            print('执行前复权更新:')
            for tsnew in tqdm(ts_today):
                if tsnew not in df_close.columns:
                    df_new:pd.DataFrame = ts.pro_bar(ts_code=tsnew, adj='qfq', start_date='20171229', end_date=self.date_str)[['trade_date', 'close']]
                    df_new.columns = ['trade_date', tsnew]
                    df_new['trade_date'] = df_new['trade_date'].astype(float)

                    df_close = pd.merge(df_close, df_new, on='trade_date', how='outer')
                else:
                    df_close[tsnew][0] = float(self.daily[self.daily['ts_code'] == tsnew]['close'].values)

            df_close_adj.iloc[0] = df_close.iloc[0]
            na_mask = df_close_adj.iloc[0].isna()
            na_ind = [i for i in range(len(df_close_adj.columns)) if na_mask.iloc[i] == True]
            
            for ind in na_ind:
                df_close_adj.iloc[0,ind] = df_close_adj.iloc[1,ind]

            df_close_adj.to_csv('temp/pre_close_adj.csv')
            df_close.to_csv('temp/pre_close.csv')
            self.df_pre = df_close

        
    def get_today(self):
        if self.pas_param == False:
            return self.df_upratio
        
        self.pre_close()
        print('执行长线表更新:')
        daily_param = [None] * 8
        #print(date)

        # 成交额()
        amount = self.daily['amount'].sum() / 100000

        # 上涨数
        self.daily['difference'] = self.daily['close']/self.daily['pre_close'] -1
        daily_param[0] = len(self.daily[self.daily['difference'] > 0]) / len(self.daily) # /总股票数

        # 涨幅 > 2%
        daily_param[1] = len(self.daily[self.daily['difference'] > 0.02]) / len(self.daily) # /总股票数

        daily_param[2] = np.median(self.daily['difference']) * 100 # 中位
        daily_param[3] = np.mean(self.daily['difference']) * 100 # 均值

        # 读入获取的百日新高新低值
        index = np.where(self.df_pre['trade_date'] == int(self.date_str))[0][0]
        df_100 = self.df_pre.iloc[index:(index+100),1:]
        daily_param[4] = (df_100.iloc[0] == df_100.max()).sum() # 新高
        daily_param[5] = (df_100.iloc[0] == df_100.min()).sum() # 新低

        # >MA20
        df_20 = self.df_pre.iloc[index:(index+20),:]
        daily_param[6] = (df_20.iloc[0][1:] >= df_20.mean()[1:]).sum() / len(self.daily) # /总股票数

        # 客观指数
        daily_param[7] = self.df_upratio.iloc[-1]['index'] * (1 + 0.5*(self.df_upratio.iloc[-1]['涨幅中位'] + self.df_upratio.iloc[-1]['涨幅均值'])/100)
            
        # 市场水温
        #heat = 0.00045 * amount + 4.5*daily_param[0]/(len(self.daily)*0.5) + 4.5*daily_param[1]/(len(self.daily)*0.5) + 500*(daily_param[2] + daily_param[3]) + 0.041*daily_param[4] + 0.01*daily_param[5] + 6.1*daily_param[6]/(len(self.daily)*0.5) + daily_param[7]
        # 历史当日数据统计
        new_row = pd.Series([self.date_str, amount]+daily_param, index=self.df_upratio.columns)
        # 汇总
        self.df_upratio.loc[len(self.df_upratio)] = new_row

        return self.df_upratio

class limit_times():
    def __init__(self, date, token, path_hist='temp/lianban.csv') -> None:
        self.date_str = date
        ts.set_token(token)
        self.pro = ts.pro_api()
        cal = pro.trade_cal(exchange='SZSE', start_date=(pd.to_datetime(date) - timedelta(days=20)).strftime("%Y%m%d"), end_date=date)
        daily = self.pro.daily(trade_date=self.date_str)
        self.daily = daily[~daily['ts_code'].str.contains('BJ')]
        self.trade_cal = cal[cal['is_open'] == 1]['pretrade_date'].unique()[:20]
        self.df_limit = pd.read_csv(path_hist).iloc[:,1:]
        self.pas_param = True
        # check
        if int(date) in self.df_limit['date'].tolist():
            self.pas_param = False
        print('是否执行连板表更新：'+str(self.pas_param))

    def get_today(self):
        if self.pas_param == False:
            return self.df_limit
        daily_param = [None] * 15

        # 拉取当天交易数据
        # 当日成交量
        amdf = pro.daily(trade_date=self.date_str)
        # 删除BJ字样
        amdf = amdf[~amdf['ts_code'].str.contains('BJ')]
        daily_param[0] = amdf['amount'].sum() / 100000

        # 涨版
        up_df = pro.limit_list_d(trade_date=self.date_str, limit_type='U', fields='ts_code,trade_date,industry,name,close,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')

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
        down_df = pro.limit_list_d(trade_date=self.date_str, limit_type='D', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
        # 跌停数
        down_number = len(down_df)
        daily_param[10] = down_number

        # 炸
        zha_df = pro.limit_list_d(trade_date=self.date_str, limit_type='Z', fields='ts_code,trade_date,industry,name,limit,pct_chg,open_times,limit_amount,fd_amount,first_time,last_time,up_stat,limit_times')
        # 炸板率
        zha_ratio = len(zha_df) / (up_number + len(zha_df))
        daily_param[11] = zha_ratio

        # 连板溢价
        # 拉取上个交易日数据
        yest_df = pro.limit_list_d(trade_date=self.date_str, limit_type='U', fields='ts_code,limit_times')
        code = yest_df[yest_df['limit_times'] >= 2]['ts_code']
        temp = pd.merge(code,amdf,on='ts_code',how='inner')['pct_chg']
        pct_chg = np.mean(temp)

        # Save涨幅均值
        daily_param[-1] = pct_chg

        # 历史当日数据统计
        new_row = pd.Series([self.date_str]+daily_param, index=self.df_limit.columns)
        # 汇总
        self.df_limit.loc[len(self.df_limit)] = new_row
        return self.df_limit

class short_in():
    def __init__(self, date, token, path_hist1='temp/shortemo.csv', path_hist2='temp/short.csv') -> None:
        self.date_str = date
        ts.set_token(token)
        self.pro = ts.pro_api()
        cal = pro.trade_cal(exchange='SZSE', start_date=(pd.to_datetime(date) - timedelta(days=40)).strftime("%Y%m%d"), end_date=date)
        daily = self.pro.daily(trade_date=self.date_str)
        self.daily = daily[~daily['ts_code'].str.contains('BJ')]
        self.trade_cal = cal[cal['is_open'] == 1]['pretrade_date'].unique()[20]
        self.df_upratio = pd.read_csv(path_hist1).iloc[:,1:]
        self.short = pd.read_csv(path_hist2).iloc[:,1:]
        self.pas_param = True
        # check
        if int(date) in self.df_upratio['date'].tolist():
            self.pas_param = False
        print('是否执行短线表更新：'+str(self.pas_param))

    def get_today(self, df_close, lianban):
        if self.pas_param == False:
            return self.df_upratio, self.short
        print(self.date_str)
        daily_param_1 = [0] * 11
        daily_param_2 = [0] * 12
        # 取当前日期前20个交易日
        date_20 = self.trade_cal

        # 拉取当天和20天前的交易数据
        today = df_close[df_close['trade_date'] == int(self.date_str)]
        pre = df_close[df_close['trade_date'] == int(date_20)]
        tempdf = pd.concat([today,pre], ignore_index=True).dropna(axis=1)
        tempdf = tempdf.iloc[:,1:]
        up_df = (tempdf.iloc[0]/tempdf.iloc[1] -1).dropna()

        # >100
        daily_param_1[0] = len(up_df[up_df >= 1.0])
        # 80-100
        daily_param_1[1] = len(up_df[(up_df>= 0.80) & (up_df< 1.0)])
        # 60-80
        daily_param_1[2] = len(up_df[(up_df>= 0.60) & (up_df< 0.80)])
        # 50-60
        daily_param_1[3] = len(up_df[(up_df>= 0.50) & (up_df< 0.60)])
        # 40-50
        daily_param_1[4] = len(up_df[(up_df>= 0.40) & (up_df< 0.50)])
        # 30-40
        daily_param_1[5] = len(up_df[(up_df>= 0.30) & (up_df< 0.40)])
        # 30-40
        daily_param_1[6] = len(up_df[(up_df>= 0.20) & (up_df< 0.30)])

        # sum
        daily_param_1[7] = sum(daily_param_1[0:6])
        all_up = self.df_upratio['all'].tolist() + [sum(daily_param_1[0:6])]
        daily_param_1[8] = 0.8*np.median(all_up) + 0.2*np.mean(all_up)

        # 历史当日数据统计
        new_row_1 = pd.Series([self.date_str]+daily_param_1, index=self.df_upratio.columns)
        # 汇总
        self.df_upratio.loc[len(self.df_upratio)] = new_row_1

        # sump
        i = len(self.df_upratio)-1
        sump = 0
        for j in range(7):
            rankj = 1- self.df_upratio.iloc[:,2+j].rank(ascending=False)[i]/(i+1)
            sump = sump + rankj

        self.df_upratio.iloc[i,-2] = sump
        self.df_upratio.iloc[i,-1] = 1 - self.df_upratio.iloc[:,-2].rank(ascending=False)[i]/(i+1)

        # 筛选涨跌幅
        # 龙1-8
        daily_param_2[:8] = up_df.sort_values(ascending=False)[:8]
        # > 9%
        daily_param_2[8] = len(up_df[up_df >= 0.09])
        # > 7%
        daily_param_2[9] = len(up_df[up_df <= -0.07])

        amt_rank = lianban.loc[0:i,]['成交量'].rank(ascending=False)
        amt_param = 1-amt_rank[i]/(i+1)
        up_rank = lianban.loc[0:i,]['涨停数'].rank(ascending=False)
        up_param = 1-up_rank[i]/(i+1)
        down_rank = lianban.loc[0:i,]['跌停数'].rank(ascending=False)
        down_param = down_rank[i]/(i+1)
        zha_rank = lianban.loc[0:i,]['炸板率'].rank(ascending=False)
        zha_param = zha_rank[i]/(i+1)
        stockh_rank = lianban.loc[0:i,]['连板高度'].rank(ascending=False)
        stockh_param = 1-stockh_rank[i]/(i+1)
        stockno_rank = lianban.loc[0:i,]['连板股数'].rank(ascending=False)
        stockno_param = 1-stockno_rank[i]/(i+1)
        stockout_rank = lianban.loc[0:i,]['连板溢价'].rank(ascending=False)
        stockout_param = 1-stockout_rank[i]/(i+1)
            
        param_lianban = 0.7*amt_param + 1*up_param+ 1.1*down_param+ 0.7*zha_param+ 1.1*stockno_param+ 1*stockout_param+ 1.2*stockh_param
        daily_param_2[-2] = param_lianban + 2*self.df_upratio['w_sump'][i] + 0.88*sum(daily_param_2[:8])
        # 历史当日数据统计
        new_row_2 = pd.Series([self.date_str]+daily_param_2, index=self.short.columns)
        # 汇总
        self.short.loc[len(self.short)] = new_row_2

        self.short['bar'][i] = np.median(self.short.iloc[:,-2])

        return self.df_upratio, self.short

        
if __name__ == '__main__':
    # 获取今天的日期
    # 获取当前系统时间
    current_time = datetime.now()
    

    # 提取日期部分的天数
    time_c = current_time.date()
    if current_time.hour < 16:
        time_c = time_c - timedelta(days=1)
    #time_c = pd.to_datetime('2024-03-12')

    time_c = time_c.strftime('%Y%m%d') 
    token = 'c336245e66e2882632285493a7d0ebc23a2fbb7392b74e4b3855a222'

    ts.set_token(token)
    pro = ts.pro_api()
    stock_code = all_stock(token)
    
    close = pd.read_csv('temp/longemo.csv').iloc[:,1:]

    cal = pro.trade_cal(exchange='SZSE', start_date=str(close['date'][len(close)-1]), end_date=time_c)
    time_l = cal[cal['is_open'] == 1]['cal_date'].tolist()
    time_l.reverse()
    
    
    print('------------------')
    print('开始风控计算：')
    for time in time_l:
        print(time)

        # 前数据提取
        # 更新收盘价
        pre20 = daily_in(time, token)
        df_L = pre20.get_today()
        df_L.to_csv('temp/longemo.csv')
        df_L.to_csv('.\\logs\\' + time + 'longemo.csv')
        
        
        # 更新连板数据
        lb = limit_times(time, token)
        df_LB = lb.get_today()
        df_LB.to_csv('temp/lianban.csv')
        df_LB.to_csv('.\\logs\\' + time + 'lianban.csv')

        # short
        df_close = pd.read_csv('temp/pre_close.csv').iloc[:,1:]
        lianban = pd.read_csv('temp/lianban.csv').iloc[:,1:]
        short = short_in(time, token)
        df_R, df_S = short.get_today(df_close, lianban)
        df_R.to_csv('temp/shortemo.csv')
        df_R.to_csv('.\\logs\\' + time + 'shortemo.csv')
        df_S.to_csv('temp/short.csv')
        df_S.to_csv('.\\logs\\' + time + 'short.csv')
    
    print('设置时间戳：')
    #ts_date = 20220104
    #df_L = df_L.iloc[int(df_L[df_L['date'] == ts_date].index.values):].reset_index().iloc[:,1:]
    #df_LB = df_LB.iloc[int(df_LB[df_LB['date'] == ts_date].index.values):].reset_index().iloc[:,1:]
    #df_S = df_S.iloc[int(df_S[df_S['date'] == ts_date].index.values):].reset_index().iloc[:,1:]

    print('------------------')
    print('开始数据储存：')
    df_R.to_excel('容错率.xlsx')
    from history import History_M
    histm = History_M(time_l, stock_code, token=token)
    df_M_now = histm.get_timeseries(df_L)  
    df_M_now.columns = ['日期', '成交量', '上涨数', '涨幅>2%', '涨幅中位', '涨幅均值', '新高', 
                        '新低', 'MA20', '指数', 'param_index', 'rank', 'rank_param', '市场水温', 
                        '市场水温Rank', '情绪雷达', '情绪加权平均', '参考仓位', '短期波动', '短期强度', 
                        '长期趋势', '趋势强度', '市场研判']
    df = pro.index_daily(ts_code='399303.SZ', start_date=str(df_M_now.loc[0,'日期']), end_date=time_c)
    ind_list = df[['open','high','low','close']].iloc[::-1].reset_index().iloc[:,1:]
    df_M_now = df_M_now.reset_index().iloc[:,1:]
    df_M_now = pd.concat([df_M_now,ind_list],axis=1)
    df_M_now.to_excel('今日长线.xlsx')

    from history import History_L
    histl = History_L(time_l, token=token)
    df_LB_now = histl.get_timeseries(df_LB)
    df_LB_now.columns = ['date', '成交量', '1', '2', '3', '4', '5', '6', '7', '7+', '涨停数', 
                        '跌停数', '炸板率', '连板高度', '连板股数', '连板溢价', '连板情绪', 'l_bar', 
                        '连板高度', '情绪加权', '参考仓位', '短期波动', '短期强度', '长期趋势', '趋势强度', '连板研判']
    df_LB_now.to_excel('今日连板.xlsx')

    from history import History_S
    hists = History_S(time_l, token=token)
    df_S_now = hists.get_timeseries(df_S) 
    df_S_now.columns = ['date', '1', '2', '3', '4', '5', '6', '7', '8', '>9', '>7', 
                        '短期情绪', '情绪阈值', '情绪加权', '参考仓位', '短期波动', '短期强度', 
                        '长期趋势', '趋势强度', '短期研判']
    df_S_now.to_excel('今日短线.xlsx')

    # 方向
    # 数据导入
    print('------------------')
    print('开始方向计算：')
    df_close = pd.read_csv('temp/pre_close.csv').iloc[:,1:]
    df_close_adj = pd.read_csv('temp/pre_close_adj.csv').iloc[:,1:]
    label = pd.read_excel('RPS_label.xlsx', sheet_name='A股数据库20240206')
    close = pd.read_csv('temp/style.csv').iloc[:,1:]

    cal = pro.trade_cal(exchange='SZSE', start_date=str(close['date'][len(close)-1]), end_date=time_c)
    time_l = cal[cal['is_open'] == 1]['cal_date'].tolist()
    time_l.reverse()

    dire = Direction(daily=True)
    if time_l != []:
        style_ts, field_ts, disp, style_t, field_t = dire.get_hist(time_l, df_close, df_close_adj, label)
        style_ts.to_csv('.\\dlogs\\' + time + 'style.csv')
        field_ts.to_csv('.\\dlogs\\' + time + 'field.csv')
        disp.to_csv('.\\dlogs\\' + time + 'dispersion.csv')
        style_ts.to_csv('temp/style.csv')
        field_ts.to_csv('temp/field.csv')
        disp.to_csv('temp/dispersion.csv')
        style_ts.to_excel('风格.xlsx')
        field_ts.to_excel('行业.xlsx')
        disp.columns = ['date', '风格', '行业']
        
        style_t.columns = ['赛道','上榜数','总值', '强度', '比例强度', '主线值', '主线值排名', '前日差', '档位']
        style_t.to_excel('今日风格上榜.xlsx')
        field_t.columns = ['赛道','上榜数','总值', '强度', '比例强度', '主线值', '主线值排名', '前日差', '档位']
        field_t.to_excel('今日行业上榜.xlsx')
    else:
        print('方向已更新至最新。')

    disp = pd.read_csv('temp/dispersion.csv').iloc[:,1:]
    df_disp = dire.get_timeseries(disp)
    disp.to_excel('离散度.xlsx')
    

    
    