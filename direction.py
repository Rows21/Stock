import pandas as pd
import numpy as np
from collections import Counter
from tqdm import tqdm

class Direction():
    def __init__(self, daily=False) -> None:
        self.daily = daily
        if self.daily == True:
            self.style = pd.read_csv('temp/style.csv').iloc[:,1:]
            self.field = pd.read_csv('temp/field.csv').iloc[:,1:]
            self.disp = pd.read_csv('temp/dispersion.csv').iloc[:,1:]

    def _rank1(self, df, name='Value'):
        counter11 = Counter(df['一阶1']) + Counter(df['一阶2'])
        counter21 = Counter(df['模糊1']) + Counter(df['模糊2'])
        counter_main = counter11 + Counter({key: value * 0.3 for key, value in counter21.items()})
        del counter_main[0]
        counter_dict = dict(counter_main)
        data = [[key, value] for key, value in counter_dict.items()]
        style_main = pd.DataFrame(data, columns=['Key', name])
        return style_main
    
    def _rank2(self, df, name='Value'):
        counter12 = Counter(df['二阶1']) + Counter(df['二阶2']) + Counter(df['二阶3'])
        counter22 = Counter(df['模糊1.1']) + Counter(df['模糊2.1'])
        counter_main = counter12 + Counter({key: value * 0.3 for key, value in counter22.items()})
        del counter_main[0]
        counter_dict = dict(counter_main)
        data = [[key, value] for key, value in counter_dict.items()]
        field_all = pd.DataFrame(data, columns=['Key', name])
        return field_all

    def get_hist(self, date_list, df_close, df_close_adj, label):
        
        date_list.reverse()
        if not self.daily:
            date_list = date_list[date_list.index('20200103'):] # start from 2020
        else:
            id = df_close[df_close['trade_date'] == int(date_list[-1])].index[0]
            date_list = df_close['trade_date'][:(id+22)].tolist()
            date_list.reverse()
        style_ts = None
        field_ts = None

        #progress_bar = tqdm(total=len(date_list[20:]), ncols=200)
        #all_date = len(date_list[20:])
        # total
        style_all = self._rank1(label)
        if style_ts is None:
            style_ts = pd.DataFrame(columns = ['date'] + style_all['Key'].tolist())

        field_all = self._rank2(label)
        if field_ts is None:
            field_ts = pd.DataFrame(columns = ['date'] + field_all['Key'].tolist())

        last_rank = []
        disp = pd.DataFrame(columns=['date', 'style', 'field'])

        enu_list = date_list[20:]

        for i, date in tqdm(enumerate(enu_list)):
            
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
                print(df_20.iloc[day-1][0])
                chgi = df_20.iloc[0,1:]/df_20.iloc[day-1,1:]
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

            rps_df: pd.DataFrame = pd.merge(rps_df,label,on='证券代码',how='right')
            #rps_df = rps_df.dropna()
            main_top_300, short_top_300 = rps_df.nlargest(300, 'main'), rps_df.nlargest(300, 'short')
            
            # 主线上榜 
            style_main = self._rank1(main_top_300, name='count')
            field_main = self._rank2(main_top_300, name='count')
            strength_list = [style_main, field_main]
            
            if self.daily:
                # 短期上榜-试错
                style_short = self._rank1(short_top_300, name='count')
                field_short = self._rank2(short_top_300, name='count')

                strength_list = [style_main, field_main, style_short, field_short]

            # 强度和比例强度 
            #rank_dif = []
            # 定义分位点列表
            quantiles = [0, 0.4, 0.6, 0.8, 1]
            for k, df in enumerate(strength_list):
                if k % 2 == 0:
                    lab = style_all
                else:
                    lab = field_all
                
                # 强度 比例强度
                df = pd.merge(df,lab,how='outer',on='Key')
                df['vol'] = (df['count'] - np.min(df['count'])) / (np.max(df['count']) - np.min(df['count']))
                df['vol_ratio'] = (df['count']/df['Value'] - np.min(df['count']/df['Value'])) / (np.max(df['count']/df['Value']) - np.min(df['count']/df['Value']))

                # OUT5-8 值
                df['prior'] = df['vol'] * 0.7 + df['vol_ratio'] * 0.3
                # OUT9-12 排名
                df['prior_rank'] = df['prior'].rank(ascending=False)
                
                # OUT13-16 排名变动
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

            #progress_bar.set_description(f"date=: {date}")
            #progress_bar.set_postfix({'Iter': i+1/all_date})

        style_ts.fillna(4)
        field_ts.fillna(4)

        if self.daily:
            style_ts = pd.concat([self.style,style_ts.drop(0)])
            field_ts = pd.concat([self.field,field_ts.drop(0)])
            disp = pd.concat([self.disp,disp.drop(0)])
            style_ts = style_ts.fillna(4)
            field_ts = field_ts.fillna(4)
            disp = disp.fillna(4)
            return style_ts, field_ts, disp, last_rank[0], last_rank[1]
        else:
            return style_ts, field_ts, disp

    def get_timeseries(self, disp: pd.DataFrame):
        date_list = disp['date'].tolist()

        style = [None] * len(disp)
        field = [None] * len(disp)
        total = [None] * len(disp)
        med = [None] * len(disp)
        for i, date in enumerate(date_list):
            style_r = disp.iloc[0:(i+1),]['style'].rank(ascending=False)
            style[i] = 1 - style_r[i]/len(disp)
            
            field_r = disp.iloc[0:(i+1),]['field'].rank(ascending=False)
            field[i] = 1 - field_r[i]/len(disp)

            total[i] = 0.7*style[i] + 0.3*field[i] + 1
            med[i] = np.median(total[:(i+1)])

        disp['style_r'] = style
        disp['field_r'] = field
        disp['total'] = total
        disp['med'] = med

        return disp
        

if __name__ == '__main__':
    import tushare as ts
    # 获取今天的日期
    time = pd.to_datetime('2024-02-08')
    formatted_time = time.strftime("%Y%m%d")

    # token
    token = 'c336245e66e2882632285493a7d0ebc23a2fbb7392b74e4b3855a222'
    ts.set_token(token)
    pro = ts.pro_api()

    print('------------------')
    print('开始方向计算：')
    df_close = pd.read_csv('temp/pre_close.csv').iloc[:,1:]
    df_close_adj = pd.read_csv('temp/pre_close_adj.csv').iloc[:,1:]
    label = pd.read_excel('RPS_label.xlsx', sheet_name='A股数据库20240206')
    close = pd.read_csv('dlogs/style.csv').iloc[:,1:]

    cal = pro.trade_cal(exchange='SZSE', start_date=str(close['date'][len(close)-1]), end_date=formatted_time)
    time_l = cal[cal['is_open'] == 1]['cal_date'].tolist()
    time_l.reverse()

    dire = Direction(daily=True)
    if time_l != []:
        style_ts, field_ts, disp, style_t, field_t = dire.get_hist(time_l, df_close, df_close_adj, label)