"""
q因子模型构建与回测 - 简化版本
基于Hou, Xue & Zhang (2015) q-factor模型

四个因子：
- MKT: 市场因子（沪深300收益率）
- ME: 规模因子（小市值 - 大市值）
- I/A: 投资因子（低投资 - 高投资）
- ROE: 盈利因子（高ROE - 低ROE）
"""

import pandas as pd
import numpy as np
import akshare as ak
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')


class QFactorModel:
    """q因子模型实现（简化版）"""
    
    def __init__(self):
        self.end_date = datetime.now()
        self.start_date = self.end_date - timedelta(days=120)  # 90天回测+30天缓冲
        self.stock_data = None
        self.portfolios = {}
        
    def get_sample_stocks(self, n=50):
        """
        获取样本股票（简化版：直接取沪深300成分股）
        """
        print("正在获取沪深300成分股...")
        try:
            # 获取沪深300成分股
            hs300 = ak.index_stock_cons_weight_csindex(symbol="000300")
            stock_list = hs300['成分券代码'].tolist()[:n]
            print(f"获取到 {len(stock_list)} 只样本股票")
            return stock_list
        except Exception as e:
            print(f"获取失败: {e}")
            # 备用：使用一些常见股票代码
            return ['000001', '000002', '000333', '000568', '000651', 
                    '000725', '000858', '002001', '002007', '002024',
                    '002142', '002230', '002415', '002594', '300003',
                    '300014', '300015', '300033', '300059', '300122',
                    '300124', '300274', '300408', '300750', '600000',
                    '600009', '600016', '600028', '600030', '600031',
                    '600036', '600048', '600050', '600104', '600276',
                    '600309', '600406', '600436', '600519', '600547',
                    '600570', '600585', '600660', '600690', '600745',
                    '600809', '600837', '600887', '600900', '601012',
                    '601088'][:n]
    
    def get_stock_fundamentals(self, stock_codes):
        """
        获取股票基本面数据（市值、ROE等）
        """
        print("\n正在获取股票基本面数据...")
        
        data_list = []
        for i, code in enumerate(stock_codes):
            if i % 10 == 0:
                print(f"  进度: {i+1}/{len(stock_codes)}")
            
            try:
                # 获取个股信息
                info = ak.stock_individual_info_em(symbol=code)
                
                # 获取财务指标
                fin = ak.stock_financial_analysis_indicator(symbol=code)
                if not fin.empty:
                    latest = fin.iloc[0]
                    roe = latest.get('净资产收益率(%)', np.nan)
                else:
                    roe = np.nan
                
                # 获取市值数据
                spot = ak.stock_zh_a_spot_em()
                spot_row = spot[spot['代码'] == code]
                
                if not spot_row.empty:
                    total_mv = spot_row['总市值'].values[0]  # 单位：元
                    # 转换为亿元
                    total_mv = total_mv / 1e8 if total_mv > 1e8 else total_mv
                else:
                    total_mv = np.nan
                
                # 获取资产负债表计算投资率
                try:
                    balance = ak.stock_balance_sheet_by_report_em(symbol=code)
                    if len(balance) >= 2:
                        assets_now = balance.iloc[0].get('资产总计', 0)
                        assets_prev = balance.iloc[1].get('资产总计', 0)
                        ia_ratio = (assets_now - assets_prev) / assets_prev if assets_prev > 0 else 0
                    else:
                        ia_ratio = 0
                except:
                    ia_ratio = 0
                
                data_list.append({
                    'code': code,
                    'name': info.get('股票简称', ''),
                    'total_mv': total_mv,  # 总市值（亿元）
                    'roe': roe,  # ROE（%）
                    'ia_ratio': ia_ratio  # 投资资产比
                })
                
            except Exception as e:
                continue
        
        df = pd.DataFrame(data_list)
        
        # 检查列是否存在
        if 'total_mv' not in df.columns or 'roe' not in df.columns:
            print(f"数据列缺失，可用列: {df.columns.tolist()}")
            return pd.DataFrame()
        
        df = df.dropna(subset=['total_mv', 'roe'])
        
        print(f"成功获取 {len(df)} 只股票数据")
        return df
    
    def construct_factors(self, df):
        """
        构建q因子
        """
        print("\n构建q因子...")
        
        # 按市值分组（5分位）
        df['me_quintile'] = pd.qcut(df['total_mv'], 5, labels=['Q1_S', 'Q2', 'Q3', 'Q4', 'Q5_B'])
        
        # 按ROE分组（5分位，注意ROE越高越好）
        df['roe_quintile'] = pd.qcut(df['roe'].rank(method='first'), 5, 
                                     labels=['Q1_Low', 'Q2', 'Q3', 'Q4', 'Q5_High'])
        
        # 按投资率分组（5分位，注意投资率越低越好）
        df['ia_quintile'] = pd.qcut(df['ia_ratio'].rank(method='first'), 5, 
                                    labels=['Q1_Low', 'Q2', 'Q3', 'Q4', 'Q5_High'])
        
        self.stock_data = df
        
        # 打印分组统计
        print("\n【规模因子(ME)分组】")
        print(df.groupby('me_quintile')['total_mv'].agg(['count', 'mean']))
        
        print("\n【盈利因子(ROE)分组】")
        print(df.groupby('roe_quintile')['roe'].agg(['count', 'mean']))
        
        print("\n【投资因子(I/A)分组】")
        print(df.groupby('ia_quintile')['ia_ratio'].agg(['count', 'mean']))
        
        return df
    
    def build_portfolios(self):
        """
        构建投资组合
        """
        print("\n构建投资组合...")
        
        df = self.stock_data
        
        # ME因子组合：小市值 - 大市值
        small_caps = df[df['me_quintile'] == 'Q1_S']['code'].tolist()
        big_caps = df[df['me_quintile'] == 'Q5_B']['code'].tolist()
        
        # ROE因子组合：高ROE - 低ROE
        high_roe = df[df['roe_quintile'] == 'Q5_High']['code'].tolist()
        low_roe = df[df['roe_quintile'] == 'Q1_Low']['code'].tolist()
        
        # I/A因子组合：低投资 - 高投资
        low_ia = df[df['ia_quintile'] == 'Q1_Low']['code'].tolist()
        high_ia = df[df['ia_quintile'] == 'Q5_High']['code'].tolist()
        
        self.portfolios = {
            'ME_Small': small_caps,
            'ME_Big': big_caps,
            'ROE_High': high_roe,
            'ROE_Low': low_roe,
            'IA_Low': low_ia,
            'IA_High': high_ia
        }
        
        for name, stocks in self.portfolios.items():
            print(f"  {name}: {len(stocks)}只股票")
        
        return self.portfolios
    
    def get_historical_returns(self, stock_codes, start_date, end_date):
        """
        获取股票历史收益率
        """
        returns_dict = {}
        
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        
        for code in stock_codes:
            try:
                df = ak.stock_zh_a_hist(symbol=code, period="daily", 
                                       start_date=start_str, end_date=end_str, adjust="qfq")
                if not df.empty and len(df) > 1:
                    df['日期'] = pd.to_datetime(df['日期'])
                    df.set_index('日期', inplace=True)
                    df['return'] = df['收盘'].pct_change()
                    returns_dict[code] = df['return']
            except:
                continue
        
        return returns_dict
    
    def backtest(self, days=90):
        """
        进行回测
        """
        print(f"\n开始{days}天回测...")
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        results = {}
        
        # 获取市场基准（沪深300）
        print("  获取沪深300数据...")
        try:
            hs300 = ak.index_zh_a_hist(symbol="000300", period="daily",
                                      start_date=start_date.strftime('%Y%m%d'),
                                      end_date=end_date.strftime('%Y%m%d'))
            hs300['日期'] = pd.to_datetime(hs300['日期'])
            hs300.set_index('日期', inplace=True)
            results['MKT'] = hs300['收盘'].pct_change()
        except Exception as e:
            print(f"  获取沪深300失败: {e}")
        
        # 对每个组合进行回测
        for portfolio_name, stock_codes in self.portfolios.items():
            if len(stock_codes) == 0:
                continue
            
            print(f"  回测组合: {portfolio_name}")
            
            # 获取组合内股票收益率
            returns_dict = self.get_historical_returns(stock_codes[:15], start_date, end_date)
            
            if returns_dict:
                # 计算等权组合日收益率
                returns_df = pd.DataFrame(returns_dict)
                portfolio_return = returns_df.mean(axis=1)
                results[portfolio_name] = portfolio_return
        
        # 汇总结果
        if results:
            results_df = pd.DataFrame(results)
            results_df = results_df.dropna(how='all')
            
            # 计算累计收益
            cumulative = (1 + results_df.fillna(0)).cumprod() - 1
            
            # 计算统计指标
            stats = {}
            for col in results_df.columns:
                returns = results_df[col].dropna()
                if len(returns) > 10:  # 至少10个交易日
                    total_return = cumulative[col].iloc[-1]
                    annual_return = returns.mean() * 252
                    annual_vol = returns.std() * np.sqrt(252)
                    sharpe = annual_return / annual_vol if annual_vol > 0 else 0
                    max_dd = (cumulative[col] - cumulative[col].cummax()).min()
                    
                    stats[col] = {
                        '累计收益': f"{total_return*100:.2f}%",
                        '年化收益': f"{annual_return*100:.2f}%",
                        '年化波动': f"{annual_vol*100:.2f}%",
                        '夏普比率': f"{sharpe:.2f}",
                        '最大回撤': f"{max_dd*100:.2f}%"
                    }
            
            return {
                'daily_returns': results_df,
                'cumulative': cumulative,
                'statistics': pd.DataFrame(stats).T
            }
        
        return None
    
    def calculate_factor_returns(self, backtest_results):
        """
        计算q因子多空收益
        """
        print("\n【q因子多空收益分析】")
        print("-" * 50)
        
        daily = backtest_results['daily_returns']
        
        factors = {}
        
        # ME因子：小市值 - 大市值
        if 'ME_Small' in daily.columns and 'ME_Big' in daily.columns:
            me_factor = daily['ME_Small'] - daily['ME_Big']
            factors['ME'] = me_factor
            print(f"ME因子（小-大）:")
            print(f"  日均收益: {me_factor.mean()*100:.4f}%")
            print(f"  累计收益: {((1+me_factor.fillna(0)).cumprod()-1).iloc[-1]*100:.2f}%")
        
        # ROE因子：高ROE - 低ROE
        if 'ROE_High' in daily.columns and 'ROE_Low' in daily.columns:
            roe_factor = daily['ROE_High'] - daily['ROE_Low']
            factors['ROE'] = roe_factor
            print(f"\nROE因子（高-低）:")
            print(f"  日均收益: {roe_factor.mean()*100:.4f}%")
            print(f"  累计收益: {((1+roe_factor.fillna(0)).cumprod()-1).iloc[-1]*100:.2f}%")
        
        # I/A因子：低投资 - 高投资
        if 'IA_Low' in daily.columns and 'IA_High' in daily.columns:
            ia_factor = daily['IA_Low'] - daily['IA_High']
            factors['I/A'] = ia_factor
            print(f"\nI/A因子（低-高）:")
            print(f"  日均收益: {ia_factor.mean()*100:.4f}%")
            print(f"  累计收益: {((1+ia_factor.fillna(0)).cumprod()-1).iloc[-1]*100:.2f}%")
        
        return factors


def main():
    """主函数"""
    print("="*60)
    print("q因子模型构建与90天回测")
    print("="*60)
    print("\n模型说明：")
    print("  MKT: 市场因子（沪深300）")
    print("  ME:  规模因子（小市值 - 大市值）")
    print("  I/A: 投资因子（低投资 - 高投资）")
    print("  ROE: 盈利因子（高ROE - 低ROE）")
    print("="*60)
    
    # 初始化模型
    model = QFactorModel()
    
    # 获取样本股票
    stock_codes = model.get_sample_stocks(n=50)
    
    # 获取基本面数据
    fundamentals = model.get_stock_fundamentals(stock_codes)
    
    if fundamentals.empty:
        print("\n无法获取股票数据，回测终止")
        return
    
    # 构建因子
    model.construct_factors(fundamentals)
    
    # 构建投资组合
    model.build_portfolios()
    
    # 进行90天回测
    results = model.backtest(days=90)
    
    if results:
        # 打印统计结果
        print("\n" + "="*60)
        print("【回测结果统计】")
        print("="*60)
        print(results['statistics'])
        
        # 计算因子收益
        factors = model.calculate_factor_returns(results)
        
        # 保存结果
        print("\n" + "="*60)
        print("保存结果...")
        results['statistics'].to_csv('/Users/ydy/WorkBuddy/20260326134350/q_factor_stats.csv')
        results['cumulative'].to_csv('/Users/ydy/WorkBuddy/20260326134350/q_factor_cumulative.csv')
        print("  ✓ q_factor_stats.csv")
        print("  ✓ q_factor_cumulative.csv")
        print("="*60)
    else:
        print("\n回测失败")


if __name__ == "__main__":
    main()
