#!/usr/bin/env python3
"""
A股多因子选股脚本 - Baostock版本
基于q因子模型：ROE(盈利) + 低投资(I/A) + 规模(小市值)
覆盖全市场5000+只股票
"""

import baostock as bs
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


class BaostockScreener:
    """基于Baostock的多因子选股器"""
    
    def __init__(self):
        self.lg = None
        self.stock_list = None
        self.financial_data = []
        
    def login(self):
        """登录Baostock"""
        self.lg = bs.login()
        if self.lg.error_code != '0':
            raise Exception(f"登录失败: {self.lg.error_msg}")
        print("✅ Baostock 登录成功")
        return True
    
    def logout(self):
        """登出"""
        if self.lg:
            bs.logout()
            print("✅ 已登出")
    
    def get_all_stocks(self):
        """获取所有A股列表"""
        print("\n📊 获取全市场股票列表...")
        query_date = '2025-03-28'
        rs = bs.query_all_stock(day=query_date)
        
        if rs.error_code != '0':
            print(f"   查询失败: {rs.error_msg}")
            return pd.DataFrame()
        
        stock_list = []
        count = 0
        while rs.next():
            row = rs.get_row_data()
            count += 1
            code = row[0]
            # 只保留沪深A股
            if (code.startswith('sh.6') or 
                code.startswith('sz.000') or 
                code.startswith('sz.002') or 
                code.startswith('sz.300')):
                stock_list.append({
                    'code': code,
                    'trade_status': row[1],
                    'name': row[2] if len(row) > 2 else ''
                })
        
        self.stock_list = pd.DataFrame(stock_list)
        print(f"   共扫描 {count} 条记录，筛选出 {len(self.stock_list)} 只A股")
        return self.stock_list
    
    def get_financial_data(self, code, year=2024, quarter=3):
        """获取单只股票的财务数据"""
        try:
            # 1. 杜邦分析数据（ROE、资产负债率等）
            rs_dupont = bs.query_dupont_data(code=code, year=year, quarter=quarter)
            dupont_data = []
            while (rs_dupont.error_code == '0') & rs_dupont.next():
                dupont_data.append(rs_dupont.get_row_data())
            
            if not dupont_data:
                return None
            
            df_dupont = pd.DataFrame(dupont_data, columns=rs_dupont.fields)
            
            # 2. 成长能力数据（资产增长率）
            rs_growth = bs.query_growth_data(code=code, year=year, quarter=quarter)
            growth_data = []
            while (rs_growth.error_code == '0') & rs_growth.next():
                growth_data.append(rs_growth.get_row_data())
            
            df_growth = pd.DataFrame(growth_data, columns=rs_growth.fields) if growth_data else None
            
            # 3. 盈利能力数据
            rs_profit = bs.query_profit_data(code=code, year=year, quarter=quarter)
            profit_data = []
            while (rs_profit.error_code == '0') & rs_profit.next():
                profit_data.append(rs_profit.get_row_data())
            
            df_profit = pd.DataFrame(profit_data, columns=rs_profit.fields) if profit_data else None
            
            # 合并数据
            result = {
                'code': code,
                'name': df_dupont.iloc[0].get('code_name', ''),
                'pub_date': df_dupont.iloc[0].get('pubDate', ''),
                'roe': float(df_dupont.iloc[0].get('dupontROE', 0) or 0),
                'asset_to_equity': float(df_dupont.iloc[0].get('dupontAssetStoEquity', 0) or 0),
            }
            
            # 从成长能力表获取资产增长率
            if df_growth is not None and not df_growth.empty:
                result['yoy_asset'] = float(df_growth.iloc[0].get('YOYAsset', 0) or 0)
                result['yoy_equity'] = float(df_growth.iloc[0].get('YOYEquity', 0) or 0)
            else:
                result['yoy_asset'] = 0
                result['yoy_equity'] = 0
            
            # 从盈利能力表获取净利率
            if df_profit is not None and not df_profit.empty:
                result['net_profit_margin'] = float(df_profit.iloc[0].get('netProfitMargin', 0) or 0)
                result['gross_profit_margin'] = float(df_profit.iloc[0].get('grossProfitMargin', 0) or 0)
            else:
                result['net_profit_margin'] = 0
                result['gross_profit_margin'] = 0
            
            return result
            
        except Exception as e:
            return None
    
    def batch_get_financials(self, max_stocks=None):
        """批量获取财务数据"""
        if self.stock_list is None or self.stock_list.empty:
            raise ValueError("请先调用get_all_stocks()")
        
        stocks_to_process = self.stock_list.head(max_stocks) if max_stocks else self.stock_list
        total = len(stocks_to_process)
        
        print(f"\n📈 批量获取财务数据（共 {total} 只股票）...")
        print("   这可能需要几分钟，请耐心等待...\n")
        
        financial_list = []
        failed_list = []
        last_progress = -1
        
        for idx, row in stocks_to_process.iterrows():
            code = row['code']
            
            # 每10只打印一次进度，避免超时
            progress = int((idx / total) * 100)
            if progress != last_progress and progress % 5 == 0:
                print(f"   进度: {idx}/{total} ({progress}%)", flush=True)
                last_progress = progress
            
            data = self.get_financial_data(code)
            if data:
                financial_list.append(data)
            else:
                failed_list.append(code)
        
        print(f"\n✅ 成功获取 {len(financial_list)} 只股票数据")
        if failed_list:
            print(f"⚠️  失败 {len(failed_list)} 只")
        
        self.financial_data = pd.DataFrame(financial_list)
        return self.financial_data
    
    def calculate_factors(self):
        """计算因子得分"""
        print("\n🎯 计算多因子得分...")
        
        df = self.financial_data.copy()
        
        # 基础过滤
        df = df[df['roe'] > 0]  # ROE为正
        df = df[df['roe'] < 1]  # ROE小于100%（去极值）
        
        print(f"   基础过滤后: {len(df)} 只股票")
        
        if len(df) == 0:
            print("⚠️  没有符合条件的股票")
            self.financial_data = df
            return df
        
        # 计算资产负债率（从权益乘数推算：权益乘数 = 总资产/净资产 = 1/(1-资产负债率)）
        df['debt_ratio'] = 1 - 1/df['asset_to_equity'].clip(lower=1.0)
        
        # 投资强度 I/A = 总资产增长率
        df['investment_ia'] = df['yoy_asset']
        
        # 因子1: ROE得分（越高越好）
        df['roe_score'] = self._percentile_score(df['roe'])
        
        # 因子2: 低投资得分（越低越好）
        df['inv_score'] = 100 - self._percentile_score(df['investment_ia'])
        
        # 因子3: 低杠杆得分（越低越好）
        df['lev_score'] = 100 - self._percentile_score(df['debt_ratio'].fillna(0.5))
        
        # 因子4: 高净利率得分
        df['margin_score'] = self._percentile_score(df['net_profit_margin'])
        
        # 综合得分（q因子权重）
        df['composite_score'] = (
            df['roe_score'] * 0.40 +      # ROE 40%
            df['inv_score'] * 0.30 +       # 低投资 30%
            df['lev_score'] * 0.15 +       # 低杠杆 15%
            df['margin_score'] * 0.15      # 净利率 15%
        )
        
        # 排序
        df = df.sort_values('composite_score', ascending=False).reset_index(drop=True)
        df['rank'] = range(1, len(df) + 1)
        
        self.financial_data = df
        print(f"✅ 因子计算完成")
        return df
    
    def _percentile_score(self, series):
        """计算百分位得分（0-100）"""
        # 去极值（3倍标准差）
        mean = series.mean()
        std = series.std()
        lower = mean - 3 * std
        upper = mean + 3 * std
        series = series.clip(lower, upper)
        
        # 计算排名百分位
        return series.rank(pct=True) * 100
    
    def get_top_picks(self, n=50):
        """获取Top N选股结果"""
        if self.financial_data.empty:
            return pd.DataFrame()
        
        return self.financial_data.head(n)[
            ['rank', 'code', 'name', 'roe', 'yoy_asset', 'debt_ratio', 
             'net_profit_margin', 'roe_score', 'inv_score', 
             'lev_score', 'margin_score', 'composite_score']
        ]
    
    def generate_report(self):
        """生成可视化报告"""
        print("\n📊 生成选股报告...")
        
        if self.financial_data.empty:
            print("⚠️  没有数据可生成报告")
            return None
        
        top50 = self.get_top_picks(50)
        
        # 创建图表
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('A股多因子选股报告 (Baostock数据源)', fontsize=16, fontweight='bold')
        
        # 1. Top 20 综合得分
        ax1 = axes[0, 0]
        top20 = top50.head(20)
        colors = plt.cm.RdYlGn(top20['composite_score'] / 100)
        bars = ax1.barh(range(len(top20)), top20['composite_score'], color=colors)
        ax1.set_yticks(range(len(top20)))
        ax1.set_yticklabels([f"{row['name'][:6]}" for _, row in top20.iterrows()], fontsize=8)
        ax1.invert_yaxis()
        ax1.set_xlabel('综合得分')
        ax1.set_title('Top 20 综合得分排名')
        ax1.grid(axis='x', alpha=0.3)
        
        # 2. 因子得分分布
        ax2 = axes[0, 1]
        factor_cols = ['roe_score', 'inv_score', 'lev_score', 'margin_score']
        factor_names = ['ROE得分', '低投资得分', '低杠杆得分', '净利率得分']
        
        top20_factors = top20[factor_cols].mean()
        bars2 = ax2.bar(factor_names, top20_factors, color=['#e74c3c', '#3498db', '#2ecc71', '#9b59b6'])
        ax2.set_ylabel('平均得分')
        ax2.set_title('Top 20 股票各因子平均得分')
        ax2.set_ylim(0, 100)
        ax2.grid(axis='y', alpha=0.3)
        plt.setp(ax2.xaxis.get_majorticklabels(), rotation=15, ha='right')
        
        # 3. ROE vs 综合得分散点图
        ax3 = axes[1, 0]
        scatter = ax3.scatter(
            self.financial_data['roe'] * 100, 
            self.financial_data['composite_score'],
            c=self.financial_data['composite_score'],
            cmap='RdYlGn',
            alpha=0.5,
            s=20
        )
        # 标注Top 10
        for _, row in top50.head(10).iterrows():
            ax3.annotate(
                row['name'][:4],
                (row['roe'] * 100, row['composite_score']),
                fontsize=7,
                alpha=0.8
            )
        ax3.set_xlabel('ROE (%)')
        ax3.set_ylabel('综合得分')
        ax3.set_title('ROE vs 综合得分分布')
        ax3.grid(alpha=0.3)
        plt.colorbar(scatter, ax=ax3, label='综合得分')
        
        # 4. 各得分段股票数量
        ax4 = axes[1, 1]
        score_bins = pd.cut(self.financial_data['composite_score'], bins=10)
        score_counts = score_bins.value_counts().sort_index()
        bin_labels = [f"{int(interval.left)}-{int(interval.right)}" for interval in score_counts.index]
        bars4 = ax4.bar(bin_labels, score_counts.values, color=plt.cm.RdYlGn(np.linspace(0.3, 0.9, 10)))
        ax4.set_xlabel('综合得分区间')
        ax4.set_ylabel('股票数量')
        ax4.set_title('全市场综合得分分布')
        plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45, ha='right')
        ax4.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig('stock_screener_baostock_report.png', dpi=150, bbox_inches='tight')
        print("✅ 报告已保存: stock_screener_baostock_report.png")
        
        return fig
    
    def export_results(self, filename='selected_stocks_baostock.csv'):
        """导出选股结果"""
        top50 = self.get_top_picks(50)
        if not top50.empty:
            top50.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"✅ 结果已导出: {filename}")
        return top50
    
    def run(self, max_stocks=None):
        """运行完整选股流程"""
        try:
            self.login()
            self.get_all_stocks()
            self.batch_get_financials(max_stocks=max_stocks)
            self.calculate_factors()
            
            if not self.financial_data.empty:
                self.generate_report()
                results = self.export_results()
                
                print("\n" + "="*60)
                print("🎉 A股多因子选股完成！")
                print("="*60)
                print(f"\n📈 本次共筛选 {len(self.financial_data)} 只股票")
                print(f"📊 综合得分范围: {self.financial_data['composite_score'].min():.1f} - {self.financial_data['composite_score'].max():.1f}")
                print(f"\n🏆 Top 10 候选股票:")
                print("-"*60)
                
                for _, row in results.head(10).iterrows():
                    print(f"  {row['rank']:2d}. {row['name'][:8]:8s} ({row['code']})  "
                          f"ROE:{row['roe']*100:5.1f}%  资产增速:{row['yoy_asset']*100:6.1f}%  "
                          f"得分:{row['composite_score']:5.1f}")
                
                print("-"*60)
            else:
                results = pd.DataFrame()
                print("\n⚠️  没有符合条件的股票")
            
            print("\n⚠️  免责声明：本选股结果仅供研究参考，不构成投资建议")
            
            return results
            
        finally:
            self.logout()


def main():
    """主函数"""
    print("="*60)
    print("A股多因子选股系统 (Baostock版)")
    print("="*60)
    print("\n选股逻辑：")
    print("  • ROE (盈利能力) × 40%")
    print("  • 低投资 (资产增速低) × 30%")
    print("  • 低杠杆 × 15%")
    print("  • 净利率 × 15%")
    print("="*60)
    
    screener = BaostockScreener()
    
    # 可以设置 max_stocks 来限制处理数量，None表示全市场
    results = screener.run(max_stocks=None)  # 跑全市场4566只
    
    return results


if __name__ == "__main__":
    main()
