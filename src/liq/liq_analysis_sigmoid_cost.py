import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import percentileofscore
import os

# ----------------------------------------------------------------------
# 0. 全局设置
# ----------------------------------------------------------------------
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# 文件读取路径
FILE_PATH = './data/realtime_data_updated.csv'

# 回测时间段
BACKTEST_START_DATE = '2023-06-17'
BACKTEST_END_DATE = '2099-12-31'

# 策略起始日期
STRATEGY_START_DATE = '2026-01-25'

# 策略参数
WINDOW_SIZE = 40        # 计算信号的滚动窗口
HISTORY_WINDOW = 20     # 历史分位数窗口
REBALANCE_THRESHOLD = 0.03  # 调仓阈值：仓位变化超过3%才调仓（减少无效交易）

# 交易成本与滑点（单边）
COST = 0.0002           # 手续费率 0.02%
SLIPPAGE = 0.0003       # 滑点 0.03%
TOTAL_COST_PER_TRADE = COST + SLIPPAGE  # 单边总成本

# ----------------------------------------------------------------------
# 1. 数据加载与预处理
# ----------------------------------------------------------------------
if not os.path.exists(FILE_PATH):
    print(f"❌ 错误：找不到文件 {FILE_PATH}")
    exit()

try:
    df = pd.read_csv(FILE_PATH, parse_dates=['TradingDay'])
    print(f"✅ 成功加载数据: {len(df)} 条记录")
except Exception as e:
    print(f"❌ 无法读取文件: {e}")
    exit()

# 按照交易日排序并设置索引
df = df.set_index('TradingDay').sort_index()

# 重命名列以便后续处理
df.rename(columns={
    'turnover_volume1': 'TV_500', 
    'turnover_volume2': 'TV_HL',
    'change_pct1': 'Ret_Index_500', 
    'change_pct2': 'Ret_Index_HL',
    'close_price1': 'Close_500', 
    'close_price2': 'Close_HL'
}, inplace=True)

# ----------------------------------------------------------------------
# 2. 信号构建（修正：使用滚动窗口标准化，避免未来函数）
# ----------------------------------------------------------------------
df['Signal_500'] = df['Ret_Index_500'].rolling(window=WINDOW_SIZE).mean() / (df['TV_500'].rolling(window=WINDOW_SIZE).mean() + 1e-10)
df['Signal_HL'] = df['Ret_Index_HL'].rolling(window=WINDOW_SIZE).mean() / (df['TV_HL'].rolling(window=WINDOW_SIZE).mean() + 1e-10)

# 修正：使用滚动窗口标准化（避免使用全序列统计量导致的未来函数）
df['Signal_500'] = (df['Signal_500'] - df['Signal_500'].rolling(WINDOW_SIZE*2).mean()) / (df['Signal_500'].rolling(WINDOW_SIZE*2).std() + 1e-10)
df['Signal_HL'] = (df['Signal_HL'] - df['Signal_HL'].rolling(WINDOW_SIZE*2).mean()) / (df['Signal_HL'].rolling(WINDOW_SIZE*2).std() + 1e-10)

df['Signal'] = df['Signal_500'] - df['Signal_HL']
df['Signal_Rank'] = df['Signal'].rolling(window=HISTORY_WINDOW).apply(
    lambda x: percentileofscore(x, x.iloc[-1]) / 100 if len(x) > 1 else 0.5, 
    raw=False
)

# 可视化分位数分布
shift = 0
rank_shifted = df['Signal_Rank'].shift(-shift)

fig, ax1 = plt.subplots(figsize=(10, 4))
ax1.plot(df.index, rank_shifted, color='tab:blue', label=f'Signal Rank (lead {shift})')
ax1.set_xlabel('Date')
ax1.set_ylabel('Signal Rank', color='tab:blue')
ax1.tick_params(axis='y', labelcolor='tab:blue')

ax2 = ax1.twinx()
ax2.plot(df.index, df['Close_500'], color='tab:orange', label='Close_500')
ax2.plot(df.index, df['Close_HL'], color='tab:green', label='Close_HL')
ax2.set_ylabel('Price', color='tab:orange')
ax2.tick_params(axis='y', labelcolor='tab:orange')

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='best')
ax1.grid(True)
plt.title('Signal Rank Over Time (shifted)')
plt.savefig('results/liq/signal_rank_shifted.png', dpi=300, bbox_inches='tight')
plt.close()  # 避免显示干扰

# 预览信号构建结果
print("\n🔍 信号构建预览:")
print(df[['Signal_500', 'Signal_HL', 'Signal', 'Signal_Rank']].tail(20))

# ----------------------------------------------------------------------
# 3. 仓位管理（含调仓阈值控制）
# ----------------------------------------------------------------------
SIGMOID_SCALE = 8

def sigmoid(x, scale=SIGMOID_SCALE):
    return 1 / (1 + np.exp(-scale * x))

df_bt = df.loc[BACKTEST_START_DATE:BACKTEST_END_DATE].copy()

# 生成目标仓位
df_bt['Target_500'] = sigmoid(df_bt['Signal_Rank'] - 0.5, scale=SIGMOID_SCALE)
df_bt['Target_HL'] = 1 - df_bt['Target_500']

# 应用调仓阈值：仅当目标仓位变化超过阈值时才调仓
df_bt['Position_500_prev'] = df_bt['Target_500'].shift(1).fillna(0.5)
df_bt['Position_HL_prev'] = 1 - df_bt['Position_500_prev']

# 计算是否需要调仓
df_bt['Need_Rebalance'] = abs(df_bt['Target_500'] - df_bt['Position_500_prev']) >= REBALANCE_THRESHOLD
df_bt['Position_500'] = np.where(
    df_bt['Need_Rebalance'], 
    df_bt['Target_500'], 
    df_bt['Position_500_prev']
)
df_bt['Position_HL'] = 1 - df_bt['Position_500']

# 预览仓位调整结果
print("\n🔍 仓位调整预览 (含调仓阈值):")
print(df_bt[['Target_500', 'Position_500', 'Need_Rebalance']].tail(20))
print(f"📊 调仓频率统计: 总交易日 {len(df_bt)}, 实际调仓次数 {df_bt['Need_Rebalance'].sum()}, 调仓比例 {df_bt['Need_Rebalance'].mean():.2%}")

# ----------------------------------------------------------------------
# 4. 回测执行（含精确交易成本计算）
# ----------------------------------------------------------------------
# 毛策略收益（未扣成本）
df_bt['Strategy_Return_Gross'] = (
    df_bt['Position_500'] * df_bt['Ret_Index_500'] + 
    df_bt['Position_HL'] * df_bt['Ret_Index_HL']
) / 100

# 基准收益（50-50固定组合）
df_bt['Benchmark_Return'] = (0.5 * df_bt['Ret_Index_500'] + 0.5 * df_bt['Ret_Index_HL']) / 100

# ===== 核心：交易成本计算 =====
# 计算实际调仓幅度（仅当调仓发生时）
df_bt['Position_Change'] = abs(df_bt['Position_500'] - df_bt['Position_500'].shift(1))
df_bt['Position_Change'].iloc[0] = 0  # 首日无调仓

# 双边交易成本：卖出旧仓位 + 买入新仓位 = 2 * 调仓幅度 * 单边成本
df_bt['Transaction_Cost'] = df_bt['Position_Change'] * TOTAL_COST_PER_TRADE * 2

# 净策略收益 = 毛收益 - 交易成本
df_bt['Strategy_Return'] = df_bt['Strategy_Return_Gross'] - df_bt['Transaction_Cost']

# 累计净值
df_bt['Cumulative_Strategy_Return'] = (1 + df_bt['Strategy_Return']).cumprod()
df_bt['Cumulative_Strategy_Return_Gross'] = (1 + df_bt['Strategy_Return_Gross']).cumprod()
df_bt['Cumulative_Benchmark_Return'] = (1 + df_bt['Benchmark_Return']).cumprod()
df_bt['Excess_Return'] = df_bt['Cumulative_Strategy_Return'] - df_bt['Cumulative_Benchmark_Return']

# 最大回撤
df_bt['Roll_Max'] = df_bt['Cumulative_Strategy_Return'].cummax()
df_bt['Drawdown'] = df_bt['Cumulative_Strategy_Return'] / df_bt['Roll_Max'] - 1
max_dd = df_bt['Drawdown'].min()

# ----------------------------------------------------------------------
# 5. 绩效报告（增强版）
# ----------------------------------------------------------------------
strategy_ret = df_bt['Cumulative_Strategy_Return'].iloc[-1] - 1
strategy_ret_gross = df_bt['Cumulative_Strategy_Return_Gross'].iloc[-1] - 1
benchmark_ret = df_bt['Cumulative_Benchmark_Return'].iloc[-1] - 1
excess_ret = df_bt['Excess_Return'].iloc[-1]
total_cost = df_bt['Transaction_Cost'].sum()
cost_as_pct_of_gross = total_cost / abs(strategy_ret_gross) if strategy_ret_gross != 0 else 0
turnover_rate = df_bt['Position_Change'].sum()  # 总换手率（双边）

print("-" * 60)
print("📊 回测绩效摘要 (含交易成本)")
print("-" * 60)
print(f"📈 策略总收益率 (净):    {strategy_ret*100:8.2f}%")
print(f"📈 策略总收益率 (毛):    {strategy_ret_gross*100:8.2f}%")
print(f"📉 基准总收益率:        {benchmark_ret*100:8.2f}%")
print(f"🔥 相对超额收益 (净):   {excess_ret*100:8.2f}%")
print(f"💸 总交易成本:          {total_cost*100:8.2f}% (占毛收益 {cost_as_pct_of_gross:.1%})")
print(f"🔄 总换手率 (双边):     {turnover_rate:.2f}x")
print(f"⚖️  调仓次数/总交易日:  {df_bt['Need_Rebalance'].sum()} / {len(df_bt)} ({df_bt['Need_Rebalance'].mean():.1%})")
print(f"📉 最大回撤:            {max_dd*100:8.2f}%")
print("-" * 60)
print(f"📅 特定区间统计: 【 {STRATEGY_START_DATE} 至今 】")
print("-" * 60)
try:
    df_spec = df_bt.loc[STRATEGY_START_DATE:]
    if not df_spec.empty:
        s_ret_net = df_spec['Cumulative_Strategy_Return'].iloc[-1] / df_spec['Cumulative_Strategy_Return'].iloc[0] - 1
        s_ret_gross = df_spec['Cumulative_Strategy_Return_Gross'].iloc[-1] / df_spec['Cumulative_Strategy_Return_Gross'].iloc[0] - 1
        b_ret = df_spec['Cumulative_Benchmark_Return'].iloc[-1] / df_spec['Cumulative_Benchmark_Return'].iloc[0] - 1
        excess_spec = s_ret_net - b_ret
        interval_cost = df_spec['Transaction_Cost'].sum()
        print(f"   🔹 策略区间收益 (净): {s_ret_net:.2%}")
        print(f"   🔹 策略区间收益 (毛): {s_ret_gross:.2%}")
        print(f"   🔹 基准区间收益:      {b_ret:.2%}")
        print(f"   🔥 区间超额收益 (净): {excess_spec:.2%}")
        print(f"   💸 区间交易成本:      {interval_cost:.2%}")
    else:
        print(f"   ⚠️ 数据未覆盖到 {STRATEGY_START_DATE}")
except Exception as e:
    print(f"   ⚠️ 统计计算错误: {e}")

# ----------------------------------------------------------------------
# 6. 可视化（增强：毛/净收益对比 + 成本曲线）
# ----------------------------------------------------------------------
fig, axs = plt.subplots(4, 1, figsize=(12, 14), sharex=True)

# 子图1：累计收益对比
axs[0].plot(df_bt.index, df_bt['Cumulative_Strategy_Return'], label='策略累计收益 (净)', linewidth=2.5)
axs[0].plot(df_bt.index, df_bt['Cumulative_Strategy_Return_Gross'], label='策略累计收益 (毛)', linestyle='--', alpha=0.7)
axs[0].plot(df_bt.index, df_bt['Cumulative_Benchmark_Return'], label='基准累计收益 (50-50)', linewidth=2)
axs[0].set_title('累计收益对比：策略(净/毛) vs 基准', fontsize=13, fontweight='bold')
axs[0].legend(loc='upper left')
axs[0].grid(True, linestyle='--', alpha=0.7)

# 子图2：累计超额收益 + 交易成本累积
axs[1].plot(df_bt.index, df_bt['Excess_Return'], color='darkgreen', label='累计超额收益 (净)', linewidth=2)
axs[1].fill_between(df_bt.index, df_bt['Excess_Return'], alpha=0.3, color='green')
axs[1].set_title('累计超额收益 (策略净收益 - 基准)', fontsize=13, fontweight='bold')
axs[1].legend(loc='upper left')
axs[1].grid(True, linestyle='--', alpha=0.7)

# 子图3：仓位分布 + 调仓标记
axs[2].plot(df_bt.index, df_bt['Position_500'], label='500仓位', linewidth=2)
axs[2].plot(df_bt.index, df_bt['Position_HL'], label='红利仓位', linewidth=2)
# 标记实际调仓日
rebalance_dates = df_bt[df_bt['Need_Rebalance']].index
if len(rebalance_dates) > 0:
    axs[2].scatter(rebalance_dates, df_bt.loc[rebalance_dates, 'Position_500'], 
                   color='red', s=15, zorder=5, label='实际调仓日', alpha=0.7)
axs[2].set_title(f'仓位分布 (调仓阈值: {REBALANCE_THRESHOLD:.0%})', fontsize=13, fontweight='bold')
axs[2].legend(loc='upper left')
axs[2].grid(True, linestyle='--', alpha=0.7)
axs[2].set_ylabel('仓位比例')

# 子图4：单日交易成本分布
axs[3].bar(df_bt.index, df_bt['Transaction_Cost'] * 100, color='salmon', alpha=0.7, width=1.5)
axs[3].axhline(y=0, color='black', linewidth=0.8)
axs[3].set_title('单日交易成本 (% 总资产)', fontsize=13, fontweight='bold')
axs[3].set_ylabel('成本 (%)')
axs[3].grid(True, linestyle='--', alpha=0.7)
axs[3].set_xlabel('日期')

plt.tight_layout()
plt.savefig('results/liq/backtest_results_with_costs.png', dpi=300, bbox_inches='tight')
print(f"\n✅ 回测图表已保存至: results/liq/backtest_results_with_costs.png")
plt.show()

# ----------------------------------------------------------------------
# 7. 保存详细交易记录（可选）
# ----------------------------------------------------------------------
trade_summary = df_bt[['Position_500', 'Position_HL', 'Position_Change', 'Transaction_Cost', 
                      'Strategy_Return_Gross', 'Strategy_Return', 'Benchmark_Return']].copy()
trade_summary.to_csv('results/liq/trade_details_with_costs.csv', index=True, encoding='utf-8-sig')
print(f"✅ 详细交易记录已保存至: results/liq/trade_details_with_costs.csv")