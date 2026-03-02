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
WINDOW_SIZE = 40 # 计算信号的滚动窗口
HISTORY_WINDOW = 20  # 历史分位数窗口

# 交易成本与滑点
COST = 0.0002
SLIPPAGE = 0.0003

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
# 交易量重命名
df.rename(columns={'turnover_volume1': 'TV_500', 'turnover_volume2': 'TV_HL'}, inplace=True)
# 涨跌幅重命名
df.rename(columns={'change_pct1': 'Ret_Index_500', 'change_pct2': 'Ret_Index_HL'}, inplace=True)
# 收盘价重命名
df.rename(columns={'close_price1': 'Close_500', 'close_price2': 'Close_HL'}, inplace=True)

# ----------------------------------------------------------------------
# 2. 信号构建
# ----------------------------------------------------------------------
# 初始信号（窗口期内的平均收益率/窗口期内的平均交易量）
"""
指标越高时，说明较小的成交量就能够导致较大幅度的价格变化，这意味着这笔交易对于个股价
格的冲击较大，从某种层面意味着个股的流动性越差。
"""
df['Signal_500'] = df['Ret_Index_500'].rolling(window=WINDOW_SIZE).mean() / df['TV_500'].rolling(window=WINDOW_SIZE).mean()
df['Signal_HL'] = df['Ret_Index_HL'].rolling(window=WINDOW_SIZE).mean() / df['TV_HL'].rolling(window=WINDOW_SIZE).mean()

# 信号标准化（因为两者市值不同）
df['Signal_500'] = (df['Signal_500'] - df['Signal_500'].mean()) / df['Signal_500'].std()
df['Signal_HL'] = (df['Signal_HL'] - df['Signal_HL'].mean()) / df['Signal_HL'].std()

# 两者作差，形成相对性信号（大于0表示500指数表现更好，小于0表示沪深300表现更好）
df['Signal'] = df['Signal_500'] - df['Signal_HL']

# 计算该信号在历史上的分位数（相对于过去HISTORY_WINDOW个交易日）
df['Signal_Rank'] = df['Signal'].rolling(window=HISTORY_WINDOW).apply(lambda x: percentileofscore(x, x.iloc[-1]) / 100)

# 可视化分位数分布(横轴是日期，纵轴是分位数,叠加500和红利的收盘价和涨跌幅走势)
shift = 0                # 想要提前的周期数
rank_shifted = df['Signal_Rank'].shift(-shift)

fig, ax1 = plt.subplots(figsize=(10, 4))

ax1.plot(df.index, rank_shifted,
         color='tab:blue',
         label=f'Signal Rank (lead {shift})')   # 标明平移
ax1.set_xlabel('Date')
ax1.set_ylabel('Signal Rank', color='tab:blue')
ax1.tick_params(axis='y', labelcolor='tab:blue')

ax2 = ax1.twinx()
ax2.plot(df.index, df['Close_500'],
         color='tab:orange', label='Close_500')
ax2.plot(df.index, df['Close_HL'],
         color='tab:green',  label='Close_HL')
ax2.set_ylabel('Price', color='tab:orange')
ax2.tick_params(axis='y', labelcolor='tab:orange')

lines1, labels1 = ax1.get_legend_handles_labels()
lines2, labels2 = ax2.get_legend_handles_labels()
ax1.legend(lines1 + lines2, labels1 + labels2, loc='best')

ax1.grid(True)
plt.title('Signal Rank Over Time (shifted)')
plt.savefig('results/liq/signal_rank_shifted.png', dpi=300)
plt.show()


# 预览信号构建结果
print("\n🔍 信号构建预览:")
print(df[['Signal_500', 'Signal_HL', 'Signal', 'Signal_Rank']].tail(20))

# ----------------------------------------------------------------------
# 3. 仓位管理，构建目标持仓序列
# ----------------------------------------------------------------------
shift = 1
df['Position'] = 'Hold'  # 初始化仓位状态为持有（不操作）

# 根据提前的因子值调整仓位
df.loc[df['Signal_Rank'].shift(-shift) >= 1, 'Position'] = 'Full_500'
df.loc[df['Signal_Rank'].shift(-shift) <= 0.05, 'Position'] = 'Full_HL'

# 初始仓位设置为50%-50%
df_bt = df.loc[BACKTEST_START_DATE:BACKTEST_END_DATE].copy()
df_bt['Position'].iloc[0] = 'Initial_50_50'

# 计算每日的仓位比例
df_bt['Position_500'] = 0.5  # 默认初始仓位是50%
df_bt['Position_HL'] = 0.5   # 默认初始仓位是50%

# 根据 Position 列生成目标仓位
df_bt.loc[df_bt['Position'] == 'Full_500', 'Target_500'] = 1.0
df_bt.loc[df_bt['Position'] == 'Full_500', 'Target_HL'] = 0.0
df_bt.loc[df_bt['Position'] == 'Full_HL', 'Target_500'] = 0.0
df_bt.loc[df_bt['Position'] == 'Full_HL', 'Target_HL'] = 1.0

# 前向填充，但注意：只在有信号后才生效
# 我们需要的是：从信号日的**下一日**开始应用新仓位

# 方法：创建一个 shift 后的信号列
df_bt['Signal_500'] = df_bt['Target_500'].where(
    (df_bt['Position'] == 'Full_500') | (df_bt['Position'] == 'Full_HL')
)
df_bt['Signal_HL'] = df_bt['Target_HL'].where(
    (df_bt['Position'] == 'Full_500') | (df_bt['Position'] == 'Full_HL')
)

# 将信号向下移动一天（因为信号在当天产生，次日调仓）
df_bt['Signal_500'] = df_bt['Signal_500'].shift(1)
df_bt['Signal_HL'] = df_bt['Signal_HL'].shift(1)

# 前向填充信号
df_bt['Position_500'] = df_bt['Signal_500'].ffill().fillna(0.5)
df_bt['Position_HL'] = df_bt['Signal_HL'].ffill().fillna(0.5)

# 预览仓位调整结果
print("\n🔍 仓位调整预览:")
print(df_bt[['Position', 'Position_500', 'Position_HL']].tail(20))

# ----------------------------------------------------------------------
# 4. 回测执行及绩效计算
# ----------------------------------------------------------------------

# 日收益率（小数）
df_bt['Strategy_Return'] = (df_bt['Position_500'] * df_bt['Ret_Index_500'] +
                            df_bt['Position_HL'] * df_bt['Ret_Index_HL']) / 100
df_bt['Benchmark_Return'] = (0.5 * df_bt['Ret_Index_500'] +
                             0.5 * df_bt['Ret_Index_HL']) / 100

# 累计净值（从 1 开始）
df_bt['Cumulative_Strategy_Return'] = (1 + df_bt['Strategy_Return']).cumprod()
df_bt['Cumulative_Benchmark_Return'] = (1 + df_bt['Benchmark_Return']).cumprod()
df_bt['Excess_Return'] = df_bt['Cumulative_Strategy_Return'] - df_bt['Cumulative_Benchmark_Return']

# 计算最大回撤（max drawdown）
df_bt['Roll_Max'] = df_bt['Cumulative_Strategy_Return'].cummax()
df_bt['Drawdown'] = df_bt['Cumulative_Strategy_Return'] / df_bt['Roll_Max'] - 1
max_dd = df_bt['Drawdown'].min()  # 负值表示回撤幅度

# 打印绩效指标
# 确保已计算净值列（如前文所述）
strategy_ret = df_bt['Cumulative_Strategy_Return'].iloc[-1] - 1
benchmark_ret = df_bt['Cumulative_Benchmark_Return'].iloc[-1] - 1
excess_ret = df_bt['Excess_Return'].iloc[-1] 


print("-" * 50)
print("\n📊 回测绩效摘要:")
print("-" * 50)
print(f"策略总收益率:        {strategy_ret*100:.2f}%")
print(f"基准总收益率:        {benchmark_ret*100:.2f}%")
print(f"相对超额收益:        {excess_ret*100:.2f}%")
print(f"最大回撤: {max_dd*100:.2f}%")
print("-" * 50)
print(f"📅 特定区间统计: 【 {STRATEGY_START_DATE} 至今 】")
print("-" * 50)
try:
    df_spec = df_bt.loc[STRATEGY_START_DATE:]
    if not df_spec.empty:
        s_ret = df_spec['Cumulative_Strategy_Return'].iloc[-1] / df_spec['Cumulative_Strategy_Return'].iloc[0] - 1
        b_ret = df_spec['Cumulative_Benchmark_Return'].iloc[-1] / df_spec['Cumulative_Benchmark_Return'].iloc[0] - 1
        excess_spec = s_ret - b_ret
        print(f"   🔹 策略区间收益: {s_ret:.2%}")
        print(f"   🔹 基准区间收益: {b_ret:.2%}")
        print(f"   🔥 区间超额收益: {excess_spec:.2%}")
    else:
        print(f"   ⚠️ 数据未覆盖到 {STRATEGY_START_DATE}")
except Exception as e:
    print(f"   ⚠️ 统计计算错误: {e}")

# 绘图
fig, axs = plt.subplots(3, figsize=(10, 8))

axs[0].plot(df_bt.index, df_bt['Cumulative_Strategy_Return'], label='策略累计收益')
axs[0].plot(df_bt.index, df_bt['Cumulative_Benchmark_Return'], label='基准累计收益')
axs[0].set_title('策略累计收益 vs 基准累计收益')
axs[0].legend()

axs[1].plot(df_bt.index, df_bt['Excess_Return'], color='green', label='累计超额收益')
axs[1].set_title('累计超额收益')
axs[1].legend()

axs[2].plot(df_bt.index, df_bt['Position_500'], label='500仓位')
axs[2].plot(df_bt.index, df_bt['Position_HL'], label='红利仓位')
axs[2].set_title('仓位分布')
axs[2].legend()

plt.tight_layout()
plt.savefig('results/liq/backtest_results.png', dpi=300)
plt.show()

