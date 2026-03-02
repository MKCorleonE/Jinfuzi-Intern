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
# 2. 改进的非流动性因子 ILLIQ2 构建（参考 Amihud 2020 + Lou & Shu 2017）
# ----------------------------------------------------------------------

def compute_illiq2(series_ret, series_vol, window=40):
    """
    计算改进型非流动性因子 ILLIQ2
    输入：
        series_ret: 日收益率序列（小数）
        series_vol: 日成交量序列（单位：手或金额）
        window: 滚动窗口
    输出：
        ILLIQ2: 改进的非流动性因子
    """
    # 第一步：计算 |r| / vol（即 LSILLiq）
    lsilliq = np.abs(series_ret) / series_vol

    # 第二步：计算 b（|r| 对 vol 的回归斜率）
    # 注意：我们使用滚动窗口进行回归
    b = []
    for i in range(window - 1, len(series_ret)):
        x = series_vol.iloc[i - window + 1:i]
        y = np.abs(series_ret.iloc[i - window + 1:i])
        if len(x) < 2 or x.std() == 0:
            b.append(0)
            continue
        # 简单线性回归斜率
        slope = np.cov(y, x)[0, 1] / x.var()
        b.append(slope)
    b = np.array(b)
    b = np.pad(b, (window - 1, 0), mode='constant')  # 填充前面

    # 第三步：计算 CV^2（变异系数平方）
    cv2 = []
    for i in range(window - 1, len(series_vol)):
        x = series_vol.iloc[i - window + 1:i]
        if x.std() == 0:
            cv2.append(0)
        else:
            cv2.append((x.std() / x.mean()) ** 2)
    cv2 = np.array(cv2)
    cv2 = np.pad(cv2, (window - 1, 0), mode='constant')

    # 第四步：ILLIQ2 = LSILLiq - b * CV^2
    illiq2 = lsilliq - b * cv2

    return illiq2

# 分别计算两个指数的 ILLIQ2 因子
df['ILLIQ2_500'] = compute_illiq2(df['Ret_Index_500'], df['TV_500'], window=WINDOW_SIZE)
df['ILLIQ2_HL'] = compute_illiq2(df['Ret_Index_HL'], df['TV_HL'], window=WINDOW_SIZE)

# 可视化 ILLIQ2 因子
fig, ax = plt.subplots(figsize=(10, 4))
ax.plot(df.index, df['ILLIQ2_500'], label='ILLIQ2_500', alpha=0.8)
ax.plot(df.index, df['ILLIQ2_HL'], label='ILLIQ2_HL', alpha=0.8)
ax.set_title('Improved Illiquidity Factor (ILLIQ2)')
ax.legend()
ax.grid(True)
plt.savefig('results/liq/illiq2_factors.png', dpi=300)
plt.show()

# ----------------------------------------------------------------------
# 3. 构建相对信号（500 vs HL）→ 标准化 → 分位数
# ----------------------------------------------------------------------

# 相对信号：500 比 HL 更“不流动”时为正
df['Signal'] = df['ILLIQ2_500'] - df['ILLIQ2_HL']

# 标准化（Z-score）
df['Signal'] = (df['Signal'] - df['Signal'].rolling(WINDOW_SIZE*2).mean()) / (df['Signal'].rolling(WINDOW_SIZE*2).std() + 1e-10)

# 计算历史分位数（过去 HISTORY_WINDOW 天）
df['Signal_Rank'] = df['Signal'].rolling(window=HISTORY_WINDOW).apply(
    lambda x: percentileofscore(x, x.iloc[-1]) / 100
)

# 可视化信号分位数
shift = 0
rank_shifted = df['Signal_Rank'].shift(-shift)

fig, ax1 = plt.subplots(figsize=(10, 4))

ax1.plot(df.index, rank_shifted,
         color='tab:blue',
         label=f'Signal Rank (lead {shift})')
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
plt.savefig('results/liq/signal_rank_shifted_illiq2.png', dpi=300)
plt.show()

print("\n🔍 信号构建预览:")
print(df[['ILLIQ2_500', 'ILLIQ2_HL', 'Signal', 'Signal_Rank']].tail(20))

# ----------------------------------------------------------------------
# 3. 仓位管理，构建目标持仓序列（Sigmoid调仓）
# ----------------------------------------------------------------------
# Sigmoid函数参数
SIGMOID_SCALE = 8  # 控制Sigmoid曲线的陡峭程度，值越大曲线越陡

# 定义Sigmoid函数
def sigmoid(x, scale=SIGMOID_SCALE):
    """
    Sigmoid函数：将信号转换为仓位比例
    sigmoid(0) = 0.5 (对称点)
    sigmoid范围 [0, 1]
    """
    return 1 / (1 + np.exp(-scale * x))

shift = 0

# 初始仓位设置为50%-50%
df_bt = df.loc[BACKTEST_START_DATE:BACKTEST_END_DATE].copy()

# 使用Sigmoid函数将Signal_Rank转换为目标仓位
# Signal_Rank: [0, 1] -> 中心点0.5，转换为 [-0.5, 0.5] -> sigmoid -> [~0.0067, ~0.993]
# 这样实现平滑的仓位调整
df_bt['Signal_Rank_Shifted'] = df_bt['Signal_Rank'].shift(-shift)  # 提前一个周期信号
df_bt['Target_500'] = sigmoid(df_bt['Signal_Rank_Shifted'] - 0.5, scale=SIGMOID_SCALE)
df_bt['Target_HL'] = 1 - df_bt['Target_500']

# 计算实际仓位：当前天持有的是前一天的目标仓位
df_bt['Position_500'] = df_bt['Target_500'].shift(1).fillna(0.5)
df_bt['Position_HL'] = df_bt['Target_HL'].shift(1).fillna(0.5)

# 预览仓位调整结果
print("\n🔍 仓位调整预览:")
print(df_bt[['Target_500', 'Target_HL', 'Position_500', 'Position_HL']].tail(20))

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
plt.savefig('results/liq/backtest_results_sigmoid.png', dpi=300)
plt.show()

