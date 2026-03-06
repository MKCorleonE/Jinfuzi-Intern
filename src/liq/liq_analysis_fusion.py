import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import percentileofscore
import os
import sys

# 设置编码
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ======================================================================
# 流动性因子 + MPC因子 融合策略
# 两个因子独立轨道运行，加权组合，具备阈值调仓限制
# ======================================================================

# -----------------------------------------------
# 0. 全局设置
# -----------------------------------------------
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# 文件读取路径
FILE_PATH = './data/realtime_data_updated.csv'

# 回测时间段
BACKTEST_START_DATE = '2023-06-17'
BACKTEST_END_DATE = '2099-12-31'
STRATEGY_START_DATE = '2026-01-25'

# ✅ 因子1: 流动性冲击因子参数
LIQ_WINDOW_SIZE = 40          # 计算信号的滚动窗口
LIQ_HISTORY_WINDOW = 20       # 历史分位数窗口
LIQ_SIGMOID_SCALE = 8         # Sigmoid曲线的陡峭程度

# ✅ 因子2: MPC因子参数
MPC_K_PERIOD = 1              # MPC计算周期
MPC_SMOOTH_WINDOW = 2         # 平滑窗口
MPC_LOOKBACK_DAYS = 20        # Z-score标准化窗口
MPC_MA5_WINDOW = 5            # 趋势均线窗口

# ✅ 融合参数
FACTOR_WEIGHTS = {
    'liq': 1,               # 流动性因子权重
    'mpc': 0                # MPC因子权重
}

# ✅ 调仓限制
ADJUST_THRESHOLD = 0.03       # 仓位变化超过3%才调仓
POSITION_BUFFER = 0.02        # 仓位缓冲

# 交易成本与滑点
COST = 0.0002
SLIPPAGE = 0.0003

print("=" * 70)
print("融合因子策略 - 流动性冲击因子 + MPC因子")
print("=" * 70)
print(f"[因子权重] 流动性: {FACTOR_WEIGHTS['liq']:.1%}, MPC: {FACTOR_WEIGHTS['mpc']:.1%}")
print(f"[调仓限制] 阈值: {ADJUST_THRESHOLD:.2%}, 缓冲: {POSITION_BUFFER:.2%}")
print("=" * 70)

# -----------------------------------------------
# 1. 数据加载与预处理
# -----------------------------------------------
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

# 重命名列
df.rename(columns={'turnover_volume1': 'TV_500', 'turnover_volume2': 'TV_HL'}, inplace=True)
df.rename(columns={'change_pct1': 'Ret_Index_500', 'change_pct2': 'Ret_Index_HL'}, inplace=True)
df.rename(columns={'close_price1': 'Close_500', 'close_price2': 'Close_HL'}, inplace=True)
df.rename(columns={'change_pct4': 'Ret_Fund_500', 'change_pct5': 'Ret_Fund_HL'}, inplace=True)

# ======================================================================
# TRACK 1: 流动性冲击因子 (LIQ)
# ======================================================================
print("\n📍 TRACK 1: 流动性冲击因子计算中...")

# 计算初始信号
df['LIQ_Signal_500'] = (df['Ret_Index_500'].rolling(window=LIQ_WINDOW_SIZE).mean() / 
                        df['TV_500'].rolling(window=LIQ_WINDOW_SIZE).mean())
df['LIQ_Signal_HL'] = (df['Ret_Index_HL'].rolling(window=LIQ_WINDOW_SIZE).mean() / 
                       df['TV_HL'].rolling(window=LIQ_WINDOW_SIZE).mean())

# 滚动窗口标准化
df['LIQ_Signal_500'] = ((df['LIQ_Signal_500'] - df['LIQ_Signal_500'].rolling(LIQ_WINDOW_SIZE*2).mean()) / 
                        (df['LIQ_Signal_500'].rolling(LIQ_WINDOW_SIZE*2).std() + 1e-10))
df['LIQ_Signal_HL'] = ((df['LIQ_Signal_HL'] - df['LIQ_Signal_HL'].rolling(LIQ_WINDOW_SIZE*2).mean()) / 
                       (df['LIQ_Signal_HL'].rolling(LIQ_WINDOW_SIZE*2).std() + 1e-10))

# 相对性信号
df['LIQ_Signal'] = df['LIQ_Signal_500'] - df['LIQ_Signal_HL']

# 分位数
df['LIQ_Signal_Rank'] = df['LIQ_Signal'].rolling(window=LIQ_HISTORY_WINDOW).apply(
    lambda x: percentileofscore(x, x.iloc[-1]) / 100 if len(x) > 0 else 0.5)

# Sigmoid函数将分位数转换为持仓仓位
def sigmoid(x, scale=LIQ_SIGMOID_SCALE):
    return 1 / (1 + np.exp(-scale * x))

# 流动性因子的目标仓位 (500仓位)
df['LIQ_Target_500'] = sigmoid(df['LIQ_Signal_Rank'] - 0.5, scale=LIQ_SIGMOID_SCALE)
df['LIQ_Target_HL'] = 1 - df['LIQ_Target_500']

print("✅ 流动性因子完成")

# ======================================================================
# TRACK 2: MPC因子 (MPC)
# ======================================================================
print("📍 TRACK 2: MPC因子计算中...")

# 计算中间价
df['Mid_Price_500'] = (df['Close_500'] + df['Close_500']) / 2  # 简化处理
df['Mid_Price_HL'] = (df['Close_HL'] + df['Close_HL']) / 2

# MPC原始信号
df['MPC_500_raw'] = df['Mid_Price_500'].pct_change(MPC_K_PERIOD).fillna(0)
df['MPC_HL_raw'] = df['Mid_Price_HL'].pct_change(MPC_K_PERIOD).fillna(0)

# 平滑
df['MPC_500'] = df['MPC_500_raw'].rolling(MPC_SMOOTH_WINDOW, min_periods=1).mean()
df['MPC_HL'] = df['MPC_HL_raw'].rolling(MPC_SMOOTH_WINDOW, min_periods=1).mean()

# Z-score标准化
def rolling_zscore(series, window):
    mean = series.rolling(window, min_periods=1).mean()
    std = series.rolling(window, min_periods=1).std().replace(0, 1e-8)
    return ((series - mean) / std).fillna(0)

df['MPC_500_z'] = rolling_zscore(df['MPC_500'], MPC_LOOKBACK_DAYS)
df['MPC_HL_z'] = rolling_zscore(df['MPC_HL'], MPC_LOOKBACK_DAYS)

# 因子差异
df['MPC_Factor_Diff'] = df['MPC_500_z'] - df['MPC_HL_z']

# 分位数权重计算 (直接使用分位数而非复杂的百分位数逻辑)
def calc_mpc_position(factor_diff_series, lookback=MPC_LOOKBACK_DAYS):
    """计算MPC因子对应的目标仓位"""
    positions = []
    for i in range(len(factor_diff_series)):
        if i < lookback:
            positions.append(0.5)
            continue
        window = factor_diff_series.iloc[max(0, i-lookback):i+1].values
        if len(window) < 2:
            positions.append(0.5)
            continue
        sorted_vals = np.sort(window)
        rank = np.searchsorted(sorted_vals, factor_diff_series.iloc[i], side='right')
        percentile = rank / len(sorted_vals)
        # 因子越大，500仓位越小
        target = 1.0 - percentile
        positions.append(target)
    return positions

df['MPC_Target_500'] = calc_mpc_position(df['MPC_Factor_Diff'])
df['MPC_Target_HL'] = 1 - df['MPC_Target_500']

# 趋势过滤 (5日均线)
df['MA5_HL'] = df['Close_HL'].rolling(MPC_MA5_WINDOW, min_periods=1).mean()
df['Trend_Up'] = df['Close_HL'] > df['MA5_HL']

# 趋势过滤下的MPC仓位
df['MPC_Target_500_filtered'] = df['MPC_Target_500'].copy()
df['MPC_Target_HL_filtered'] = df['MPC_Target_HL'].copy()
# 趋势向下时降仓
mask_down = ~df['Trend_Up']
df.loc[mask_down, 'MPC_Target_500_filtered'] = 0.7 * 0.6
df.loc[mask_down, 'MPC_Target_HL_filtered'] = 0.3 * 0.6

print("✅ MPC因子完成")

# ======================================================================
# TRACK 3: 因子融合 (FUSION)
# ======================================================================
print("📍 TRACK 3: 因子融合计算中...")

# 加权组合目标仓位
w_liq = FACTOR_WEIGHTS['liq']
w_mpc = FACTOR_WEIGHTS['mpc']

df['Fusion_Target_500'] = (w_liq * df['LIQ_Target_500'] + 
                           w_mpc * df['MPC_Target_500_filtered'])
df['Fusion_Target_HL'] = (w_liq * df['LIQ_Target_HL'] + 
                          w_mpc * df['MPC_Target_HL_filtered'])

print("✅ 因子融合完成")

# ======================================================================
# TRACK 4: 仓位管理与调仓限制
# ======================================================================
print("📍 TRACK 4: 阈值调仓限制计算中...")

# 执行仓位管理逻辑：当前日的持仓是前一日的目标仓位
df['Fusion_Position_500'] = df['Fusion_Target_500'].shift(1).fillna(0.5)
df['Fusion_Position_HL'] = df['Fusion_Target_HL'].shift(1).fillna(0.5)

# 计算仓位变化
df['Position_Change'] = abs(df['Fusion_Position_500'] - df['Fusion_Position_500'].shift(1)).fillna(0)

# 阈值调仓标志：仅当仓位变化超过阈值时标记为调仓
df['Rebalance_Flag'] = df['Position_Change'] > ADJUST_THRESHOLD

# 实际执行仓位（考虑缓冲）
current_position = 0.5
actual_positions_500 = []
actual_positions_hl = []
adjustment_count = 0

for i in range(len(df)):
    target = df['Fusion_Position_500'].iloc[i]
    
    if abs(target - current_position) > ADJUST_THRESHOLD:
        # 触发调仓：逐步调向目标
        if target > current_position:
            current_position = min(current_position + (abs(target - current_position) - POSITION_BUFFER), target)
        else:
            current_position = max(current_position - (abs(target - current_position) - POSITION_BUFFER), target)
        adjustment_count += 1
    
    actual_positions_500.append(current_position)
    actual_positions_hl.append(1 - current_position)

df['Actual_Position_500'] = actual_positions_500
df['Actual_Position_HL'] = actual_positions_hl

print("✅ 阈值调仓限制完成")
print(f"✅ 调仓天数: {adjustment_count}天 ({adjustment_count/len(df):.2%})")

# ======================================================================
# 5. 回测执行及绩效计算
# ======================================================================
print("\n📍 TRACK 5: 回测执行中...")

# 过滤数据到回测区间
df_bt = df.loc[BACKTEST_START_DATE:BACKTEST_END_DATE].copy()

# 日收益率（小数）
df_bt['Strategy_Return'] = (df_bt['Actual_Position_500'] * df_bt['Ret_Fund_500'] +
                            df_bt['Actual_Position_HL'] * df_bt['Ret_Fund_HL']) / 100
df_bt['Benchmark_Return'] = (0.5 * df_bt['Ret_Fund_500'] +
                             0.5 * df_bt['Ret_Fund_HL']) / 100

# 累计净值
df_bt['Cumulative_Strategy_Return'] = (1 + df_bt['Strategy_Return']).cumprod()
df_bt['Cumulative_Benchmark_Return'] = (1 + df_bt['Benchmark_Return']).cumprod()
df_bt['Excess_Return'] = df_bt['Cumulative_Strategy_Return'] - df_bt['Cumulative_Benchmark_Return']

# 最大回撤
df_bt['Roll_Max'] = df_bt['Cumulative_Strategy_Return'].cummax()
df_bt['Drawdown'] = df_bt['Cumulative_Strategy_Return'] / df_bt['Roll_Max'] - 1
max_dd = df_bt['Drawdown'].min()

# 绩效指标
strategy_ret = df_bt['Cumulative_Strategy_Return'].iloc[-1] - 1
benchmark_ret = df_bt['Cumulative_Benchmark_Return'].iloc[-1] - 1
excess_ret = df_bt['Excess_Return'].iloc[-1]

num_years = (df_bt.index[-1] - df_bt.index[0]).days / 252
annualized_strategy_ret = (1 + strategy_ret) ** (1 / num_years) - 1
annualized_benchmark_ret = (1 + benchmark_ret) ** (1 / num_years) - 1
annualized_excess_ret = annualized_strategy_ret - annualized_benchmark_ret

# ======================================================================
# 6. 结果输出
# ======================================================================
print("\n" + "=" * 70)
print("📊 融合因子策略 - 回测绩效摘要")
print("=" * 70)
print(f"💰 总收益率:")
print(f"   策略总收益率:        {strategy_ret*100:.2f}%")
print(f"   基准总收益率:        {benchmark_ret*100:.2f}%")
print(f"   相对超额收益:        {excess_ret*100:.2f}%")
print(f"\n📈 年化收益率 (年数: {num_years:.2f}):")
print(f"   年化策略收益率:      {annualized_strategy_ret*100:.2f}%")
print(f"   年化基准收益率:      {annualized_benchmark_ret*100:.2f}%")
print(f"   年化超额收益率:      {annualized_excess_ret*100:.2f}%")
print(f"\n⚠️  风险指标:")
print(f"   最大回撤:            {max_dd*100:.2f}%")
print(f"\n🔄 调仓统计:")
print(f"   总调仓天数:          {adjustment_count}天 ({adjustment_count/len(df_bt):.2%})")
print(f"   平均持仓500权重:     {df_bt['Actual_Position_500'].mean():.2%}")
print("=" * 70)

# 特定区间统计
print(f"\n📅 特定区间统计: 【 {STRATEGY_START_DATE} 至今 】")
print("-" * 70)
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

# ======================================================================
# 7. 可视化
# ======================================================================
print("\n📊 生成可视化图表...")

fig, axs = plt.subplots(4, 2, figsize=(16, 12))

# 第1行：两个因子的目标仓位
axs[0, 0].plot(df_bt.index, df_bt['LIQ_Target_500'], label='LIQ-500', alpha=0.7, linewidth=1.5)
axs[0, 0].set_title('因子1 (LIQ): 流动性冲击因子目标仓位')
axs[0, 0].set_ylabel('500仓位权重')
axs[0, 0].legend()
axs[0, 0].grid(True, alpha=0.3)

axs[0, 1].plot(df_bt.index, df_bt['MPC_Target_500_filtered'], label='MPC-500', alpha=0.7, linewidth=1.5, color='orange')
axs[0, 1].set_title('因子2 (MPC): MPC因子目标仓位 (趋势过滤后)')
axs[0, 1].set_ylabel('500仓位权重')
axs[0, 1].legend()
axs[0, 1].grid(True, alpha=0.3)

# 第2行：融合因子
axs[1, 0].plot(df_bt.index, df_bt['Fusion_Target_500'], label='融合目标', alpha=0.7, linewidth=1.5, color='green')
axs[1, 0].plot(df_bt.index, df_bt['Actual_Position_500'], label='实际仓位 (阈值限制)', alpha=0.9, linewidth=1, color='darkgreen', linestyle='--')
axs[1, 0].set_title('融合因子: 目标仓位 vs 实际仓位')
axs[1, 0].set_ylabel('500仓位权重')
axs[1, 0].legend()
axs[1, 0].grid(True, alpha=0.3)

# 仓位变化
axs[1, 1].plot(df_bt.index, df_bt['Position_Change'] * 100, color='red', alpha=0.6, linewidth=1)
axs[1, 1].axhline(y=ADJUST_THRESHOLD*100, color='orange', linestyle='--', label=f'调仓阈值: {ADJUST_THRESHOLD:.1%}')
axs[1, 1].set_title('日仓位变化幅度与调仓阈值')
axs[1, 1].set_ylabel('变化幅度 (%)')
axs[1, 1].legend()
axs[1, 1].grid(True, alpha=0.3)

# 第3行：净值对比
axs[2, 0].plot(df_bt.index, df_bt['Cumulative_Strategy_Return'], label='策略累计收益', linewidth=2)
axs[2, 0].plot(df_bt.index, df_bt['Cumulative_Benchmark_Return'], label='基准累计收益 (50/50)', linewidth=2, linestyle='--', alpha=0.7)
axs[2, 0].set_title('策略累计收益 vs 基准累计收益')
axs[2, 0].set_ylabel('累计收益倍数')
axs[2, 0].legend()
axs[2, 0].grid(True, alpha=0.3)

# 超额收益
axs[2, 1].plot(df_bt.index, df_bt['Excess_Return'], color='green', linewidth=2, label='累计超额收益')
axs[2, 1].axhline(y=0, color='gray', linestyle=':', alpha=0.5)
axs[2, 1].set_title('累计超额收益')
axs[2, 1].set_ylabel('超额收益倍数')
axs[2, 1].legend()
axs[2, 1].grid(True, alpha=0.3)

# 第4行：回撤与信号
axs[3, 0].fill_between(df_bt.index, df_bt['Drawdown']*100, 0, alpha=0.3, color='red')
axs[3, 0].plot(df_bt.index, df_bt['Drawdown']*100, color='darkred', linewidth=1)
axs[3, 0].set_title('策略最大回撤')
axs[3, 0].set_ylabel('回撤 (%)')
axs[3, 0].grid(True, alpha=0.3)

# 因子信号对比
axs[3, 1].plot(df_bt.index, df_bt['LIQ_Signal_Rank'], label='LIQ信号分位数', alpha=0.7, linewidth=1)
ax_mpc = axs[3, 1].twinx()
ax_mpc.plot(df_bt.index, df_bt['MPC_Factor_Diff'], label='MPC因子差异', alpha=0.7, linewidth=1, color='orange')
axs[3, 1].set_title('独立因子信号对比')
axs[3, 1].set_ylabel('LIQ分位数', color='tab:blue')
ax_mpc.set_ylabel('MPC差异', color='orange')
axs[3, 1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig('results/liq/fusion_backtest_results.png', dpi=300, bbox_inches='tight')
print("✅ 图表已保存到 results/liq/fusion_backtest_results.png")
plt.show()

# ======================================================================
# 8. 保存详细数据
# ======================================================================
print("\n💾 保存详细数据...")

# 保存到CSV
output_df = pd.DataFrame({
    'Date': df_bt.index,
    'LIQ_Signal_Rank': df_bt['LIQ_Signal_Rank'],
    'LIQ_Target_500': df_bt['LIQ_Target_500'],
    'MPC_Factor_Diff': df_bt['MPC_Factor_Diff'],
    'MPC_Target_500': df_bt['MPC_Target_500_filtered'],
    'Fusion_Target_500': df_bt['Fusion_Target_500'],
    'Actual_Position_500': df_bt['Actual_Position_500'],
    'Position_Change': df_bt['Position_Change'],
    'Rebalance_Flag': df_bt['Rebalance_Flag'],
    'Strategy_Return': df_bt['Strategy_Return'],
    'Benchmark_Return': df_bt['Benchmark_Return'],
    'Cumulative_Strategy': df_bt['Cumulative_Strategy_Return'],
    'Cumulative_Benchmark': df_bt['Cumulative_Benchmark_Return'],
    'Excess_Return': df_bt['Excess_Return'],
    'Drawdown': df_bt['Drawdown']
})

output_df.to_csv('results/liq/fusion_detailed_results.csv', index=False)
print("✅ 详细数据已保存到 results/liq/fusion_detailed_results.csv")

print("\n" + "=" * 70)
print("✨ 融合因子回测完成！")
print("=" * 70)
