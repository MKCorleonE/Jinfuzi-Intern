import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import percentileofscore
import os

"""
Last Edit Date: 2026-01-30
Author: Jiawen Liang
Project: Two-factor independent track real trading strategy
"""

# ===================== 0. 全局设定 =====================

# 字体设定
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# 文件读取路径
FILE_PATH = './data/realtime_data/merged_index_fund_returns.csv'

# 回测时间区间
BACKTEST_START_DATE = '2023-01-01'
BACKTEST_END_DATE = '2099-12-31'

# 特定日期记录
SPECIFIC_STAT_DATE = '2025-12-24'

# RTVR 策略参数
RTVR_WINDOW = 40 # 40-day average smoothing
RTVR_LOOKBACK = 66 # Calculate the historical percentile of the current value over the past 66 days
RTVR_THRESHOLDS = {'H': 0.70, 'L': 0.30, 'FH': 0.90, 'FL': 0.10, 'MH': 0.60, 'ML': 0.40} # Threshold

# TSM 策略参数
TSM_MIN_STEP = 0.01
TSM_SENSITIVITY = 30

# 交易成本与滑点
COST = 0.0002  # 佣金/印花税等固定成本 (万二)
SLIPPAGE = 0.0003  # 滑点 (万三)：模拟大额订单偏离VWAP的冲击成本

# ===================== 1. 数据加载与预处理（此部分暂无额外预处理） =====================

# Data loading
if not os.path.exists(FILE_PATH):
    print(f"❌ 错误：找不到文件 {FILE_PATH}")
    exit()
try:
    df = pd.read_csv(FILE_PATH, parse_dates=['TradingDay'])
    print(f"✅ 成功加载数据: {len(df)} 条记录")
except Exception as e:
    print(f"❌ 无法读取文件: {e}")
    exit()

# 按照交易日排序
df = df.set_index('TradingDay').sort_index()

#预览已加载的数据
print("数据预览：")
print(df.head())

# ===================== 2. 区分【信号源数据】和【标的数据】 =====================

# 1. 信号源数据：来自指数数据，构建一个新的df，只包含指数数据
signal_df = df[[col for col in df.columns if 'idx' in col]]

# 将0值视为缺失值 (避免除以0错误)
signal_df.replace(0, np.nan, inplace=True)

# 2. 标的数据：来自基金数据，构建一个新的df，只包含基金数据
target_df = df[[col for col in df.columns if 'fund' in col]]

# 将0值视为缺失值 (避免除以0错误)
target_df.replace(0, np.nan, inplace=True)

# 计算标的日涨跌幅
df['Ret_ETF_500'] = target_df['fund_512510__prev_close'].pct_change().fillna(0)
df['Ret_ETF_HL'] = target_df['fund_515180__prev_close'].pct_change().fillna(0)

# ===================== 3. 因子原始数据准备 =====================

# RTVR 数据准备, 中证500交易额 / 中证500交易额 + 红利交易额
df['RTVR_raw'] = signal_df['idx_000905_SH__turnover_value'] / (signal_df['idx_000905_SH__turnover_value'] + signal_df['idx_000922_SH__turnover_value'])

# 计算滑动平均值
df['RTVR_factor'] = df['RTVR_raw'].rolling(window=RTVR_WINDOW, min_periods=1).mean()

# 计算当前值在过去66天中的分位数
df['RTVR_rank'] = df['RTVR_factor'].rolling(window=RTVR_LOOKBACK, min_periods=1).apply(
    lambda x: percentileofscore(x[:-1], x.iloc[-1]) / 100 if len(x) == RTVR_LOOKBACK else np.nan, raw=False
)

# TSM 数据准备, 使用指数数据, 000905和000922
def compute_tsm_factor(idx_num):
    high = signal_df[f'idx_{idx_num}_SH__high_price']
    low = signal_df[f'idx_{idx_num}_SH__low_price']
    close = signal_df[f'idx_{idx_num}_SH__prev_close']
    open = signal_df[f'idx_{idx_num}_SH__open_price']
    range = (high - low).replace(0, np.nan)
    t1 = ((high - close) / range).fillna(0).rolling(69).mean()
    t2 = ((high - open) / range).fillna(0).rolling(3).mean()
    return 0.5 * t1 + 0.5 * t2

# 计算TSM因子：
for idx in ['000905', '000922']:
    df[f'TSM_factor_{idx}'] = compute_tsm_factor(idx)

df['TSM_rel'] = (df['TSM_factor_000905'] - df['TSM_factor_000922']).ewm(span=TSM_SENSITIVITY, adjust=False).mean()
df['TSM_slope_abs'] = df['TSM_rel'].diff().abs().fillna(0)

# ===================== 4. 信号生成 =====================

# 1. RTVR 信号计算函数
def generate_signal_rtvr(P):
    if pd.isna(P): return 0.5
    if P > 0.90: return 0.0 # 中证500太拥挤了，直接空仓中证500
    if 0.70 < P <= 0.90: return 0.5 - ((P - 0.70) / 0.20) * 0.5
    if P < 0.10: return 1.0 # 红利太拥挤了，直接空仓红利
    if 0.10 <= P < 0.30: return 0.5 + ((0.30 - P) / 0.20) * 0.5
    return np.nan

# 信号初始化
df['RTVR_target'] = 0.5
rtvr_w = 0.5

# 开始生成RTVR信号
for i in range(3, len(df)):
    P = df['RTVR_rank'].iloc[i]
    if 0.40 <= P <= 0.60:
        rtvr_w = 0.5
    elif P > 0.70 or P < 0.30:
        p_cur, p_prev, p_prev2 = df['RTVR_rank'].iloc[i: i - 3: -1] # 取值：当前日，前一日，前两日
        is_trend = (p_cur > p_prev > p_prev2) if P > 0.7 else (p_cur < p_prev < p_prev2) # 判断是否有趋势
        if is_trend:
            calc_w = generate_signal_rtvr(P)
            if not pd.isna(calc_w):
                rtvr_w = min(rtvr_w, calc_w) if P > 0.7 else max(rtvr_w, calc_w)
    df.iloc[i, df.columns.get_loc('RTVR_target')] = rtvr_w

# 预览RTVR信号
print("RTVR信号预览：")
print(df[['RTVR_factor', 'RTVR_rank', 'RTVR_target']].dropna().head(10))

# 2. TSM 信号计算函数
df['TSM_target'] = 0.5
tsm_w = 0.5

# 开始生成TSM信号

# 计算斜率符号
slope_signs = np.sign(df['TSM_rel'].diff()).fillna(0).values
tsm_vals = df['TSM_rel'].values

for i in range(3, len(df)):
    val = tsm_vals[i]
    slopes = slope_signs[i - 2:i + 1]  # 取最近3天 (i-2, i-1, i)

    # 逻辑: 只有连续3天斜率一致才触发
    if val > 0.04 and np.all(slopes == 1):
        tsm_w = 1.0
    elif val < -0.04 and np.all(slopes == -1):
        tsm_w = 0.0
    elif (val > 0.04 and np.all(slopes == -1)) or (val < -0.04 and np.all(slopes == 1)):
        tsm_w = 0.5
    # 若不满足上述任何条件，保持上一次的 tsm_w 不变

    df.iloc[i, df.columns.get_loc('TSM_target')] = tsm_w

# 预览TSM信号
print("TSM信号预览：")
print(df[['TSM_rel', 'TSM_slope_abs', 'TSM_target']].dropna().head(10))

# ===================== 5. 双因子独立轨道执行 =====================

# 根据回看窗口，确定有效数据起始点
start_idx = max(RTVR_LOOKBACK, 90)
df_valid = df.iloc[start_idx:].copy()

# 信号滞后 (T日收盘信号 -> T+1日执行)
df_valid['RTVR_target_exec'] = df_valid['RTVR_target'].shift(1)
df_valid['TSM_target_exec'] = df_valid['TSM_target'].shift(1)
df_valid['TSM_slope_abs_exec'] = df_valid['TSM_slope_abs'].shift(1)

# 确定回测时间段
try:
    df_bt = df_valid.loc[BACKTEST_START_DATE:BACKTEST_END_DATE].copy()
    if df_bt.empty: raise ValueError("Selected date range is empty")
    print(f"✅ 已筛选回测区间: {df_bt.index[0].date()} 至 {df_bt.index[-1].date()}")
except Exception as e:
    print(f"⚠️ 日期筛选异常，使用全部数据: {e}")
    df_bt = df_valid.copy()

# 准备数据数组

# 提取涨跌幅数据
ret_500 = df_bt['Ret_ETF_500'].values
ret_hl = df_bt['Ret_ETF_HL'].values

rtvr_target_exec = df_bt['RTVR_target_exec'].fillna(0.5).values
tsm_target_exec = df_bt['TSM_target_exec'].fillna(0.5).values
tsm_slope_abs_exec = df_bt['TSM_slope_abs_exec'].fillna(0).values

# 1. RTVR 因子独立轨道

# 仓位初始化
w_actual_rtvr = np.zeros(len(df_bt))
w_close_rtvr = rtvr_target_exec[0]

for i in range(len(df_bt)):
    w_curr = w_close_rtvr
    tgt = rtvr_target_exec[i]

    if abs(w_curr - tgt) > 0.00001:
        w_curr = tgt

    w_actual_rtvr[i] = w_curr

    # 漂移计算
    r_day = w_curr * ret_500[i] + (1 - w_curr) * ret_hl[i]
    w_close_rtvr = w_curr * (1 + ret_500[i]) / (1 + r_day)
    w_close_rtvr = np.clip(w_close_rtvr, 0.0, 1.0)

# 2. TSM 因子独立轨道

# 仓位初始化
w_actual_tsm = np.zeros(len(df_bt))
w_close_tsm = tsm_target_exec[0]

for i in range(len(df_bt)):
    w_curr = w_close_tsm
    tgt = tsm_target_exec[i]
    slope = tsm_slope_abs_exec[i]

    step = 1.0 if abs(tgt - 0.5) < 1e-5 else min(1.0, TSM_MIN_STEP + slope * TSM_SENSITIVITY)

    if w_curr < tgt:
        w_curr = min(w_curr + step, tgt)
    elif w_curr > tgt:
        w_curr = max(w_curr - step, tgt)

    w_actual_tsm[i] = w_curr

    # 漂移计算
    r_day = w_curr * ret_500[i] + (1 - w_curr) * ret_hl[i]
    w_close_tsm = w_curr * (1 + ret_500[i]) / (1 + r_day)
    w_close_tsm = np.clip(w_close_tsm, 0.0, 1.0)

# ===================== 6. 策略组合与绩效评估（FoF模式，VWAP收益率计算） =====================

df_bt['W_Actual_RTVR'] = w_actual_rtvr
df_bt['W_Actual_TSM'] = w_actual_tsm
df_bt['W_500_Final'] = 0.5 * df_bt['W_Actual_RTVR'] + 0.5 * df_bt['W_Actual_TSM']
df_bt['W_HL_Final'] = 1.0 - df_bt['W_500_Final']

# 计算单边换手率
init_w = df_bt['W_500_Final'].iloc[0]
df_bt['Turnover'] = (df_bt['W_500_Final'] - df_bt['W_500_Final'].shift(1).fillna(init_w)).abs()


# === 策略收益计算 (VWAP) ===
def calc_vwap_contrib(w_curr, w_prev, close, prev, vwap):
    delta = w_curr - w_prev
    ret_hold = np.minimum(w_curr, w_prev) * (close / prev - 1)
    ret_buy = delta.clip(lower=0) * (close / vwap - 1)
    ret_sell = delta.clip(upper=0).abs() * (vwap / prev - 1)
    return ret_hold + ret_buy + ret_sell


# 获取上一期权重
w_500_prev = df_bt['W_500_Final'].shift(1).fillna(init_w)
w_hl_prev = df_bt['W_HL_Final'].shift(1).fillna(1.0 - init_w)

# 更改表头名称


contrib_500 = calc_vwap_contrib(df_bt['W_500_Final'], w_500_prev,
                                df_bt['fund_512510__close_price'], df_bt['fund_512510__prev_close'], df_bt['fund_512510__avg_price'])
contrib_hl = calc_vwap_contrib(df_bt['W_HL_Final'], w_hl_prev,
                               df_bt['fund_515180__close_price'], df_bt['fund_515180__prev_close'], df_bt['fund_515180__avg_price'])

# 总成本 = 换手率 * (固定佣金 + 滑点)
df_bt['Strat_Ret'] = (contrib_500 + contrib_hl) - (df_bt['Turnover'] * (COST + SLIPPAGE) * 2)
df_bt['Strat_Cum'] = (1 + df_bt['Strat_Ret']).cumprod()

# === 基准收益 (ETF 涨跌幅) ===
nav_500 = (1 + df_bt['Ret_ETF_500']).cumprod()
nav_hl = (1 + df_bt['Ret_ETF_HL']).cumprod()
df_bt['Bench_Cum'] = 0.5 * nav_500 + 0.5 * nav_hl
df_bt['Bench_Cum'] = df_bt['Bench_Cum'] / df_bt['Bench_Cum'].iloc[0] * df_bt['Strat_Cum'].iloc[0]

# ===================== 7. 结果输出与可视化 =====================

ann_ret = (df_bt['Strat_Cum'].iloc[-1] / df_bt['Strat_Cum'].iloc[0]) ** (252 / len(df_bt)) - 1
bench_ann = (df_bt['Bench_Cum'].iloc[-1] / df_bt['Bench_Cum'].iloc[0]) ** (252 / len(df_bt)) - 1
mdd = (df_bt['Strat_Cum'] / df_bt['Strat_Cum'].cummax() - 1).min()
sharpe = ann_ret / (df_bt['Strat_Ret'].std() * np.sqrt(252))

print("\n" + "=" * 50)
print(f"🚀 独立双轨并行策略 (T信号 -> T+1 VWAP执行) 🚀")
print(f"📅 回测区间: {df_bt.index[0].date()} 至 {df_bt.index[-1].date()}")
print(f"💸 费率设置: COST={COST * 10000:.0f}bps, SLIPPAGE={SLIPPAGE * 10000:.0f}bps")
print("=" * 50)
print(f"策略年化收益: {ann_ret:.2%}")
print(f"基准年化收益: {bench_ann:.2%}")
print(f"超额年化收益: {ann_ret - bench_ann:.2%}")
print(f"最大回撤:    {mdd:.2%}")
print(f"夏普比率:    {sharpe:.2f}")
print(f"日均换手率:   {df_bt['Turnover'].mean():.2%}")
print("-" * 50)

# 特定时间段超额收益统计
print(f"📅 特定区间统计: 【 {SPECIFIC_STAT_DATE} 至今 】")
try:
    df_spec = df_bt.loc[SPECIFIC_STAT_DATE:]
    if not df_spec.empty:
        # 归一化重新计算
        s_ret = df_spec['Strat_Cum'].iloc[-1] / df_spec['Strat_Cum'].iloc[0] - 1
        b_ret = df_spec['Bench_Cum'].iloc[-1] / df_spec['Bench_Cum'].iloc[0] - 1
        excess_spec = s_ret - b_ret
        print(f"   🔹 策略区间收益: {s_ret:.2%}")
        print(f"   🔹 基准区间收益: {b_ret:.2%}")
        print(f"   🔥 区间超额收益: {excess_spec:.2%}")
    else:
        print(f"   ⚠️ 数据未覆盖到 {SPECIFIC_STAT_DATE}")
except Exception as e:
    print(f"   ⚠️ 统计计算错误: {e}")

# ===================== 8. 实盘配仓建议，因子状态详解 =====================
try:
    latest_row = df_bt.iloc[-1]
    latest_date = df_bt.index[-1]

    raw_target_rtvr = latest_row['Target_RTVR']
    raw_target_tsm = latest_row['Target_TSM']
    curr_w_rtvr = latest_row['W_Actual_RTVR']
    curr_w_tsm = latest_row['W_Actual_TSM']

    next_w_rtvr = curr_w_rtvr
    if abs(curr_w_rtvr - raw_target_rtvr) > 0.00001:
        next_w_rtvr = raw_target_rtvr

    tsm_slope = latest_row['TSM_Slope_Abs']
    step = 1.0 if abs(raw_target_tsm - 0.5) < 1e-5 else min(1.0, TSM_MIN_STEP + tsm_slope * TSM_SENSITIVITY)

    next_w_tsm = curr_w_tsm
    if curr_w_tsm < raw_target_tsm:
        next_w_tsm = min(curr_w_tsm + step, raw_target_tsm)
    elif curr_w_tsm > raw_target_tsm:
        next_w_tsm = max(curr_w_tsm - step, raw_target_tsm)

    final_500 = 0.5 * next_w_rtvr + 0.5 * next_w_tsm
    final_hl = 1.0 - final_500

    print("\n" + "#" * 60)
    print(f"📢 实盘配仓指导 (基于数据截止: {latest_date.strftime('%Y-%m-%d')})")
    print("#" * 60)

    # 🌟 【新增需求】 因子状态详解 🌟
    print(f"📊 【因子状态详解】")

    # 1. RTVR 部分
    rtvr_val = latest_row['RTVR_Rank']
    print(f"   1️⃣ RTVR (拥挤度因子):")
    print(f"       👉 当前历史分位数: 【 {rtvr_val:.2%} 】")
    print(f"       📝 判断标准: ")
    print(f"          - [>90%]: 极度拥挤 -> 空仓 (0.0)")
    print(f"          - [70%~90%]: 拥挤 -> 减仓 (0.5->0.0)")
    print(f"          - [40%~60%]: 噪音区 -> 标配 (0.5)")
    print(f"          - [10%~30%]: 恐慌 -> 加仓 (1.0->0.5)")
    print(f"          - [<10%]: 极度恐慌 -> 满仓 (1.0)")

    # 2. TSM 部分 (逻辑清晰化)
    tsm_val = latest_row['TSM_Rel']

    # 获取最后3天的Slope数值 (不做Sign处理，直接显示diff数值)
    idx_loc = df.index.get_loc(latest_date)
    # 提取 TSM_Rel 的差分值（即斜率数值）
    last_3_raw_slopes = df['TSM_Rel'].diff().fillna(0).values[idx_loc - 2: idx_loc + 1]
    # 格式化显示保留5位小数
    formatted_slopes = [float(f"{x:.5f}") for x in last_3_raw_slopes]

    print(f"\n   2️⃣ TSM (时序动量因子):")
    print(f"       👉 当前 TSM 值:    【 {tsm_val:.4f} 】 (阈值: +/- 0.04)")
    print(f"       👉 近3日斜率数值:  【 {formatted_slopes} 】 (>0 向上, <0 向下)")
    print(f"       📝 判断标准 (优先级从上至下):")
    print(f"          1. [值 > 0.04] 且 [3日连续向上] -> 满仓 (1.0)")
    print(f"          2. [值 < -0.04] 且 [3日连续向下] -> 空仓 (0.0)")
    print(f"          3. [值 > 0.04 但趋势反转] 或 [值 < -0.04 但趋势反转] -> 回归标配 (0.5)")
    print(f"          4. 其他情况 -> 维持原有仓位不变")
    print(f"       👉 当前信号判定: {raw_target_tsm}")

    print("-" * 50)
    print(f"👉 【下一日 建议目标仓位】:")
    print(f"   🔴 中证500 (TV_500):  【 {final_500:.2%} 】")
    print(f"   🔵 红利低波 (TV_HL):   【 {final_hl:.2%} 】")
    print("-" * 50)
    print(f"🔍 归因 (T日信号 -> T+1 VWAP):")
    print(f"   RTVR子策略: 当前 {curr_w_rtvr:.2%} -> 原始信号 {raw_target_rtvr:.2%} -> 建议执行 {next_w_rtvr:.2%}")
    print(f"   TSM 子策略: 当前 {curr_w_tsm:.2%} -> 原始信号 {raw_target_tsm:.2%} -> 建议执行 {next_w_tsm:.2%}")
    print("\n💡 操作提示: 此建议已计算了策略的渐进调整步长，请直接按此比例挂单。")
    print("#" * 60 + "\n")
except Exception as e:
    print(f"⚠️ 无法生成实盘建议: {e}")

# 画图
fig, axes = plt.subplots(3, 1, figsize=(12, 12), sharex=True)

axes[0].plot(df_bt['Strat_Cum'], label='双轨合成策略', color='red', linewidth=2)
axes[0].plot(df_bt['Bench_Cum'], label='基准 (Buy&Hold)', color='black', linestyle='--')
axes[0].set_title('策略累计净值 (T+1 Execution Mode)', fontsize=12)
axes[0].legend()
axes[0].grid(True, alpha=0.3)

axes[1].plot(df_bt['W_Actual_RTVR'], color='green', alpha=0.6, label='子账户A: RTVR实际持仓', linewidth=1)
axes[1].plot(df_bt['W_Actual_TSM'], color='orange', alpha=0.6, label='子账户B: TSM实际持仓', linewidth=1)
axes[1].plot(df_bt['W_500_Final'], color='blue', linewidth=2, label='总账户: 合成持仓', linestyle='--')
axes[1].set_title('子策略独立运作 vs 最终合成仓位', fontsize=12)
axes[1].set_ylabel('中证500权重')
axes[1].legend(loc='upper left')
axes[1].grid(True, alpha=0.3)

axes[2].plot(df_bt['Strat_Cum'] / df_bt['Bench_Cum'], color='blue', label='超额净值')
axes[2].axhline(1.0, linestyle='--', color='gray')
axes[2].set_title('超额收益', fontsize=12)
axes[2].grid(True, alpha=0.3)

plt.tight_layout()
plt.show()