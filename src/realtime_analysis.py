import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import percentileofscore
import os

# ----------------------------------------------------------------------
# 📌 0. 全局设置
# ----------------------------------------------------------------------
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# 🌟 参数设置 🌟
FILE_PATH = './data/realtime_data_updated.csv'

# 🌟 回测时间区间选择 🌟
BACKTEST_START_DATE = '2023-01-01'
BACKTEST_END_DATE = '2099-12-31'

# 🌟 【新增】特定统计起始日期 🌟
SPECIFIC_STAT_DATE = '2025-12-24'

# RTVR 策略参数
RTVR_WINDOW = 40 # 对 RTVR 做 40 日均值平滑
RTVR_LOOKBACK = 66 # 在最近 66 天内计算当前值的历史分位数
RTVR_THRESHOLDS = {'H': 0.70, 'L': 0.30, 'FH': 0.90, 'FL': 0.10, 'MH': 0.60, 'ML': 0.40}

# TSM 策略参数
TSM_MIN_STEP = 0.01
TSM_SENSITIVITY = 30

# 🌟 交易成本与滑点 🌟
COST = 0.0002  # 佣金/印花税等固定成本 (万二)
SLIPPAGE = 0.0003  # 滑点 (万三)：模拟大额订单偏离VWAP的冲击成本

# ----------------------------------------------------------------------
# 1. 数据加载与预处理(暂无预处理)
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

df = df.set_index('TradingDay').sort_index()

# ----------------------------------------------------------------------
# 🌟 修改点：区分【信号源数据】和【回测标的数据】
# ----------------------------------------------------------------------
# 1. 信号源：继续使用指数数据 (尾号1, 2)
df.rename(columns={'turnover_value1': 'TV_500', 'turnover_value2': 'TV_HL'}, inplace=True)
df.rename(columns={'index_return1': 'Ret_Index_500', 'index_return2': 'Ret_Index_HL'}, inplace=True)

# 2. 回测标的：使用 ETF 数据 (尾号4, 5)
# 步骤A: 将0值视为缺失值 (避免除以0错误)
cols_to_clean = ['close_price4', 'prev_close4', 'avg_price4', 'close_price5', 'prev_close5', 'avg_price5']
for col in cols_to_clean:
    if col in df.columns:
        df[col] = df[col].replace(0, np.nan)

# 步骤B: 前向填充 (处理停牌) -> 后向填充 (处理上市初期)
df[cols_to_clean] = df[cols_to_clean].ffill().bfill()

# 步骤C: 再次兜底 VWAP (如果某天avg_price缺失，用close兜底)
df['VWAP_500'] = df['avg_price4'].fillna(df['close_price4'])
df['Close_500'] = df['close_price4']
df['Prev_500'] = df['prev_close4']
df['Prev_500'] = df['Prev_500'].fillna(df['open_price4'])

df['VWAP_HL'] = df['avg_price5'].fillna(df['close_price5'])
df['Close_HL'] = df['close_price5']
df['Prev_HL'] = df['prev_close5']
df['Prev_HL'] = df['Prev_HL'].fillna(df['open_price5'])

# 计算 ETF 涨跌幅 (现已保证分母不为0)
df['Ret_ETF_500'] = df['Close_500'] / df['Prev_500'] - 1
df['Ret_ETF_HL'] = df['Close_HL'] / df['Prev_HL'] - 1

# ----------------------------------------------------------------------
# 2. 准备两个因子的原始数据 (逻辑不变，使用指数数据)
# ----------------------------------------------------------------------
# === 因子 A: RTVR 数据准备 ===
df['RTVR'] = df['TV_500'] / (df['TV_500'] + df['TV_HL']) # 中证500交易额 / 中证500交易额 + 红利交易额
df['RTVR_Factor'] = df['RTVR'].rolling(RTVR_WINDOW).mean() # 取滑动平均值
df['RTVR_Rank'] = df['RTVR_Factor'].rolling(RTVR_LOOKBACK).apply(
    lambda x: percentileofscore(x[:-1], x.iloc[-1]) / 100 if len(x) == RTVR_LOOKBACK else np.nan, raw=False
) # 计算当前值在过去66天中的分位数


# === 因子 B: TSM 数据准备 ===
def calc_tsm(prefix):
    high, low, close = df[f'high_price{prefix}'], df[f'low_price{prefix}'], df[f'prev_close{prefix}']
    open_p = df[f'open_price{prefix}']
    rng = (high - low).replace(0, np.nan)
    t1 = ((high - close) / rng).fillna(0).rolling(69).mean()
    t2 = ((high - open_p) / rng).fillna(0).rolling(3).mean()
    return 0.5 * t1 + 0.5 * t2


df['TSM_Rel'] = (calc_tsm('1') - calc_tsm('2')).ewm(span=25, adjust=False).mean()
df['TSM_Slope_Abs'] = df['TSM_Rel'].diff().abs().fillna(0)


# ----------------------------------------------------------------------
# 3. 计算各自的目标仓位 (Target Generation - T日信号)
# ----------------------------------------------------------------------

# --- RTVR Target Logic ---
def get_rtvr_target(P):
    if pd.isna(P): return 0.5
    if P > 0.90: return 0.0 # 中证500太拥挤了，直接空仓中证500
    if 0.70 < P <= 0.90: return 0.5 - ((P - 0.70) / 0.20) * 0.5
    if P < 0.10: return 1.0 # 红利太拥挤了，直接空仓红利
    if 0.10 <= P < 0.30: return 0.5 + ((0.30 - P) / 0.20) * 0.5
    return np.nan


df['Target_RTVR'] = 0.5
rtvr_w = 0.5
for i in range(3, len(df)):
    P = df['RTVR_Rank'].iloc[i]
    if 0.40 <= P <= 0.60:
        rtvr_w = 0.5
    elif P > 0.70 or P < 0.30:
        p_cur, p_prev, p_prev2 = df['RTVR_Rank'].iloc[i: i - 3: -1]
        is_trend = (p_cur > p_prev > p_prev2) if P > 0.7 else (p_cur < p_prev < p_prev2)
        if is_trend:
            calc_w = get_rtvr_target(P)
            if not pd.isna(calc_w):
                rtvr_w = min(rtvr_w, calc_w) if P > 0.7 else max(rtvr_w, calc_w)
    df.iloc[i, df.columns.get_loc('Target_RTVR')] = rtvr_w

# --- TSM Target Logic ---
df['Target_TSM'] = 0.5
tsm_w = 0.5
# 提前计算slope符号，方便循环
slope_signs = np.sign(df['TSM_Rel'].diff()).fillna(0).values
tsm_vals = df['TSM_Rel'].values

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

    df.iloc[i, df.columns.get_loc('Target_TSM')] = tsm_w

# ----------------------------------------------------------------------
# 4. 双轨独立执行 (实现 T+1 开盘/VWAP 调仓)
# ----------------------------------------------------------------------
start_idx = max(RTVR_LOOKBACK, 90)
df_valid = df.iloc[start_idx:].copy()

# 信号滞后 (T日收盘信号 -> T+1日执行)
df_valid['Target_RTVR_Exec'] = df_valid['Target_RTVR'].shift(1)
df_valid['Target_TSM_Exec'] = df_valid['Target_TSM'].shift(1)
df_valid['Slope_Exec'] = df_valid['TSM_Slope_Abs'].shift(1)

# 时间筛选
try:
    df_bt = df_valid.loc[BACKTEST_START_DATE:BACKTEST_END_DATE].copy()
    if df_bt.empty: raise ValueError("Selected date range is empty")
    print(f"✅ 已筛选回测区间: {df_bt.index[0].date()} 至 {df_bt.index[-1].date()}")
except Exception as e:
    print(f"⚠️ 日期筛选异常，使用全部数据: {e}")
    df_bt = df_valid.copy()

# 准备数据数组
ret_500 = df_bt['Ret_ETF_500'].values
ret_hl = df_bt['Ret_ETF_HL'].values

target_rtvr_exec = df_bt['Target_RTVR_Exec'].fillna(0.5).values
target_tsm_exec = df_bt['Target_TSM_Exec'].fillna(0.5).values
slope_exec = df_bt['Slope_Exec'].fillna(0).values

# === 轨道 1: RTVR ===
w_actual_rtvr = np.zeros(len(df_bt))

# 初始仓位Warm Start
w_close_rtvr = target_rtvr_exec[0]

for i in range(len(df_bt)):
    w_curr = w_close_rtvr
    tgt = target_rtvr_exec[i]

    if abs(w_curr - tgt) > 0.00001:
        w_curr = tgt

    w_actual_rtvr[i] = w_curr

    # 漂移计算
    r_day = w_curr * ret_500[i] + (1 - w_curr) * ret_hl[i]
    w_close_rtvr = w_curr * (1 + ret_500[i]) / (1 + r_day)
    w_close_rtvr = np.clip(w_close_rtvr, 0.0, 1.0)

# === 轨道 2: TSM ===
w_actual_tsm = np.zeros(len(df_bt))

# 初始仓位Warm Start
w_close_tsm = target_tsm_exec[0]

for i in range(len(df_bt)):
    w_curr = w_close_tsm
    tgt = target_tsm_exec[i]
    slope = slope_exec[i]

    step = 1.0 if abs(tgt - 0.5) < 1e-5 else min(1.0, TSM_MIN_STEP + slope * TSM_SENSITIVITY)

    if w_curr < tgt:
        w_curr = min(w_curr + step, tgt)
    elif w_curr > tgt:
        w_curr = max(w_curr - step, tgt)

    w_actual_tsm[i] = w_curr

    r_day = w_curr * ret_500[i] + (1 - w_curr) * ret_hl[i]
    w_close_tsm = w_curr * (1 + ret_500[i]) / (1 + r_day)
    w_close_tsm = np.clip(w_close_tsm, 0.0, 1.0)

# ----------------------------------------------------------------------
# 5. 组合与绩效 (FoF 模式 - VWAP 收益)
# ----------------------------------------------------------------------
df_bt['W_Actual_RTVR'] = w_actual_rtvr
df_bt['W_Actual_TSM'] = w_actual_tsm
df_bt['W_500_Final'] = 0.5 * df_bt['W_Actual_RTVR'] + 0.5 * df_bt['W_Actual_TSM']
df_bt['W_HL_Final'] = 1.0 - df_bt['W_500_Final']

# 🔴 计算单边换手率 (无重复计算) 🔴
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

contrib_500 = calc_vwap_contrib(df_bt['W_500_Final'], w_500_prev,
                                df_bt['Close_500'], df_bt['Prev_500'], df_bt['VWAP_500'])
contrib_hl = calc_vwap_contrib(df_bt['W_HL_Final'], w_hl_prev,
                               df_bt['Close_HL'], df_bt['Prev_HL'], df_bt['VWAP_HL'])

# 总成本 = 换手率 * (固定佣金 + 滑点)
df_bt['Strat_Ret'] = (contrib_500 + contrib_hl) - (df_bt['Turnover'] * (COST + SLIPPAGE) * 2)
df_bt['Strat_Cum'] = (1 + df_bt['Strat_Ret']).cumprod()

# === 基准收益 (ETF 涨跌幅) ===
nav_500 = (1 + df_bt['Ret_ETF_500']).cumprod()
nav_hl = (1 + df_bt['Ret_ETF_HL']).cumprod()
df_bt['Bench_Cum'] = 0.5 * nav_500 + 0.5 * nav_hl
df_bt['Bench_Cum'] = df_bt['Bench_Cum'] / df_bt['Bench_Cum'].iloc[0] * df_bt['Strat_Cum'].iloc[0]

# ----------------------------------------------------------------------
# 6. 报告与画图
# ----------------------------------------------------------------------
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

# 🌟 【新增需求】 特定时间段超额收益统计 🌟
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

# ----------------------------------------------------------------------
# 7. 实盘配仓建议 & 因子状态详解
# ----------------------------------------------------------------------
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