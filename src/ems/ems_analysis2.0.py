import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os

# ----------------------------------------------------------------------
# 📌 0. 全局设置（已更新回测起始日期）
# ----------------------------------------------------------------------
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

FILE_PATH = r"D:\jinfuziquant\data\realtime_data_updated.csv"
BACKTEST_START_DATE = '2026-01-25'  # ✅ 实盘起始日期
BACKTEST_END_DATE = '2099-12-31'

# ======================================================================
# 🌟 核心参数
# ======================================================================
SENTIMENT_WINDOW = 30
MID_TERM_WINDOW = 20
SHORT_TERM_WINDOW = 4
REVERSAL_WEIGHT = 0.8

STRENGTH_WINDOW = 60
THRES_START = 0.5
THRES_FULL = 1.5
THRES_RESET = 0.2

COST = 0.0002
SLIPPAGE = 0.0003

# ----------------------------------------------------------------------
# 1. 数据准备
# ----------------------------------------------------------------------
if not os.path.exists(FILE_PATH):
    print(f"❌ 错误：找不到文件 {FILE_PATH}")
    exit()

df = pd.read_csv(FILE_PATH, parse_dates=['TradingDay']).set_index('TradingDay').sort_index()

df.rename(columns={
    'index_return1': 'Ret_Idx_500', 'turnover_value1': 'Val_500', 'negotiable_mv1': 'MV_500',
    'index_return2': 'Ret_Idx_HL', 'turnover_value2': 'Val_HL', 'negotiable_mv2': 'MV_HL'
}, inplace=True)

cols_etf = ['close_price4', 'prev_close4', 'close_price5', 'prev_close5']
df[cols_etf] = df[cols_etf].replace(0, np.nan).ffill().bfill()
df['Ret_ETF_500'] = df['close_price4'] / df['prev_close4'] - 1
df['Ret_ETF_HL'] = df['close_price5'] / df['prev_close5'] - 1

# ----------------------------------------------------------------------
# 2. 因子计算（在整个数据集上计算，确保Z-Score有足够历史窗口）
# ----------------------------------------------------------------------
def calc_sentiment_residual(series_ret, series_val, series_mv, window):
    if series_mv.sum() == 0 or series_mv.isna().all():
        tr = np.log(series_val)
        delta_tr = tr.diff()
    else:
        tr = series_val / series_mv
        delta_tr = tr / tr.shift(1) - 1
    delta_tr = delta_tr.replace([np.inf, -np.inf], np.nan).fillna(0)
    
    cov = series_ret.rolling(window).cov(delta_tr)
    var = delta_tr.rolling(window).var()
    beta = cov / var
    alpha = series_ret.rolling(window).mean() - beta * delta_tr.rolling(window).mean()
    return series_ret - (alpha + beta * delta_tr)

df['Sent_500'] = calc_sentiment_residual(df['Ret_Idx_500'], df['Val_500'], df['MV_500'], SENTIMENT_WINDOW)
df['Sent_HL'] = calc_sentiment_residual(df['Ret_Idx_HL'], df['Val_HL'], df['MV_HL'], SENTIMENT_WINDOW)
df['Factor_Cum'] = (df['Sent_500'] - df['Sent_HL']).cumsum()
df['Mom_Mid'] = df['Factor_Cum'].diff(MID_TERM_WINDOW)
df['Mom_Short'] = df['Factor_Cum'].diff(SHORT_TERM_WINDOW)
df['Alpha_Score'] = df['Mom_Mid'] - (REVERSAL_WEIGHT * df['Mom_Short'])
df['Alpha_Score_Smooth'] = df['Alpha_Score'].rolling(3).mean()

# 计算全量Z-Score（需要历史窗口）
roll_mean = df['Alpha_Score_Smooth'].rolling(STRENGTH_WINDOW).mean()
roll_std = df['Alpha_Score_Smooth'].rolling(STRENGTH_WINDOW).std()
df['Signal_Z'] = (df['Alpha_Score_Smooth'] - roll_mean) / roll_std

# ----------------------------------------------------------------------
# 3. 🔥 棘轮仓位管理函数（保持不变）
# ----------------------------------------------------------------------
def calculate_ratchet_weight(z_values, start, full, reset):
    weights = []
    current_w = 0.5  # 严格从50%初始化
    for z in z_values:
        if pd.isna(z):
            weights.append(0.5)
            continue
        if current_w > 0.5:
            if z < reset:
                current_w = 0.5
            else:
                raw_w = 0.5 + 0.5 * (z - start) / (full - start)
                raw_w = min(raw_w, 1.0)
                current_w = max(current_w, raw_w)
        elif current_w < 0.5:
            if z > -reset:
                current_w = 0.5
            else:
                raw_w = 0.5 - 0.5 * (abs(z) - start) / (full - start)
                raw_w = max(raw_w, 0.0)
                current_w = min(current_w, raw_w)
        else:
            if z > start:
                current_w = 0.5 + 0.01
            elif z < -start:
                current_w = 0.5 - 0.01
        weights.append(current_w)
    return np.array(weights)

# ----------------------------------------------------------------------
# 4. 回测执行（关键：仅基于回测区间重新计算仓位，严格初始化50%-50%）
# ----------------------------------------------------------------------
df_bt = df.loc[BACKTEST_START_DATE:BACKTEST_END_DATE].copy()

if df_bt.empty:
    print("⚠️  警告：指定日期范围内无有效数据")
    exit()

# ✅ 核心修复：仅使用回测区间内的Signal_Z重新计算Target_Weight（仓位逻辑完全独立）
z_values_bt = df_bt['Signal_Z'].values
targets_bt = calculate_ratchet_weight(z_values_bt, THRES_START, THRES_FULL, THRES_RESET)
df_bt['Target_Weight'] = targets_bt
df_bt['Exec_Weight'] = df_bt['Target_Weight'].shift(1)
df_bt.iloc[0, df_bt.columns.get_loc('Exec_Weight')] = 0.5  # 首日强制50%执行仓位

# 清理无效数据
df_bt = df_bt.dropna(subset=['Signal_Z', 'Exec_Weight', 'Ret_ETF_500', 'Ret_ETF_HL'])
if df_bt.empty:
    print("⚠️  警告：清理后无有效数据")
    exit()

# 执行回测
targets = df_bt['Exec_Weight'].values
weights = np.zeros(len(df_bt))
w_curr = targets[0]

ret_500 = df_bt['Ret_ETF_500'].values
ret_hl = df_bt['Ret_ETF_HL'].values

for i in range(len(df_bt)):
    if abs(w_curr - targets[i]) > 0.001:
        w_curr = targets[i]
    weights[i] = w_curr
    r_day = w_curr * ret_500[i] + (1 - w_curr) * ret_hl[i]
    w_curr = w_curr * (1 + ret_500[i]) / (1 + r_day) if (1 + r_day) != 0 else w_curr
    w_curr = np.clip(w_curr, 0.0, 1.0)

df_bt['W_500'] = weights
df_bt['Turnover'] = (df_bt['W_500'] - df_bt['W_500'].shift(1).fillna(weights[0])).abs()
raw_ret = df_bt['W_500'] * ret_500 + (1 - df_bt['W_500']) * ret_hl
df_bt['Strat_Ret'] = raw_ret - (df_bt['Turnover'] * (COST + SLIPPAGE) * 2)
df_bt['Strat_Cum'] = (1 + df_bt['Strat_Ret']).cumprod()
df_bt['Bench_Cum'] = (1 + (0.5 * ret_500 + 0.5 * ret_hl)).cumprod()
df_bt['Excess_Cum'] = df_bt['Strat_Cum'] / df_bt['Bench_Cum'] - 1

# ----------------------------------------------------------------------
# 5. 📊 输出监控表格（控制台 + CSV文件）
# ----------------------------------------------------------------------
# 构建CSV数据（格式化为与示例完全一致的字符串）
csv_data = []
for idx, row in df_bt.iterrows():
    date_str = idx.strftime('%Y-%m-%d')
    w_500_target = row['Target_Weight'] if pd.notna(row['Target_Weight']) else 0.5
    w_hl_target = 1 - w_500_target
    alpha_smooth = row['Alpha_Score_Smooth'] if pd.notna(row['Alpha_Score_Smooth']) else 0.0
    z_score = row['Signal_Z'] if pd.notna(row['Signal_Z']) else 0.0
    factor_cum = row['Factor_Cum'] if pd.notna(row['Factor_Cum']) else 0.0
    sent_500 = row['Sent_500'] if pd.notna(row['Sent_500']) else 0.0
    sent_hl = row['Sent_HL'] if pd.notna(row['Sent_HL']) else 0.0
    ret_500_val = row['Ret_ETF_500'] if pd.notna(row['Ret_ETF_500']) else 0.0
    ret_hl_val = row['Ret_ETF_HL'] if pd.notna(row['Ret_ETF_HL']) else 0.0
    strat_cum = row['Strat_Cum'] if pd.notna(row['Strat_Cum']) else 1.0
    excess_cum = row['Excess_Cum'] if pd.notna(row['Excess_Cum']) else 0.0
    
    # 按示例格式构建CSV行（字符串格式，保留%符号和小数位）
    csv_data.append({
        '日期': date_str,
        '目标仓位(500)': f"{w_500_target:.1%}",
        '目标仓位(HL)': f"{w_hl_target:.1%}",
        '平滑Alpha Score': f"{alpha_smooth:.4f}",
        'Z-Score': f"{z_score:.3f}",
        '情绪累积因子': f"{factor_cum:.4f}",
        '500情绪残差': f"{sent_500:.4f}",
        '红利情绪残差': f"{sent_hl:.4f}",
        'ETF_500收益': f"{ret_500_val:.2%}",
        'ETF_红利收益': f"{ret_hl_val:.2%}",
        '策略累计净值': f"{strat_cum:.4f}",
        '累计超额': f"{excess_cum:.2%}"
    })

# 创建CSV DataFrame
csv_df = pd.DataFrame(csv_data, columns=[
    '日期', '目标仓位(500)', '目标仓位(HL)', '平滑Alpha Score', 'Z-Score',
    '情绪累积因子', '500情绪残差', '红利情绪残差', 'ETF_500收益',
    'ETF_红利收益', '策略累计净值', '累计超额'
])

# 保存CSV文件（UTF-8-SIG确保Excel正确显示中文）
output_dir = "images/ems"
os.makedirs(output_dir, exist_ok=True)
csv_path = os.path.join(output_dir, "backtest_monitor_table.csv")
csv_df.to_csv(csv_path, index=False, encoding='utf-8-sig')

# 同时在控制台打印表格（保持原有格式化输出）
print("\n" + "="*145)
print(f"📅 每日实盘监控指标（起始日期: {BACKTEST_START_DATE} | 初始执行仓位: 50% 500 + 50% 红利）")
print(f"✅ CSV表格已保存至: {csv_path}")
print("="*145)
header = (
    f"{'日期':<12} {'目标仓位(500)':>12} {'目标仓位(HL)':>12} {'平滑Alpha Score':>15} "
    f"{'Z-Score':>10} {'情绪累积因子':>12} {'500情绪残差':>12} {'红利情绪残差':>12} "
    f"{'ETF_500收益':>12} {'ETF_红利收益':>12} {'策略累计净值':>12} {'累计超额':>10}"
)
print(header)
print("-"*145)

for _, row in csv_df.iterrows():
    print(
        f"{row['日期']:<12} "
        f"{row['目标仓位(500)']:>11} {row['目标仓位(HL)']:>11} "
        f"{row['平滑Alpha Score']:>14} {row['Z-Score']:>9} "
        f"{row['情绪累积因子']:>11} {row['500情绪残差']:>11} {row['红利情绪残差']:>11} "
        f"{row['ETF_500收益']:>11} {row['ETF_红利收益']:>11} "
        f"{row['策略累计净值']:>11} {row['累计超额']:>9}"
    )

print("-"*145)
print(f"\n✅ CSV监控表格已保存至: {csv_path}")

# ----------------------------------------------------------------------
# 6. 📈 计算并打印累计绩效（非年化，基于2026-01-17起始）
# ----------------------------------------------------------------------
total_return = df_bt['Strat_Cum'].iloc[-1] - 1
bench_return = df_bt['Bench_Cum'].iloc[-1] - 1
total_excess = df_bt['Excess_Cum'].iloc[-1]
turnover_avg = df_bt['Turnover'].mean()
end_date = df_bt.index[-1].strftime('%Y-%m-%d')

print("\n" + "=" * 60)
print(f"🏆 棘轮策略累计绩效（{BACKTEST_START_DATE} 至 {end_date}）🏆")
print(f"⚙️ 初始执行仓位: 50% 500 + 50% 红利 | 仓位逻辑: 从起始日独立初始化")
print("=" * 60)
print(f"✅ 策略累计收益率: {total_return:.2%}")
print(f"🔹 基准累计收益率: {bench_return:.2%}")
print(f"🔥 累计超额收益率: {total_excess:.2%}")
print(f"💸 日均换手率:    {turnover_avg:.2%}")
print(f"📊 总交易日数:     {len(df_bt)}")
print("-" * 60)

# ----------------------------------------------------------------------
# 7. 🌟 画图（保持原逻辑不变）
# ----------------------------------------------------------------------
fig, axes = plt.subplots(4, 1, figsize=(10, 15), sharex=True)

# 1. 净值
axes[0].plot(df_bt['Strat_Cum'], color='#d62728', lw=2, label='棘轮策略')
axes[0].plot(df_bt['Bench_Cum'], color='gray', ls='--', label='基准 (50-50)')
axes[0].set_title('净值表现')
axes[0].legend(loc='upper left')
axes[0].grid(True, alpha=0.3)

# 2. 超额
axes[1].plot(df_bt['Excess_Cum'], color='blue', lw=1.5, label='累计超额收益')
axes[1].axhline(0, color='black', ls='--')
axes[1].fill_between(df_bt.index, df_bt['Excess_Cum'], 0, 
                     where=(df_bt['Excess_Cum'] > 0), color='red', alpha=0.1)
axes[1].set_title('累计超额收益')
axes[1].legend(loc='upper left')
axes[1].grid(True, alpha=0.3)

# 3. 信号强度 Z-Score
axes[2].plot(df_bt['Signal_Z'], color='purple', lw=1, label='Z-Score')
axes[2].axhline(THRES_START, color='red', ls=':', label='加仓起点(0.5)')
axes[2].axhline(THRES_FULL, color='red', ls='--', label='满仓点(1.5)')
axes[2].axhline(THRES_RESET, color='green', ls='-', label='止盈重置点(0.2)')
axes[2].axhline(-THRES_START, color='orange', ls=':')
axes[2].set_title('信号强度与关键阈值')
axes[2].legend(loc='upper left')
axes[2].grid(True, alpha=0.3)

# 4. 棘轮仓位（展示实际执行仓位W_500）
axes[3].plot(df_bt.index, df_bt['W_500'], color='orange', lw=1.5, label='500实际仓位')
axes[3].fill_between(df_bt.index, df_bt['W_500'], 0, color='orange', alpha=0.3)
axes[3].axhline(0.5, color='gray', ls=':', label='标配线 (50%)')
axes[3].set_title('实际持仓仓位 (阶梯式加仓 → 垂直重置)')
axes[3].set_ylim(-0.05, 1.05)
axes[3].set_xlabel('日期')
axes[3].legend(loc='upper left')
axes[3].grid(True, alpha=0.3)

plt.tight_layout()

# 保存高清图片
plt.savefig(os.path.join(output_dir, "backtest_result.png"), dpi=300, bbox_inches='tight', facecolor='white')
print(f"✅ 净值图表已保存至: {os.path.join(output_dir, 'backtest_result.png')}")
plt.show()