import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from scipy.stats import spearmanr, pearsonr
import warnings
warnings.filterwarnings('ignore')

# ----------------------------------------------------------------------
# 📌 0. 全局设置（关键修复：分离IC分析区间与实盘回测区间）
# ----------------------------------------------------------------------
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

FILE_PATH = r"D:\jinfuziquant\data\realtime_data_updated.csv"
BACKTEST_START_DATE = '2026-01-25'  # ✅ 实盘起始日期（保持不变）
BACKTEST_END_DATE = '2099-12-31'
IC_ANALYSIS_START_DATE = '2022-01-01'  # 🔑 新增：IC分析专用起始日（早于实盘日）

# ======================================================================
# 🌟 核心参数（保持不变）
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

# 输出目录提前创建
output_dir = "images/ems"
os.makedirs(output_dir, exist_ok=True)

# ----------------------------------------------------------------------
# 1. 数据准备（保持不变）
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
# 2. 因子计算（保持不变）
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

roll_mean = df['Alpha_Score_Smooth'].rolling(STRENGTH_WINDOW).mean()
roll_std = df['Alpha_Score_Smooth'].rolling(STRENGTH_WINDOW).std()
df['Signal_Z'] = (df['Alpha_Score_Smooth'] - roll_mean) / roll_std
df['Rel_Ret_Next'] = (df['Ret_ETF_500'] - df['Ret_ETF_HL']).shift(-1)  # T日因子预测T+1日相对收益

# ----------------------------------------------------------------------
# 🔍 2.5 因子有效性检验：IC分析（核心修复：使用独立IC分析区间）
# ----------------------------------------------------------------------
print("\n" + "="*70)
print(f"🔍 因子有效性检验：IC分析（IC区间: {IC_ANALYSIS_START_DATE} → 最新 | 实盘区间: {BACKTEST_START_DATE} → ）")
print("="*70)

# 全样本IC（整个数据集）
valid_all = df[['Alpha_Score_Smooth', 'Rel_Ret_Next']].dropna()
if len(valid_all) > 30:
    ic_spearman_all, p_spearman_all = spearmanr(valid_all['Alpha_Score_Smooth'], valid_all['Rel_Ret_Next'])
    ic_pearson_all, p_pearson_all = pearsonr(valid_all['Alpha_Score_Smooth'], valid_all['Rel_Ret_Next'])
    print(f"✅ 全样本IC分析（样本量: {len(valid_all)}）:")
    print(f"   • Spearman IC: {ic_spearman_all:+.4f} | P值: {p_spearman_all:.4f} {'✅显著' if p_spearman_all<0.05 else '❌不显著'}")
    print(f"   • Pearson IC:  {ic_pearson_all:+.4f} | P值: {p_pearson_all:.4f}")
else:
    print("⚠️ 全样本有效数据不足")
    ic_spearman_all = ic_pearson_all = np.nan

# 🔑 核心修复：使用IC_ANALYSIS_START_DATE进行IC分析（非实盘起始日！）
df_ic_analysis = df.loc[IC_ANALYSIS_START_DATE:BACKTEST_END_DATE]
valid_ic = df_ic_analysis[['Alpha_Score_Smooth', 'Rel_Ret_Next']].dropna()
ic_spearman_bt = ic_pearson_bt = np.nan
p_spearman_bt = p_pearson_bt = 1.0  # 初始化避免未定义

if len(valid_ic) > 30:
    ic_spearman_bt, p_spearman_bt = spearmanr(valid_ic['Alpha_Score_Smooth'], valid_ic['Rel_Ret_Next'])
    ic_pearson_bt, p_pearson_bt = pearsonr(valid_ic['Alpha_Score_Smooth'], valid_ic['Rel_Ret_Next'])
    ic_abs_mean = valid_ic['Alpha_Score_Smooth'].abs().mean()
    
    print(f"\n🎯 IC分析区间检验（{IC_ANALYSIS_START_DATE} 至 {df_ic_analysis.index[-1].strftime('%Y-%m-%d')} | 样本量: {len(valid_ic)}）:")
    print(f"   • Spearman IC: {ic_spearman_bt:+.4f} | P值: {p_spearman_bt:.4f} {'✅显著' if p_spearman_bt<0.05 else '❌不显著'}")  # 修复变量名
    print(f"   • Pearson IC:  {ic_pearson_bt:+.4f} | P值: {p_pearson_bt:.4f}")  # 修复变量名
    print(f"   • |IC|均值:    {abs(ic_spearman_bt):.4f} | 因子波动: {ic_abs_mean:.4f}")
    
    # 滚动IC稳定性分析
    ROLL_WINDOW = 60
    rolling_ic = []
    rolling_dates = []
    for i in range(ROLL_WINDOW, len(valid_ic)):
        window_data = valid_ic.iloc[i-ROLL_WINDOW:i]
        if len(window_data) >= 30:
            ic_val, _ = spearmanr(window_data['Alpha_Score_Smooth'], window_data['Rel_Ret_Next'])
            rolling_ic.append(ic_val)
            rolling_dates.append(valid_ic.index[i])
    
    if rolling_ic:
        rolling_ic_series = pd.Series(rolling_ic, index=rolling_dates)
        ic_stability = rolling_ic_series.mean() / rolling_ic_series.std() if rolling_ic_series.std() != 0 else 0
        ic_win_rate = (rolling_ic_series > 0).mean()
        
        print(f"\n📈 滚动IC稳定性（窗口={ROLL_WINDOW}日）:")
        print(f"   • IC均值: {rolling_ic_series.mean():+.4f} | IC标准差: {rolling_ic_series.std():.4f}")
        print(f"   • ICIR:   {ic_stability:.2f} {'✅高稳定性' if ic_stability>0.5 else '⚠️稳定性不足'}")
        print(f"   • 胜率(IC>0): {ic_win_rate:.1%} {'✅优秀' if ic_win_rate>0.6 else '⚠️需优化'}")
        
        # 保存滚动IC数据
        ic_analysis = pd.DataFrame({
            '日期': rolling_ic_series.index,
            '滚动IC': rolling_ic_series.values,
            '累计均值IC': rolling_ic_series.expanding().mean().values
        })
        ic_analysis.to_csv(os.path.join(output_dir, "factor_rolling_ic.csv"), index=False, encoding='utf-8-sig')
        print(f"   💾 滚动IC明细已保存: {os.path.join(output_dir, 'factor_rolling_ic.csv')}")
else:
    print(f"⚠️ IC分析区间有效样本不足（需>30，当前{len(valid_ic)}），请检查IC_ANALYSIS_START_DATE设置")

# ----------------------------------------------------------------------
# 📊 IC可视化（智能处理数据不足情况）
# ----------------------------------------------------------------------
fig_ic, axes = plt.subplots(2, 2, figsize=(14, 10))

# (1) 滚动IC序列
if 'rolling_ic_series' in locals() and len(rolling_ic_series) > 0:
    axes[0,0].plot(rolling_ic_series.index, rolling_ic_series, color='#1f77b4', alpha=0.7, label='滚动IC')
    axes[0,0].axhline(rolling_ic_series.mean(), color='red', linestyle='--', 
                      label=f'均值: {rolling_ic_series.mean():+.4f}', linewidth=1.5)
    axes[0,0].axhline(0, color='black', linestyle=':', linewidth=0.8)
    axes[0,0].fill_between(rolling_ic_series.index, rolling_ic_series, 0, where=(rolling_ic_series>0), 
                          color='red', alpha=0.1)
    axes[0,0].fill_between(rolling_ic_series.index, rolling_ic_series, 0, where=(rolling_ic_series<0), 
                          color='green', alpha=0.1)
    axes[0,0].set_title(f'滚动IC序列（窗口={ROLL_WINDOW}日）', fontsize=11, fontweight='bold')
    axes[0,0].set_ylabel('IC值')
    axes[0,0].legend(loc='upper right')
    axes[0,0].grid(True, alpha=0.3)
else:
    axes[0,0].text(0.5, 0.5, '数据不足，无法绘制', ha='center', va='center', fontsize=12, transform=axes[0,0].transAxes)
    axes[0,0].set_title('滚动IC序列（数据不足）', fontsize=11)

# (2) IC分布
if 'rolling_ic_series' in locals() and len(rolling_ic_series) > 0:
    axes[0,1].hist(rolling_ic_series, bins=25, color='#2ca02c', edgecolor='black', alpha=0.7)
    axes[0,1].axvline(rolling_ic_series.mean(), color='red', linestyle='--', linewidth=2, 
                     label=f'均值: {rolling_ic_series.mean():+.4f}')
    axes[0,1].axvline(0, color='black', linestyle=':', linewidth=1)
    axes[0,1].set_title('IC分布直方图', fontsize=11, fontweight='bold')
    axes[0,1].set_xlabel('IC值')
    axes[0,1].set_ylabel('频数')
    axes[0,1].legend()
    axes[0,1].grid(True, alpha=0.3)
else:
    axes[0,1].text(0.5, 0.5, '数据不足，无法绘制', ha='center', va='center', fontsize=12, transform=axes[0,1].transAxes)
    axes[0,1].set_title('IC分布（数据不足）', fontsize=11)

# (3) 因子 vs 收益散点图
if len(valid_ic) > 0:
    axes[1,0].scatter(valid_ic['Alpha_Score_Smooth'], valid_ic['Rel_Ret_Next'], 
                     alpha=0.5, s=15, color='#ff7f0e', edgecolors='none')
    axes[1,0].axhline(0, color='black', linestyle=':', linewidth=0.8)
    axes[1,0].axvline(0, color='black', linestyle=':', linewidth=0.8)
    title_ic = f"{ic_spearman_bt:+.4f}" if not np.isnan(ic_spearman_bt) else "N/A"
    axes[1,0].set_title(f'因子值 vs 下期相对收益\n(Spearman IC={title_ic})', fontsize=11, fontweight='bold')
    axes[1,0].set_xlabel('平滑Alpha Score')
    axes[1,0].set_ylabel('下期500相对红利收益')
    axes[1,0].grid(True, alpha=0.3)
else:
    axes[1,0].text(0.5, 0.5, '数据不足，无法绘制', ha='center', va='center', fontsize=12, transform=axes[1,0].transAxes)
    axes[1,0].set_title('因子 vs 收益（数据不足）', fontsize=11)

# (4) 累计均值IC
if 'rolling_ic_series' in locals() and len(rolling_ic_series) > 0:
    cum_ic = rolling_ic_series.expanding().mean()
    axes[1,1].plot(cum_ic.index, cum_ic, color='#d62728', linewidth=2, label='累计均值IC')
    axes[1,1].axhline(0, color='black', linestyle=':', linewidth=0.8)
    axes[1,1].set_title('累计均值IC（稳定性观察）', fontsize=11, fontweight='bold')
    axes[1,1].set_ylabel('累计均值IC')
    axes[1,1].legend()
    axes[1,1].grid(True, alpha=0.3)
else:
    axes[1,1].text(0.5, 0.5, '数据不足，无法绘制', ha='center', va='center', fontsize=12, transform=axes[1,1].transAxes)
    axes[1,1].set_title('累计均值IC（数据不足）', fontsize=11)

plt.suptitle('📊 因子IC有效性检验报告（基于历史数据）', fontsize=14, fontweight='bold', y=0.995)
plt.tight_layout(rect=[0, 0.03, 1, 0.95])
ic_plot_path = os.path.join(output_dir, "factor_ic_analysis.png")
plt.savefig(ic_plot_path, dpi=300, bbox_inches='tight', facecolor='white')
print(f"\n✅ IC分析图表已保存至: {ic_plot_path}")
plt.show()

# ----------------------------------------------------------------------
# 💡 诊断结论（基于IC分析区间结果）
# ----------------------------------------------------------------------
print("\n" + "="*70)
print("💡 IC诊断结论与行动建议（基于历史数据检验）")
print("="*70)
if not np.isnan(ic_spearman_bt) and len(valid_ic) > 30:
    if abs(ic_spearman_bt) <= 0.03 or p_spearman_bt >= 0.05:
        print("❌【严重警告】因子在历史数据上预测能力不足！")
        print("   → 强烈建议：")
        print("      1. 重构因子逻辑（当前Spearman IC仅{:.4f}，P值{:.4f}）".format(ic_spearman_bt, p_spearman_bt))
        print("      2. 重点检查：情绪残差计算、窗口参数、数据质量")
        print("      3. 尝试调整：SENTIMENT_WINDOW, MID_TERM_WINDOW, REVERSAL_WEIGHT")
        print("      4. ❗ 切勿直接用于实盘！")
    elif abs(ic_spearman_bt) <= 0.06:
        print("⚠️【优化建议】因子有一定效果但需增强（Spearman IC={:.4f}）".format(ic_spearman_bt))
        print("   → 建议：")
        print("      1. 微调平滑参数（Alpha_Score_Smooth窗口）")
        print("      2. 检查Z-Score标准化窗口（STRENGTH_WINDOW）")
        print("      3. 分析滚动IC图表，确认信号持续性")
        print("      4. 谨慎小资金试运行")
    else:
        print("✅【通过】因子历史预测能力良好（Spearman IC={:.4f}）".format(ic_spearman_bt))
        print("   → 建议：")
        print("      1. 重点优化仓位管理参数（THRES_START/FULL/RESET）")
        print("      2. 检查换手率与交易成本影响")
        print("      3. 进行分年度/分市场环境的稳健性检验")
    
    print("\n❗ 重要提示：")
    print("   • IC分析基于历史数据（{}至{}），实盘表现可能变化".format(
        IC_ANALYSIS_START_DATE, df_ic_analysis.index[-1].strftime('%Y-%m-%d')))
    print("   • 实盘回测仍严格从 {} 开始，不受IC分析区间影响".format(BACKTEST_START_DATE))
    print("   • 建议结合分市场环境（牛市/熊市/震荡）进行稳健性检验")
else:
    print("❓ 无法生成诊断结论：IC分析区间样本不足")
    print("   → 请检查：")
    print(f"      1. IC_ANALYSIS_START_DATE ({IC_ANALYSIS_START_DATE}) 是否早于数据起始日")
    print(f"      2. 原始数据文件是否包含足够历史数据")
print("="*70 + "\n")

# ----------------------------------------------------------------------
# 3-7. 棘轮仓位管理 + 回测执行 + 输出（完全保持原逻辑，仅使用BACKTEST_START_DATE）
# ----------------------------------------------------------------------
# ...（此处完全保留您原有代码的第3-7部分，无需任何修改）...
# 为节省篇幅，此处省略重复代码，实际使用时请粘贴您原代码中"3. 棘轮仓位管理函数"至结尾部分
# 关键：实盘回测逻辑完全不受IC_ANALYSIS_START_DATE影响，仍从BACKTEST_START_DATE开始

# ========== 以下是原代码第3-7部分的精简保留（确保完整性）==========
def calculate_ratchet_weight(z_values, start, full, reset):
    weights = []
    current_w = 0.5
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

# 回测执行（严格使用BACKTEST_START_DATE）
df_bt = df.loc[BACKTEST_START_DATE:BACKTEST_END_DATE].copy()
if df_bt.empty:
    print("⚠️ 警告：实盘回测区间无有效数据")
    exit()

z_values_bt = df_bt['Signal_Z'].values
targets_bt = calculate_ratchet_weight(z_values_bt, THRES_START, THRES_FULL, THRES_RESET)
df_bt['Target_Weight'] = targets_bt
df_bt['Exec_Weight'] = df_bt['Target_Weight'].shift(1)
df_bt.iloc[0, df_bt.columns.get_loc('Exec_Weight')] = 0.5

df_bt = df_bt.dropna(subset=['Signal_Z', 'Exec_Weight', 'Ret_ETF_500', 'Ret_ETF_HL'])
if df_bt.empty:
    print("⚠️ 警告：清理后无有效数据")
    exit()

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

# 输出监控表格（保持原逻辑）
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

csv_df = pd.DataFrame(csv_data, columns=[
    '日期', '目标仓位(500)', '目标仓位(HL)', '平滑Alpha Score', 'Z-Score',
    '情绪累积因子', '500情绪残差', '红利情绪残差', 'ETF_500收益',
    'ETF_红利收益', '策略累计净值', '累计超额'
])
csv_path = os.path.join(output_dir, "backtest_monitor_table.csv")
csv_df.to_csv(csv_path, index=False, encoding='utf-8-sig')

print("\n" + "="*145)
print(f"📅 每日实盘监控指标（实盘起始日期: {BACKTEST_START_DATE} | 初始执行仓位: 50% 500 + 50% 红利）")
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

# 累计绩效
total_return = df_bt['Strat_Cum'].iloc[-1] - 1
bench_return = df_bt['Bench_Cum'].iloc[-1] - 1
total_excess = df_bt['Excess_Cum'].iloc[-1]
turnover_avg = df_bt['Turnover'].mean()
end_date = df_bt.index[-1].strftime('%Y-%m-%d')

print("\n" + "=" * 60)
print(f"🏆 棘轮策略累计绩效（实盘区间: {BACKTEST_START_DATE} 至 {end_date}）🏆")
print(f"⚙️ 初始执行仓位: 50% 500 + 50% 红利 | 仓位逻辑: 从实盘起始日独立初始化")
print("=" * 60)
print(f"✅ 策略累计收益率: {total_return:.2%}")
print(f"🔹 基准累计收益率: {bench_return:.2%}")
print(f"🔥 累计超额收益率: {total_excess:.2%}")
print(f"💸 日均换手率:    {turnover_avg:.2%}")
print(f"📊 总交易日数:     {len(df_bt)}")
print("-" * 60)

# 画图
fig, axes = plt.subplots(4, 1, figsize=(10, 15), sharex=True)
axes[0].plot(df_bt['Strat_Cum'], color='#d62728', lw=2, label='棘轮策略')
axes[0].plot(df_bt['Bench_Cum'], color='gray', ls='--', label='基准 (50-50)')
axes[0].set_title('净值表现')
axes[0].legend(loc='upper left')
axes[0].grid(True, alpha=0.3)

axes[1].plot(df_bt['Excess_Cum'], color='blue', lw=1.5, label='累计超额收益')
axes[1].axhline(0, color='black', ls='--')
axes[1].fill_between(df_bt.index, df_bt['Excess_Cum'], 0, 
                     where=(df_bt['Excess_Cum'] > 0), color='red', alpha=0.1)
axes[1].set_title('累计超额收益')
axes[1].legend(loc='upper left')
axes[1].grid(True, alpha=0.3)

axes[2].plot(df_bt['Signal_Z'], color='purple', lw=1, label='Z-Score')
axes[2].axhline(THRES_START, color='red', ls=':', label='加仓起点(0.5)')
axes[2].axhline(THRES_FULL, color='red', ls='--', label='满仓点(1.5)')
axes[2].axhline(THRES_RESET, color='green', ls='-', label='止盈重置点(0.2)')
axes[2].axhline(-THRES_START, color='orange', ls=':')
axes[2].set_title('信号强度与关键阈值')
axes[2].legend(loc='upper left')
axes[2].grid(True, alpha=0.3)

axes[3].plot(df_bt.index, df_bt['W_500'], color='orange', lw=1.5, label='500实际仓位')
axes[3].fill_between(df_bt.index, df_bt['W_500'], 0, color='orange', alpha=0.3)
axes[3].axhline(0.5, color='gray', ls=':', label='标配线 (50%)')
axes[3].set_title('实际持仓仓位 (阶梯式加仓 → 垂直重置)')
axes[3].set_ylim(-0.05, 1.05)
axes[3].set_xlabel('日期')
axes[3].legend(loc='upper left')
axes[3].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(os.path.join(output_dir, "backtest_result.png"), dpi=300, bbox_inches='tight', facecolor='white')
print(f"✅ 净值图表已保存至: {os.path.join(output_dir, 'backtest_result.png')}")
plt.show()