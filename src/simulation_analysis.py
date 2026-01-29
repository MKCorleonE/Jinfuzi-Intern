import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import warnings

# ----------------------------------------------------------------------
# 📌 0. 全局配置 (Configuration)
# ----------------------------------------------------------------------
warnings.filterwarnings('ignore')
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False


class Config:
    # 🌟 文件路径 (请修改为你的实际路径)
    FILE_PATH = './data/整合数据.csv'

    # 🌟 1. 回测参数 (从2023年开始冷启动)
    START_DATE = '2023-01-01'
    END_DATE = '2099-12-31'

    # 🌟 2. 实盘记录参数 (以此日收盘净值为基准 1.0)
    REAL_START_DATE = '2026-01-26'

    # 🌟 3. 费率设置 (双边)
    COST = 0.0002  # 佣金
    SLIPPAGE = 0.0003  # 冲击成本

    # --- 因子1 参数 (情绪/波动率) ---
    F1_STD_WINDOW = 126
    F1_RANK_WINDOW = 60
    F1_SMOOTH = 5
    F1_HIGH = 0.70
    F1_LOW = 0.30

    # --- 因子2 参数 (资金流/约束) ---
    F2_FLOW_WINDOW = 10
    F2_Z_WINDOW = 32
    F2_SPREAD_TH = 0.4
    F2_REQ_DAYS = 3


# ----------------------------------------------------------------------
# 📌 1. 数据加载与清洗 (强制冷启动截断)
# ----------------------------------------------------------------------
def load_and_clean_data(file_path, cfg):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ 错误：找不到文件 {file_path}")

    print("⏳ 正在加载数据...")
    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
    except Exception as e:
        raise ValueError(f"❌ 读取失败: {e}")

    df['TradingDay'] = pd.to_datetime(df['TradingDay'])
    df = df.set_index('TradingDay').sort_index()

    # 强制截断 (模拟从2023年开始积攒数据)
    df = df[df.index >= cfg.START_DATE].copy()
    if df.empty:
        raise ValueError(f"❌ 错误：截断后无数据，请检查日期或文件。")

    print(f"✂️ 已执行冷启动截断，数据范围: {df.index[0].date()} 至 {df.index[-1].date()}")

    # 列名映射
    df['TV_500'] = df['idx_000905_SH__turnover_value']
    df['MktVal_500'] = df['idx_000905_SH__free_float_mktval']

    main_net_500 = (df['idx_000905_SH__buy_value_xl'] + df['idx_000905_SH__buy_value_l']) - \
                   (df['idx_000905_SH__sell_value_xl'] + df['idx_000905_SH__sell_value_l'])
    retail_net_500 = df['idx_000905_SH__buy_value_s'] - df['idx_000905_SH__sell_value_s']
    df['Flow_Net_500'] = main_net_500 - retail_net_500
    df['Flow_Main_Raw_500'] = main_net_500

    df['TV_HL'] = df['idx_000922_SH__turnover_value']
    df['MktVal_HL'] = df['idx_000922_SH__free_float_mktval']

    main_net_hl = (df['idx_000922_SH__buy_value_xl'] + df['idx_000922_SH__buy_value_l']) - \
                  (df['idx_000922_SH__sell_value_xl'] + df['idx_000922_SH__sell_value_l'])
    retail_net_hl = df['idx_000922_SH__buy_value_s'] - df['idx_000922_SH__sell_value_s']
    df['Flow_Net_HL'] = main_net_hl - retail_net_hl
    df['Flow_Main_Raw_HL'] = main_net_hl

    df['Open_500'] = df['fund_512510__open_price']
    df['Close_500'] = df['fund_512510__close_price']
    df['Prev_500'] = df['fund_512510__prev_close']
    df['VWAP_500'] = df['fund_512510__avg_price']

    df['Open_HL'] = df['fund_515180__open_price']
    df['Close_HL'] = df['fund_515180__close_price']
    df['Prev_HL'] = df['fund_515180__prev_close']
    df['VWAP_HL'] = df['fund_515180__avg_price']

    price_cols = ['Close_500', 'Prev_500', 'VWAP_500', 'Close_HL', 'Prev_HL', 'VWAP_HL']
    for col in price_cols:
        if col in df.columns:
            df[col] = df[col].replace(0, np.nan)

    df[price_cols] = df[price_cols].ffill().bfill()
    df['VWAP_500'] = df['VWAP_500'].fillna(df['Close_500'])
    df['VWAP_HL'] = df['VWAP_HL'].fillna(df['Close_HL'])

    print(f"✅ 数据加载并清洗完成: {len(df)} 条记录")
    return df


# ----------------------------------------------------------------------
# 📌 2. 因子计算引擎
# ----------------------------------------------------------------------
def calc_factor_1(df, cfg):
    tv_500 = np.log(df['TV_500'])
    tv_hl = np.log(df['TV_HL'])

    std_500 = tv_500.rolling(window=cfg.F1_STD_WINDOW).std()
    std_hl = tv_hl.rolling(window=cfg.F1_STD_WINDOW).std()

    factor_raw = std_500 - std_hl
    factor_smooth = factor_raw.rolling(cfg.F1_SMOOTH).mean()

    raw_rank = factor_smooth.rolling(cfg.F1_RANK_WINDOW).rank(pct=False)
    factor_rank = (raw_rank - 1) / (cfg.F1_RANK_WINDOW - 1)

    df['F1_Rank'] = factor_rank
    return df


def calc_factor_2(df, cfg):
    def get_z_score(flow, mkt_val):
        ratio = (flow / mkt_val).rolling(cfg.F2_FLOW_WINDOW).sum()
        z = (ratio - ratio.rolling(cfg.F2_Z_WINDOW).mean()) / ratio.rolling(cfg.F2_Z_WINDOW).std()
        return z

    z1 = get_z_score(df['Flow_Net_500'], df['MktVal_500'])
    z2 = get_z_score(df['Flow_Net_HL'], df['MktVal_HL'])

    df['F2_Spread'] = z1 - z2

    df['MA20_500'] = df['Close_500'].rolling(20).mean()
    df['MA20_HL'] = df['Close_HL'].rolling(20).mean()
    return df


# ----------------------------------------------------------------------
# 📌 3. 信号生成器
# ----------------------------------------------------------------------
def generate_signals(df, cfg):
    print("🚦 正在生成双轨信号...")

    f1_targets = []
    f2_targets = []

    f1_prev_w_base = 0.5
    f1_prev_w_final = 0.5

    f2_last_locked_w = 0.5
    f2_consecutive_bull = 0
    f2_consecutive_bear = 0

    for i in range(len(df)):
        # === 轨道 A: 因子1 (情绪) ===
        rank = df['F1_Rank'].iloc[i]
        curr_w_f1 = 0.5

        if np.isnan(rank):
            curr_w_f1 = 0.5
        elif 0.40 <= rank <= 0.60:
            curr_w_f1 = 0.5
        elif rank >= cfg.F1_HIGH:
            progress = (rank - cfg.F1_HIGH) / (1.0 - cfg.F1_HIGH)
            curr_w_f1 = max(0.0, 0.5 - (progress * 0.5))
        elif rank <= cfg.F1_LOW:
            progress = (cfg.F1_LOW - rank) / cfg.F1_LOW
            curr_w_f1 = min(1.0, 0.5 + (progress * 0.5))
        else:
            curr_w_f1 = f1_prev_w_base

        f1_prev_w_base = curr_w_f1

        # 棘轮
        final_w_f1 = curr_w_f1
        if curr_w_f1 > 0.5:
            final_w_f1 = max(curr_w_f1, f1_prev_w_final) if f1_prev_w_final > 0.5 else curr_w_f1
        elif curr_w_f1 < 0.5:
            final_w_f1 = min(curr_w_f1, f1_prev_w_final) if f1_prev_w_final < 0.5 else curr_w_f1
        else:
            final_w_f1 = 0.5

        f1_prev_w_final = final_w_f1
        f1_targets.append(final_w_f1)

        # === 轨道 B: 因子2 (资金流) ===
        spread = df['F2_Spread'].iloc[i]
        p1, m1 = df['Close_500'].iloc[i], df['MA20_500'].iloc[i]
        p2, m2 = df['Close_HL'].iloc[i], df['MA20_HL'].iloc[i]
        rf1 = df['Flow_Main_Raw_500'].iloc[i]
        rf2 = df['Flow_Main_Raw_HL'].iloc[i]

        curr_w_f2 = f2_last_locked_w

        if pd.isna(spread) or pd.isna(m1):
            curr_w_f2 = 0.5
        else:
            if spread > cfg.F2_SPREAD_TH:
                f2_consecutive_bull += 1
                f2_consecutive_bear = 0
            elif spread < -cfg.F2_SPREAD_TH:
                f2_consecutive_bear += 1
                f2_consecutive_bull = 0
            else:
                f2_consecutive_bull = 0
                f2_consecutive_bear = 0

            if abs(spread) <= cfg.F2_SPREAD_TH:
                curr_w_f2 = 0.5
            elif spread > cfg.F2_SPREAD_TH:
                # 做多 500
                if (f2_consecutive_bull >= cfg.F2_REQ_DAYS) and (p1 > m1) and (rf1 > 0):
                    pct = (spread - cfg.F2_SPREAD_TH) / (2.0 - cfg.F2_SPREAD_TH)
                    raw_w = min(1.0, 0.5 + 0.5 * pct)
                    curr_w_f2 = max(f2_last_locked_w, raw_w) if f2_last_locked_w > 0.5 else raw_w
                else:
                    curr_w_f2 = f2_last_locked_w
            else:
                # 做多 HL
                if (f2_consecutive_bear >= cfg.F2_REQ_DAYS) and (p2 > m2) and (rf2 > 0):
                    pct = (abs(spread) - cfg.F2_SPREAD_TH) / (2.0 - cfg.F2_SPREAD_TH)
                    raw_w = max(0.0, 0.5 - 0.5 * pct)
                    curr_w_f2 = min(f2_last_locked_w, raw_w) if f2_last_locked_w < 0.5 else raw_w
                else:
                    curr_w_f2 = f2_last_locked_w

        f2_last_locked_w = curr_w_f2
        f2_targets.append(curr_w_f2)

    df['Target_F1'] = f1_targets
    df['Target_F2'] = f2_targets
    return df


# ----------------------------------------------------------------------
# 📌 4. 回测执行引擎 (VWAP 撮合 + Shift对齐)
# ----------------------------------------------------------------------
def run_backtest(df, cfg):
    print("🏃 开始回测 (含预热期)...")
    df_bt = df.copy()

    df_bt['Target_F1_Exec'] = df_bt['Target_F1'].shift(1).fillna(0.5)
    df_bt['Target_F2_Exec'] = df_bt['Target_F2'].shift(1).fillna(0.5)

    close_500 = df_bt['Close_500'].values
    prev_500 = df_bt['Prev_500'].values

    close_hl = df_bt['Close_HL'].values
    prev_hl = df_bt['Prev_HL'].values

    def calc_actual_weights(targets, ret_a, ret_b):
        w_actual = np.zeros(len(targets))
        w_curr = targets[0]
        for i in range(len(targets)):
            tgt = targets[i]
            if abs(tgt - w_curr) > 1e-4:
                w_curr = tgt
            w_actual[i] = w_curr
            r_day = w_curr * ret_a[i] + (1 - w_curr) * ret_b[i]
            w_curr = w_curr * (1 + ret_a[i]) / (1 + r_day)
            w_curr = np.clip(w_curr, 0.0, 1.0)
        return w_actual

    ret_500_full = close_500 / prev_500 - 1
    ret_hl_full = close_hl / prev_hl - 1

    w_act_f1 = calc_actual_weights(df_bt['Target_F1_Exec'].values, ret_500_full, ret_hl_full)
    w_act_f2 = calc_actual_weights(df_bt['Target_F2_Exec'].values, ret_500_full, ret_hl_full)

    df_bt['W_F1_Real'] = w_act_f1
    df_bt['W_F2_Real'] = w_act_f2
    df_bt['W_500_Final'] = 0.5 * w_act_f1 + 0.5 * w_act_f2
    df_bt['W_HL_Final'] = 1.0 - df_bt['W_500_Final']

    def calc_vwap_contrib(w_curr, w_prev, close, prev, vwap):
        delta = w_curr - w_prev
        ret_hold = np.minimum(w_curr, w_prev) * (close / prev - 1)
        ret_buy = delta.clip(lower=0) * (close / vwap - 1)
        ret_sell = delta.clip(upper=0).abs() * (vwap / prev - 1)
        return ret_hold + ret_buy + ret_sell

    init_w_500 = df_bt['W_500_Final'].iloc[0]
    init_w_hl = df_bt['W_HL_Final'].iloc[0]

    w_500_prev = df_bt['W_500_Final'].shift(1).fillna(init_w_500)
    w_hl_prev = df_bt['W_HL_Final'].shift(1).fillna(init_w_hl)

    df_bt['Turnover'] = (df_bt['W_500_Final'] - w_500_prev).abs()

    contrib_500 = calc_vwap_contrib(df_bt['W_500_Final'], w_500_prev,
                                    df_bt['Close_500'], df_bt['Prev_500'], df_bt['VWAP_500'])

    contrib_hl = calc_vwap_contrib(df_bt['W_HL_Final'], w_hl_prev,
                                   df_bt['Close_HL'], df_bt['Prev_HL'], df_bt['VWAP_HL'])

    total_fee = df_bt['Turnover'] * (cfg.COST + cfg.SLIPPAGE) * 2

    df_bt['Strat_Ret'] = contrib_500 + contrib_hl - total_fee
    df_bt['Strat_Cum'] = (1 + df_bt['Strat_Ret']).cumprod()

    b_500 = (1 + ret_500_full).cumprod()
    b_hl = (1 + ret_hl_full).cumprod()
    df_bt['Bench_Cum'] = 0.5 * b_500 + 0.5 * b_hl
    df_bt['Bench_Cum'] = df_bt['Bench_Cum'] / df_bt['Bench_Cum'].iloc[0] * df_bt['Strat_Cum'].iloc[0]

    return df_bt


# ----------------------------------------------------------------------
# 📌 5. 绩效报告与绘图
# ----------------------------------------------------------------------
def analyze_performance(df_bt, cfg):
    warmup_days = cfg.F1_STD_WINDOW + cfg.F1_RANK_WINDOW + cfg.F1_SMOOTH

    if len(df_bt) > warmup_days:
        df_plot = df_bt.iloc[warmup_days:].copy()
        print(f"✂️ 报告展示区间: {df_plot.index[0].date()} 至 {df_plot.index[-1].date()}")
    else:
        df_plot = df_bt.copy()
        print("⚠️ 数据过短，无法切除预热期")

    df_plot['Strat_Cum'] = df_plot['Strat_Cum'] / df_plot['Strat_Cum'].iloc[0]
    df_plot['Bench_Cum'] = df_plot['Bench_Cum'] / df_plot['Bench_Cum'].iloc[0]

    days = len(df_plot)
    if days > 0:
        ann_ret = (df_plot['Strat_Cum'].iloc[-1] / df_plot['Strat_Cum'].iloc[0]) ** (252 / days) - 1
        bench_ret = (df_plot['Bench_Cum'].iloc[-1] / df_plot['Bench_Cum'].iloc[0]) ** (252 / days) - 1
        mdd = (df_plot['Strat_Cum'] / df_plot['Strat_Cum'].cummax() - 1).min()
        sharpe = (df_plot['Strat_Ret'].mean() / df_plot['Strat_Ret'].std()) * np.sqrt(252)
        turnover = df_plot['Turnover'].mean()
    else:
        ann_ret = bench_ret = mdd = sharpe = turnover = 0

    print("\n" + "=" * 50)
    print("🏆 全局有效区间回测 (剔除预热期)")
    print("=" * 50)
    print(f"区间策略年化: {ann_ret:.2%}")
    print(f"区间基准年化: {bench_ret:.2%}")
    print(f"区间超额年化: {ann_ret - bench_ret:.2%}")
    print(f"区间最大回撤: {mdd:.2%}")
    print(f"区间夏普比率: {sharpe:.2f}")
    print(f"区间日均换手: {turnover:.2%}")
    print("=" * 50)

    fig, axes = plt.subplots(3, 1, figsize=(12, 12), sharex=True)
    axes[0].plot(df_plot['Strat_Cum'], color='#d62728', lw=2, label='策略净值')
    axes[0].plot(df_plot['Bench_Cum'], color='gray', ls='--', label='基准 (50/50)')
    axes[0].set_title('累计净值曲线 (已剔除预热期)')
    axes[0].legend(loc='upper left')
    axes[0].grid(True, alpha=0.3)

    axes[1].plot(df_plot['W_F1_Real'], color='orange', alpha=0.5, lw=1, label='因子1:情绪')
    axes[1].plot(df_plot['W_F2_Real'], color='green', alpha=0.5, lw=1, label='因子2:资金')
    axes[1].plot(df_plot['W_500_Final'], color='blue', lw=2, label='500总仓位')
    axes[1].set_ylabel('权重')
    axes[1].legend(loc='upper left')
    axes[1].grid(True, alpha=0.3)

    excess = df_plot['Strat_Cum'] / df_plot['Bench_Cum'] - 1
    axes[2].plot(excess, color='purple', alpha=0.8, lw=1.5)
    axes[2].fill_between(excess.index, excess, 0, where=(excess > 0), color='red', alpha=0.1)
    axes[2].fill_between(excess.index, excess, 0, where=(excess < 0), color='green', alpha=0.1)
    axes[2].set_title('相对基准超额收益')
    axes[2].grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()


# ----------------------------------------------------------------------
# 📌 6. 实盘跟踪记录 (基准日修正版)
# ----------------------------------------------------------------------
def print_real_tracking_record(df_bt, cfg):
    """
    计算 REAL_START_DATE 之后的累计收益
    逻辑：将 REAL_START_DATE 当日的收盘净值设为基准 (0%)，统计之后的表现。
    """
    record_date = pd.to_datetime(cfg.REAL_START_DATE)

    # 1. 截取从 Start Date 开始的数据
    df_record = df_bt[df_bt.index >= record_date].copy()

    if df_record.empty:
        print(f"\n⚠️ 无法生成实盘记录: 数据尚未更新到 {cfg.REAL_START_DATE}")
        return

    # 2. 核心修改：使用切片后第一天(即 Start Date) 的净值作为分母
    # 这样 Start Date 当天的收益被归零，只看未来的变化
    base_strat = df_record['Strat_Cum'].iloc[0]
    base_bench = df_record['Bench_Cum'].iloc[0]

    curr_strat = df_record['Strat_Cum'].iloc[-1]
    curr_bench = df_record['Bench_Cum'].iloc[-1]

    strat_cum = curr_strat / base_strat - 1
    bench_cum = curr_bench / base_bench - 1
    excess_cum = strat_cum - bench_cum

    print("\n" + "#" * 60)
    print(f"📈 实盘跟踪记录 (基准日: {cfg.REAL_START_DATE})")
    print("#" * 60)
    print(f"   📅 统计区间: {df_record.index[0].date()}  ->  {df_record.index[-1].date()}")
    print(f"   💰 策略累计收益: {strat_cum:+.2%}")
    print(f"   📊 基准累计收益: {bench_cum:+.2%}")
    print(f"   🔥 累计超额收益: {excess_cum:+.2%}")
    print("#" * 60)


# ----------------------------------------------------------------------
# 📌 7. 详细决策诊断 (已恢复详细打印 & 增加资金数值)
# ----------------------------------------------------------------------
def print_latest_advice_detailed(df_raw, cfg):
    last = df_raw.iloc[-1]
    dt = df_raw.index[-1].date()

    # --- 因子 A (情绪) ---
    rank = last['F1_Rank']
    f1_tgt = last['Target_F1']

    if np.isnan(rank):
        f1_status = "⏳ 数据不足 (NaN)"
        f1_logic = "标配 (0.5)"
    elif rank > cfg.F1_HIGH:
        f1_status = f"🔴 高拥挤/恐慌 (Rank > {cfg.F1_HIGH})"
        f1_logic = "减仓/做空"
    elif rank < cfg.F1_LOW:
        f1_status = f"🟢 低拥挤/极寒 (Rank < {cfg.F1_LOW})"
        f1_logic = "加仓/做多"
    elif 0.40 <= rank <= 0.60:
        f1_status = "⚪ 中性噪音区 (0.4 ~ 0.6)"
        f1_logic = "回归标配 (0.5)"
    else:
        f1_status = "🟡 缓冲观察区"
        f1_logic = "棘轮锁定 (维持前值)"

    # --- 因子 B (资金流) ---
    spread = last['F2_Spread']
    f2_tgt = last['Target_F2']
    rf1 = last['Flow_Main_Raw_500']
    rf2 = last['Flow_Main_Raw_HL']
    p1, m1 = last['Close_500'], last['MA20_500']
    p2, m2 = last['Close_HL'], last['MA20_HL']

    f2_details = ""
    if pd.isna(spread):
        f2_status = "⏳ 数据不足"
        f2_logic = "标配 (0.5)"
    elif spread > cfg.F2_SPREAD_TH:
        f2_status = f"🟢 资金倾向 500 (Spread > {cfg.F2_SPREAD_TH})"
        check_p = "✅" if p1 > m1 else "❌"
        check_f = "✅" if rf1 > 0 else "❌"
        f2_details = f"[约束: 500价格>MA20? {check_p} | 500主力净买>0? {check_f}]"
        f2_logic = "加仓 500" if (p1 > m1 and rf1 > 0) else "约束未通过 -> 不动"

    elif spread < -cfg.F2_SPREAD_TH:
        f2_status = f"🔴 资金倾向 红利 (Spread < -{cfg.F2_SPREAD_TH})"
        check_p = "✅" if p2 > m2 else "❌"
        check_f = "✅" if rf2 > 0 else "❌"
        f2_details = f"[约束: 红利价格>MA20? {check_p} | 红利主力净买>0? {check_f}]"
        f2_logic = "加仓 红利" if (p2 > m2 and rf2 > 0) else "约束未通过 -> 不动"
    else:
        f2_status = "⚪ 震荡中性区"
        f2_logic = "回归标配 (0.5)"

    final_500 = 0.5 * f1_tgt + 0.5 * f2_tgt

    # --- 打印输出 ---
    print("\n" + "#" * 70)
    print(f"📝 策略决策诊断书 (数据截止: {dt})")
    print("#" * 70)

    print(f"\n1️⃣ 因子A [交易情绪]: Rank = {rank:.2%}")
    print(f"   🔹 状态: {f1_status}")
    print(f"   🔹 逻辑: {f1_logic}")
    print(f"   👉 目标仓位 (A轨): {f1_tgt:.2%}")

    print(f"\n2️⃣ 因子B [资金博弈]: Spread = {spread:.4f}")
    print(f"   🔹 状态: {f2_status}")
    # 🌟 新增：资金数值打印
    print(f"   🔹 数值: 500主力={rf1 / 1e8:+.2f}亿 | 红利主力={rf2 / 1e8:+.2f}亿")
    if f2_details:
        print(f"   🔹 细节: {f2_details}")
    print(f"   🔹 逻辑: {f2_logic}")
    print(f"   👉 目标仓位 (B轨): {f2_tgt:.2%}")

    print("-" * 70)
    print(f"🚀 明日(T+1) 综合建议仓位:")
    print(f"   🔴 中证500 ETF:  【 {final_500:.2%} 】")
    print(f"   🔵 红利低波 ETF:  【 {1 - final_500:.2%} 】")
    print("#" * 70 + "\n")


# ----------------------------------------------------------------------
# 📌 主程序
# ----------------------------------------------------------------------
if __name__ == "__main__":
    try:
        cfg = Config()
        df_raw = load_and_clean_data(cfg.FILE_PATH, cfg)
        df_f1 = calc_factor_1(df_raw, cfg)
        df_f2 = calc_factor_2(df_f1, cfg)
        df_sig = generate_signals(df_f2, cfg)
        df_res = run_backtest(df_sig, cfg)
        analyze_performance(df_res, cfg)
        print_real_tracking_record(df_res, cfg)
        print_latest_advice_detailed(df_sig, cfg)
    except Exception as e:
        print(f"\n❌ 运行出错: {e}")
        import traceback

        traceback.print_exc()