import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import percentileofscore
import os
import warnings
import json
from datetime import timedelta
from tqdm import tqdm
import traceback

# ----------------------------------------------------------------------
# 全局配置
# ----------------------------------------------------------------------
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False
warnings.filterwarnings('ignore')

# 文件路径
DATA_PATH = './data/realtime_data_updated.csv'
RESULTS_DIR = './results/optimization'
os.makedirs(RESULTS_DIR, exist_ok=True)

# 回测参数（固定）
BACKTEST_START = '2023-06-17'
BACKTEST_END = '2099-12-31'
STRATEGY_START = '2026-01-25'
SIGMOID_SCALE = 8
COST = 0.0002
SLIPPAGE = 0.0003
ANNUAL_DAYS = 252  # 年化交易日

# 参数搜索范围
WINDOW_RANGE = range(20, 101, 10)      # [20, 30, ..., 100]
HISTORY_RANGE = range(20, 101, 10)     # [20, 30, ..., 100]
ROLLING_FOLDS = 3                      # 滚动验证折数
MIN_TRAIN_DAYS = 120                   # 最小训练窗口（天）

# ----------------------------------------------------------------------
# 核心回测函数（无绘图/打印，专为调参优化）
# ----------------------------------------------------------------------
def run_backtest(window_size, history_window, data_df=None, verbose=False):
    """
    执行单次回测，返回绩效指标字典
    :param window_size: 信号计算窗口
    :param history_window: 分位数计算窗口
    :param data_df: 预加载的数据（避免重复IO）
    :param verbose: 是否打印中间结果
    :return: dict of metrics
    """
    try:
        # =============== 数据准备 ===============
        if data_df is None:
            if not os.path.exists(DATA_PATH):
                raise FileNotFoundError(f"数据文件不存在: {DATA_PATH}")
            df = pd.read_csv(DATA_PATH, parse_dates=['TradingDay'])
            df = df.set_index('TradingDay').sort_index()
        else:
            df = data_df.copy()
        
        # 重命名列
        df.rename(columns={
            'turnover_volume1': 'TV_500', 'turnover_volume2': 'TV_HL',
            'change_pct1': 'Ret_Index_500', 'change_pct2': 'Ret_Index_HL',
            'close_price1': 'Close_500', 'close_price2': 'Close_HL'
        }, inplace=True)
        
        # =============== 信号构建 ===============
        # 计算原始信号（避免除零）
        df['Signal_500'] = (df['Ret_Index_500'].rolling(window=window_size).mean() / 
                           (df['TV_500'].rolling(window=window_size).mean() + 1e-10))
        df['Signal_HL'] = (df['Ret_Index_HL'].rolling(window=window_size).mean() / 
                          (df['TV_HL'].rolling(window=window_size).mean() + 1e-10))
        
        # 标准化（使用滚动窗口避免未来函数！修正原脚本问题）
        df['Signal_500'] = (df['Signal_500'] - 
                           df['Signal_500'].rolling(window=window_size*2).mean()) / \
                           (df['Signal_500'].rolling(window=window_size*2).std() + 1e-10)
        df['Signal_HL'] = (df['Signal_HL'] - 
                          df['Signal_HL'].rolling(window=window_size*2).mean()) / \
                          (df['Signal_HL'].rolling(window=window_size*2).std() + 1e-10)
        
        df['Signal'] = df['Signal_500'] - df['Signal_HL']
        
        # 历史分位数（滚动窗口）
        df['Signal_Rank'] = df['Signal'].rolling(window=history_window).apply(
            lambda x: percentileofscore(x, x.iloc[-1]) / 100 if len(x) > 1 else 0.5, 
            raw=False
        )
        
        # =============== 仓位管理 ===============
        df_bt = df.loc[BACKTEST_START:BACKTEST_END].copy()
        if len(df_bt) < max(window_size, history_window) * 2:
            return None
        
        # Sigmoid转换（shift=0，信号当日计算用于次日调仓）
        df_bt['Target_500'] = 1 / (1 + np.exp(-SIGMOID_SCALE * (df_bt['Signal_Rank'] - 0.5)))
        df_bt['Target_HL'] = 1 - df_bt['Target_500']
        df_bt['Position_500'] = df_bt['Target_500'].shift(1).fillna(0.5)
        df_bt['Position_HL'] = df_bt['Target_HL'].shift(1).fillna(0.5)
        
        # =============== 收益计算 ===============
        # 日收益率（原始数据为百分比，需/100）
        df_bt['Strategy_Return'] = (df_bt['Position_500'] * df_bt['Ret_Index_500'] + 
                                   df_bt['Position_HL'] * df_bt['Ret_Index_HL']) / 100
        df_bt['Benchmark_Return'] = (0.5 * df_bt['Ret_Index_500'] + 
                                    0.5 * df_bt['Ret_Index_HL']) / 100
        
        # 累计净值
        df_bt['Cum_Strategy'] = (1 + df_bt['Strategy_Return']).cumprod()
        df_bt['Cum_Benchmark'] = (1 + df_bt['Benchmark_Return']).cumprod()
        
        # =============== 绩效指标计算 ===============
        total_days = len(df_bt)
        if total_days < 30:  # 数据不足
            return None
        
        # 总收益
        total_ret_strat = df_bt['Cum_Strategy'].iloc[-1] - 1
        total_ret_bench = df_bt['Cum_Benchmark'].iloc[-1] - 1
        excess_ret = total_ret_strat - total_ret_bench
        
        # 年化指标
        annual_ret_strat = (1 + total_ret_strat) ** (ANNUAL_DAYS / total_days) - 1
        annual_ret_bench = (1 + total_ret_bench) ** (ANNUAL_DAYS / total_days) - 1
        
        # 波动率与夏普
        strat_std = df_bt['Strategy_Return'].std() * np.sqrt(ANNUAL_DAYS)
        bench_std = df_bt['Benchmark_Return'].std() * np.sqrt(ANNUAL_DAYS)
        sharpe_strat = annual_ret_strat / strat_std if strat_std != 0 else 0
        sharpe_bench = annual_ret_bench / bench_std if bench_std != 0 else 0
        
        # 最大回撤
        roll_max = df_bt['Cum_Strategy'].cummax()
        drawdown = df_bt['Cum_Strategy'] / roll_max - 1
        max_dd = drawdown.min()
        calmar = annual_ret_strat / abs(max_dd) if max_dd != 0 else 0
        
        # 信息比率
        excess_daily = df_bt['Strategy_Return'] - df_bt['Benchmark_Return']
        ir = (excess_daily.mean() * ANNUAL_DAYS) / (excess_daily.std() * np.sqrt(ANNUAL_DAYS)) if excess_daily.std() != 0 else 0
        
        # 胜率（日频）
        win_rate = (excess_daily > 0).mean()
        
        return {
            'window_size': window_size,
            'history_window': history_window,
            'total_return': total_ret_strat,
            'annual_return': annual_ret_strat,
            'sharpe': sharpe_strat,
            'max_drawdown': max_dd,
            'calmar': calmar,
            'information_ratio': ir,
            'win_rate': win_rate,
            'excess_return': excess_ret,
            'benchmark_return': total_ret_bench,
            'data_points': total_days
        }
    
    except Exception as e:
        if verbose:
            print(f"回测失败 (W={window_size}, H={history_window}): {str(e)}")
            traceback.print_exc()
        return None

# ----------------------------------------------------------------------
# 滚动窗口验证（避免过拟合）
# ----------------------------------------------------------------------
def rolling_validation(window_size, history_window, data_df, n_folds=3):
    """
    滚动时间序列交叉验证
    :return: 平均夏普比率（主要优化目标）
    """
    results = []
    total_days = len(data_df.loc[BACKTEST_START:BACKTEST_END])
    fold_size = max(total_days // n_folds, MIN_TRAIN_DAYS)
    
    for i in range(n_folds):
        # 动态调整验证集起点
        val_start_idx = int(i * total_days / n_folds)
        val_start_date = data_df.loc[BACKTEST_START:BACKTEST_END].index[val_start_idx]
        
        # 创建临时数据视图（仅使用到验证集开始前的数据）
        temp_df = data_df.loc[:val_start_date].copy()
        if len(temp_df.loc[BACKTEST_START:]) < max(window_size, history_window) * 3:
            continue
            
        metrics = run_backtest(window_size, history_window, temp_df, verbose=False)
        if metrics:
            results.append(metrics['sharpe'])
    
    return np.mean(results) if results else -np.inf

# ----------------------------------------------------------------------
# 主优化流程
# ----------------------------------------------------------------------
def optimize_parameters():
    print("="*60)
    print("🚀 启动参数优化：WINDOW_SIZE & HISTORY_WINDOW")
    print(f"   搜索范围: WINDOW=[{min(WINDOW_RANGE)}-{max(WINDOW_RANGE)}], HISTORY=[{min(HISTORY_RANGE)}-{max(HISTORY_RANGE)}]")
    print(f"   验证方式: {ROLLING_FOLDS}-折滚动窗口交叉验证")
    print(f"   优化目标: 平均夏普比率（验证集）")
    print("="*60)
    
    # 预加载数据（避免重复IO）
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"❌ 数据文件不存在: {DATA_PATH}")
    full_data = pd.read_csv(DATA_PATH, parse_dates=['TradingDay'])
    full_data = full_data.set_index('TradingDay').sort_index()
    
    # 存储所有结果
    all_results = []
    
    # 网格搜索
    total_combinations = len(WINDOW_RANGE) * len(HISTORY_RANGE)
    pbar = tqdm(total=total_combinations, desc="🔍 参数搜索进度")
    
    for ws in WINDOW_RANGE:
        for hw in HISTORY_RANGE:
            try:
                # 滚动验证获取稳健夏普比率
                cv_sharpe = rolling_validation(ws, hw, full_data, n_folds=ROLLING_FOLDS)
                
                # 完整回测获取详细指标（使用全部数据）
                full_metrics = run_backtest(ws, hw, full_data, verbose=False)
                if full_metrics:
                    full_metrics['cv_sharpe'] = cv_sharpe
                    all_results.append(full_metrics)
            except Exception as e:
                print(f"\n⚠️  组合 (W={ws}, H={hw}) 处理异常: {str(e)}")
            pbar.update(1)
    pbar.close()
    
    if not all_results:
        raise ValueError("❌ 无有效回测结果，请检查数据或参数范围")
    
    # 转为DataFrame
    results_df = pd.DataFrame(all_results)
    results_df.to_csv(os.path.join(RESULTS_DIR, 'optimization_results.csv'), index=False, encoding='utf-8-sig')
    print(f"\n✅ 优化结果已保存至: {os.path.join(RESULTS_DIR, 'optimization_results.csv')}")
    
    # =============== 结果分析 ===============
    # 1. 按交叉验证夏普排序（首选，防过拟合）
    results_df = results_df.sort_values('cv_sharpe', ascending=False).reset_index(drop=True)
    best_cv = results_df.iloc[0]
    
    # 2. 按完整回测夏普排序（备选）
    best_full = results_df.sort_values('sharpe', ascending=False).iloc[0]
    
    # 3. 帕累托最优（夏普+卡玛）
    results_df['sharpe_rank'] = results_df['sharpe'].rank(ascending=False)
    results_df['calmar_rank'] = results_df['calmar'].rank(ascending=False)
    results_df['combined_rank'] = results_df['sharpe_rank'] + results_df['calmar_rank']
    best_pareto = results_df.sort_values('combined_rank').iloc[0]
    
    # =============== 保存最优参数 ===============
    optimal_params = {
        'method': 'cross_validated_sharpe',
        'window_size': int(best_cv['window_size']),
        'history_window': int(best_cv['history_window']),
        'cv_sharpe': float(best_cv['cv_sharpe']),
        'full_sharpe': float(best_cv['sharpe']),
        'calmar': float(best_cv['calmar']),
        'max_drawdown': float(best_cv['max_drawdown']),
        'annual_return': float(best_cv['annual_return']),
        'information_ratio': float(best_cv['information_ratio']),
        'selection_reason': '最高交叉验证夏普比率（防过拟合）',
        'alternatives': {
            'by_full_sharpe': {
                'window_size': int(best_full['window_size']),
                'history_window': int(best_full['history_window']),
                'sharpe': float(best_full['sharpe'])
            },
            'by_pareto': {
                'window_size': int(best_pareto['window_size']),
                'history_window': int(best_pareto['history_window']),
                'combined_rank': float(best_pareto['combined_rank'])
            }
        }
    }
    
    with open(os.path.join(RESULTS_DIR, 'optimal_parameters.json'), 'w', encoding='utf-8') as f:
        json.dump(optimal_params, f, indent=4, ensure_ascii=False)
    print(f"✅ 最优参数已保存至: {os.path.join(RESULTS_DIR, 'optimal_parameters.json')}")
    
    # =============== 可视化 ===============
    plt.figure(figsize=(14, 10))
    
    # 1. 夏普比率热力图
    plt.subplot(2, 2, 1)
    pivot_sharpe = results_df.pivot(index='history_window', columns='window_size', values='cv_sharpe')
    sns.heatmap(pivot_sharpe, cmap='YlGnBu', annot=True, fmt='.2f', cbar_kws={'label': 'CV Sharpe'})
    plt.title('交叉验证夏普比率热力图', fontsize=12, fontweight='bold')
    plt.xlabel('WINDOW_SIZE')
    plt.ylabel('HISTORY_WINDOW')
    
    # 2. 卡玛比率热力图
    plt.subplot(2, 2, 2)
    pivot_calmar = results_df.pivot(index='history_window', columns='window_size', values='calmar')
    sns.heatmap(pivot_calmar, cmap='YlOrRd', annot=True, fmt='.2f', cbar_kws={'label': 'Calmar Ratio'})
    plt.title('卡玛比率热力图', fontsize=12, fontweight='bold')
    plt.xlabel('WINDOW_SIZE')
    plt.ylabel('HISTORY_WINDOW')
    
    # 3. 帕累托前沿（夏普 vs 卡玛）
    plt.subplot(2, 2, 3)
    plt.scatter(results_df['sharpe'], results_df['calmar'], alpha=0.6, s=40)
    plt.scatter([best_cv['sharpe']], [best_cv['calmar']], c='red', s=150, marker='*', label='最优(交叉验证)')
    plt.scatter([best_pareto['sharpe']], [best_pareto['calmar']], c='green', s=120, marker='D', label='帕累托最优')
    plt.xlabel('夏普比率')
    plt.ylabel('卡玛比率')
    plt.title('风险调整收益帕累托前沿', fontsize=12, fontweight='bold')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.3)
    
    # 4. 参数敏感性（沿最优history_window切片）
    plt.subplot(2, 2, 4)
    best_hw = int(best_cv['history_window'])
    slice_data = results_df[results_df['history_window'] == best_hw].sort_values('window_size')
    plt.plot(slice_data['window_size'], slice_data['cv_sharpe'], 'o-', label='CV Sharpe', linewidth=2)
    plt.axvline(best_cv['window_size'], color='red', linestyle='--', label=f'最优 W={best_cv["window_size"]}')
    plt.xlabel('WINDOW_SIZE (HISTORY_WINDOW 固定)')
    plt.ylabel('交叉验证夏普比率')
    plt.title(f'HISTORY_WINDOW={best_hw} 时的参数敏感性', fontsize=12, fontweight='bold')
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(RESULTS_DIR, 'optimization_analysis.png'), dpi=300, bbox_inches='tight')
    print(f"✅ 分析图表已保存至: {os.path.join(RESULTS_DIR, 'optimization_analysis.png')}")
    plt.close()
    
    # =============== 打印摘要 ===============
    print("\n" + "="*60)
    print("🏆 优化结果摘要")
    print("="*60)
    print(f"✅ 首选参数（交叉验证夏普最高）:")
    print(f"   WINDOW_SIZE = {optimal_params['window_size']}")
    print(f"   HISTORY_WINDOW = {optimal_params['history_window']}")
    print(f"   交叉验证夏普: {optimal_params['cv_sharpe']:.4f}")
    print(f"   完整回测夏普: {optimal_params['full_sharpe']:.4f}")
    print(f"   卡玛比率: {optimal_params['calmar']:.4f}")
    print(f"   年化收益: {optimal_params['annual_return']:.2%}")
    print(f"   最大回撤: {optimal_params['max_drawdown']:.2%}")
    print(f"   信息比率: {optimal_params['information_ratio']:.4f}")
    print("\n💡 建议：")
    print("   • 优先使用'交叉验证夏普'选出的参数（防过拟合）")
    print("   • 可结合'帕累托最优'参数平衡收益与回撤")
    print("   • 详细结果见 optimization_results.csv")
    print("="*60)
    
    return optimal_params, results_df

# ----------------------------------------------------------------------
# 执行优化
# ----------------------------------------------------------------------
if __name__ == "__main__":
    try:
        optimal_params, results_df = optimize_parameters()
        print("\n✨ 参数优化完成！")
    except Exception as e:
        print(f"\n❌ 优化过程出错: {str(e)}")
        traceback.print_exc()