"""
流动性因子计算模块
输入：原始行情DataFrame（需含指定列）
输出：标准化因子值Series（索引=日期，值∈[0,1]分位数）
设计原则：
- 无副作用（不修改输入df）
- 严格避免未来函数（纯滚动计算）
- 明确输入/输出契约
- 参数可配置
- 完整异常处理
"""
import pandas as pd
import numpy as np
from scipy.stats import percentileofscore
from typing import Tuple, Optional

def calculate_liquidity_factor(
    df: pd.DataFrame,
    window_size: int = 40,
    history_window: int = 20,
    col_ret_500: str = 'Ret_Index_500', # Ret_Index_500
    col_tv_500: str = 'TV_500', # TV_500
    col_ret_hl: str = 'Ret_Index_HL', # Ret_Index_HL
    col_tv_hl: str = 'TV_HL', # TV_HL
    validate_input: bool = True
) -> pd.Series:
    """
    计算流动性冲击因子分位数序列
    
    参数:
        df: 原始数据DataFrame（索引为TradingDay，已排序）
        window_size: 原始信号计算窗口
        history_window: 分位数计算窗口
        col_*: 指定输入列名（支持自定义列名映射）
        validate_input: 是否验证输入数据完整性
    
    返回:
        pd.Series: 索引=日期，值=因子分位数(0~1)，名称='Liquidity_Factor_Rank'
    
    异常:
        ValueError: 输入数据缺失必要列或为空
        RuntimeError: 计算过程中出现非预期错误
    """
    try:
        # ===== 1. 输入验证 =====
        if validate_input:
            required_cols = [col_ret_500, col_tv_500, col_ret_hl, col_tv_hl]
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                raise ValueError(f"缺失必要列: {missing}. 请确保输入数据包含: {required_cols}")
            if df.empty:
                raise ValueError("输入DataFrame为空")
            if not isinstance(df.index, pd.DatetimeIndex):
                raise ValueError("索引必须为DatetimeIndex")
        
        # ===== 2. 安全复制（避免修改原数据）=====
        # 仅选取必要列计算，减少内存占用
        calc_df = df[[col_ret_500, col_tv_500, col_ret_hl, col_tv_hl]].copy()
        
        # ===== 3. 原始信号计算 =====
        # 注：交易量为0时用1e-10避免除零（业务上交易量应>0）
        signal_500 = (
            calc_df[col_ret_500].rolling(window=window_size).mean() / 
            (calc_df[col_tv_500].rolling(window=window_size).mean().replace(0, 1e-10))
        )
        signal_hl = (
            calc_df[col_ret_hl].rolling(window=window_size).mean() / 
            (calc_df[col_tv_hl].rolling(window=window_size).mean().replace(0, 1e-10))
        )
        
        # ===== 4. 滚动标准化（严格避免未来函数）=====
        norm_window = window_size * 2
        signal_500_std = (
            (signal_500 - signal_500.rolling(norm_window).mean()) / 
            (signal_500.rolling(norm_window).std() + 1e-10)
        )
        signal_hl_std = (
            (signal_hl - signal_hl.rolling(norm_window).mean()) / 
            (signal_hl.rolling(norm_window).std() + 1e-10)
        )
        
        # ===== 5. 相对信号与分位数 =====
        relative_signal = signal_500_std - signal_hl_std
        
        # 使用raw=False确保传入Series，支持x.iloc[-1]
        factor_rank = relative_signal.rolling(
            window=history_window
        ).apply(
            lambda x: percentileofscore(x.dropna(), x.iloc[-1]) / 100.0 if len(x.dropna()) > 1 else np.nan,
            raw=False,
            engine='numba' if hasattr(pd.core.window.rolling, 'numba') else 'cython'
        )
        
        # ===== 6. 输出标准化 =====
        result = factor_rank.rename('Liquidity_Factor_Rank')
        result.index.name = 'TradingDay'
        
        # 验证输出范围（调试用，生产环境可注释）
        if not result.dropna().between(0, 1).all():
            invalid = result[~result.between(0, 1, inclusive='both')].dropna()
            print(f"⚠️ 警告: {len(invalid)}个因子值超出[0,1]范围（可能因窗口内数据异常）")
        
        return result
    
    except Exception as e:
        raise RuntimeError(f"流动性因子计算失败: {str(e)}") from e


# ==================== 使用示例（模块内测试） ====================
if __name__ == "__main__":
    # 模拟测试数据（实际使用时由策略脚本传入）
    dates = pd.date_range('2023-01-01', periods=200, freq='B')
    np.random.seed(42)
    test_df = pd.DataFrame({
        'Ret_Index_500': np.random.randn(200) * 0.01,
        'TV_500': np.abs(np.random.randn(200)) * 1e8 + 1e7,
        'Ret_Index_HL': np.random.randn(200) * 0.01,
        'TV_HL': np.abs(np.random.randn(200)) * 1e8 + 1e7
    }, index=dates)
    test_df.index.name = 'TradingDay'
    
    factor = calculate_liquidity_factor(
        test_df,
        window_size=20,
        history_window=10
    )
    
    print("✅ 因子计算成功！示例输出:")
    print(factor.tail(10))
    print(f"\n📊 因子统计: min={factor.min():.4f}, max={factor.max():.4f}, "
          f"mean={factor.mean():.4f}, NaN比例={factor.isna().mean():.2%}")