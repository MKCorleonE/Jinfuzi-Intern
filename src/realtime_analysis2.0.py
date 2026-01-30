import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from scipy.stats import percentileofscore
import os

"""
Last Edit Date: 2026-01-30
Author: Jiawen Liang
Project：Two-factor independent track real trading strategy
"""

# ===================== 0. 全局设定 =====================

# 字体设定
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# 文件读取路径
FILE_PATH = './data/交易情绪因子1.csv'

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

# ===================== 1. 数据加载与预处理(暂无预处理) =====================

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


# ===================== 2. 区分【信号源数据】和【标的数据】 =====================

# 1. 信号源数据：来自指数数据

