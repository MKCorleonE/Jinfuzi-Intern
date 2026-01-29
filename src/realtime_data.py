# coding: utf-8

"""
Last edit date: 20260129
Author: Xupeng Wang, Jiawen Liang
Project: Realtime Analysis
"""

# 加载包
import os
import pandas as pd
import numpy as np
import pymysql
import logging
from datetime import date, datetime, timedelta
import traceback
import time
from typing import Union, List, Dict
import sys


# 获取项目根目录路径
current_dir = os.path.dirname(os.path.abspath(__file__))  # src目录
project_root = os.path.dirname(current_dir)  # 项目根目录

# 将项目根目录添加到Python路径
if project_root not in sys.path:
    sys.path.insert(0, project_root)  # 添加到开头，优先搜索

try:
    # 现在可以正常导入了
    from utils.data_fetcher import DataFetcher
    from utils.data_fetcher_jy import DataFetcher_jy

    print("成功导入 DataFetcher 和 DataFetcher_jy!")
except ImportError as e:
    print(f"导入失败，使用备用类。错误信息: {e}")

    # 备用类
    class DataFetcher:
        def __init__(self, data_dir: str):
            pass

        def trade_cal(self, exchange: str, start_date: str, end_date: str, is_open: str):
            dates = pd.date_range(start=start_date, end=end_date, freq='B')
            df = pd.DataFrame(index=dates)
            return df


# 使用示例
if __name__ == "__main__":
    data_dir = './data/prod_data'
    os.makedirs(data_dir, exist_ok=True)

    jy_fetcher = DataFetcher_jy(data_dir=data_dir)
    start_date = '20201228'
    end_date = '20261231'
    index_codes = [
        '000905.SH',
        '000985',
        '000922.SH',
        '512510',      # 基金
        '515180',      # 基金
    ]
    results = jy_fetcher.fetch_a_index_weight_and_return(
        index_codes=index_codes,
        start_date=start_date,
        end_date=end_date
    )
    if results:
        for code, df in results.items():
            print(f"\n--- {code} 数据预览 ---")
            print(df.head())
            print(f"DataFrame 形状: {df.shape}")
            print(f"DataFrame 列名: {list(df.columns)}")

    # ==================== 新增：合并指定5个文件 ====================
    file_paths = [
        os.path.join(data_dir, "idx_000905_SH_return.csv"),
        os.path.join(data_dir, "idx_000922_SH_return.csv"),
        os.path.join(data_dir, "idx_000985_return.csv"),
        os.path.join(data_dir, "fund_512510_return.csv"),
        os.path.join(data_dir, "fund_515180_return.csv")
    ]

    available_files = []
    for fp in file_paths:
        if os.path.exists(fp):
            available_files.append(fp)
        else:
            jy_fetcher.logger.warning(f"合并所需文件不存在: {fp}")

    if available_files:
        dfs = []
        for fp in available_files:
            df = pd.read_csv(fp, index_col=0, parse_dates=True)
            prefix = os.path.basename(fp).replace('_return.csv', '')
            df = df.add_prefix(prefix + '__')
            dfs.append(df)

        merged_df = pd.concat(dfs, axis=1, join='outer')
        merged_df.index.name = 'TradingDay'

        merged_output_path = os.path.join(data_dir, "merged_index_fund_returns.csv")
        merged_df.to_csv(merged_output_path)
        jy_fetcher.logger.info(f"已成功合并 {len(dfs)} 个文件，保存至: {merged_output_path}")
        jy_fetcher.logger.info(f"合并后数据形状: {merged_df.shape}")
        print("\n--- 合并后数据预览 ---")
        print(merged_df.head())
    else:
        jy_fetcher.logger.error("没有可合并的文件，跳过合并步骤。")
    # ==================== 新增结束 ====================

    jy_fetcher.close()