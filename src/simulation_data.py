#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pandas as pd
import numpy as np
import pymysql
import logging
from datetime import date, datetime
import traceback
from typing import Union, List, Dict
import sys

# 尝试导入外部 DataFetcher，失败则使用 Mock
try:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from utils.data_fetcher import DataFetcher
except ImportError:
    class DataFetcher:
        def __init__(self, data_dir: str):
            pass

        def trade_cal(self, exchange: str, start_date: str, end_date: str, is_open: str):
            dates = pd.date_range(start=start_date, end=end_date, freq='B')
            df = pd.DataFrame(index=dates)
            return df


class DataFetcher_jy:
    """
    A股指数 & 基金数据获取器（统一版）

    功能：
    1. 获取指数成分股权重（保存为 idx_XXX_weight.csv）
    2. 获取指数行情 + 资金流 + 自由流通市值（保存为 idx_XXX_return.csv）
    3. 自动识别基金代码并获取基金行情（保存为 fund_XXX_return.csv）
    4. 自动合并指定5个文件为 merged_index_fund_returns.csv
    """

    def __init__(self, data_dir: str, max_retries: int = 3):
        self.data_dir = data_dir
        self.max_retries = max_retries
        self.logger = self._setup_logger()

        self.db_config = {
            'host': '192.168.20.195',
            'port': 3308,
            'user': 'reader',
            'password': '1qazcde3%TGB',
            'database': 'juyuandb'
        }

        os.makedirs(data_dir, exist_ok=True)
        self._setup_database()

    def _setup_logger(self):
        logger = logging.getLogger('DataFetcher_jy')
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            console_handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        return logger

    def _setup_database(self):
        try:
            self.conn = pymysql.connect(**self.db_config)
            self.logger.info("数据库连接成功")
        except Exception as e:
            self.logger.error(f"数据库连接失败: {str(e)}")
            raise

    def _execute_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        if not hasattr(self, 'conn') or self.conn is None or not self.conn.open:
            self._setup_database()
        try:
            df = pd.read_sql(query, self.conn, params=params)
            return df
        except Exception as e:
            self.logger.error(f"SQL查询失败: {query}, 错误: {e}")
            raise

    def close(self):
        try:
            if hasattr(self, 'conn') and self.conn is not None and self.conn.open:
                self.conn.close()
                self.logger.info("数据库连接已成功关闭")
        except Exception as e:
            self.logger.error(f"关闭数据库连接失败: {str(e)}")

    def fetch_a_index_weight_and_return(
        self,
        index_codes: Union[str, List[str]],
        start_date: str = None,
        end_date: str = None
    ) -> Dict[str, pd.DataFrame]:
        """
        主入口：获取指数/基金数据并保存CSV
        """
        try:
            if isinstance(index_codes, str):
                index_codes = [index_codes]

            self.logger.info(f"开始获取数据：{', '.join(index_codes)}")

            # 格式化日期
            start_date_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}" if start_date else None
            end_date_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}" if end_date else None

            # 清理指数代码（去后缀）
            clean_index_codes = [code.split('.')[0] if '.' in code else code for code in index_codes]

            # === 1. 获取指数 InnerCode ===
            placeholders = ','.join(['%s'] * len(clean_index_codes))
            index_query = f"""
                SELECT InnerCode, SecuCode, SecuMarket
                FROM juyuandb.secumain 
                WHERE SecuCode IN ({placeholders}) AND SecuCategory = 4
            """
            index_df = self._execute_query(index_query, tuple(clean_index_codes))
            index_clean_set = set(index_df['SecuCode'].astype(str)) if not index_df.empty else set()
            index_code_map = dict(zip(index_df['SecuCode'], index_df['InnerCode'])) if not index_df.empty else {}
            market_map = dict(zip(index_df['SecuCode'], index_df['SecuMarket'])) if not index_df.empty else {}

            # 构建带后缀的映射
            index_suffix_map = {}
            for code in clean_index_codes:
                orig = next((c for c in index_codes if c.startswith(code)), code)
                if code in market_map:
                    market = market_map[code]
                    suffix = '.SH' if market == 83 else '.SZ' if market == 90 else ''
                    final_code = f"{code}{suffix}" if '.' not in orig else orig
                else:
                    final_code = orig
                index_suffix_map[code] = final_code

            # === 2. 获取指数权重数据 ===
            weight_df = pd.DataFrame()
            if index_code_map:
                inner_codes = list(index_code_map.values())
                w_placeholders = ','.join(['%s'] * len(inner_codes))
                w_query = f"""
                    SELECT IndexCode, EndDate, InnerCode, Weight
                    FROM juyuandb.LC_IndexComponentsWeight
                    WHERE IndexCode IN ({w_placeholders})
                """
                w_params = inner_codes.copy()
                if start_date: w_query += " AND EndDate >= %s"; w_params.append(pd.to_datetime(start_date_fmt))
                if end_date:   w_query += " AND EndDate <= %s"; w_params.append(pd.to_datetime(end_date_fmt))

                weight_df = self._execute_query(w_query, tuple(w_params))
                if not weight_df.empty:
                    weight_df['EndDate'] = pd.to_datetime(weight_df['EndDate'])

                    # 获取股票代码
                    comp_inner = weight_df['InnerCode'].unique().tolist()
                    if comp_inner:
                        s_placeholders = ','.join(['%s'] * len(comp_inner))
                        stock_query = f"SELECT InnerCode, SecuCode FROM juyuandb.secumain WHERE InnerCode IN ({s_placeholders})"
                        stock_df = self._execute_query(stock_query, tuple(comp_inner))
                        stock_map = dict(zip(stock_df['InnerCode'], stock_df['SecuCode']))
                        weight_df['stock_code'] = weight_df['InnerCode'].map(stock_map)

            # === 3. 获取交易日历 ===
            data_fetcher = DataFetcher(data_dir=self.data_dir)
            trade_cal_df = data_fetcher.trade_cal(
                exchange='XSHG',
                start_date=start_date_fmt,
                end_date=end_date_fmt,
                is_open='1'
            )
            if trade_cal_df.empty:
                self.logger.warning("无法获取交易日历，使用日期范围生成")
                date_range = pd.date_range(start=start_date_fmt, end=end_date_fmt, freq='D')
                trade_dates = date_range.tolist()
            else:
                trade_dates = trade_cal_df.index.tolist()

            # === 4. 获取指数行情 (QT_IndexQuote) ===
            quotes_df = pd.DataFrame()
            if index_code_map:
                q_inner_list = list(index_code_map.values())
                q_placeholders = ','.join(['%s'] * len(q_inner_list))
                q_query = f"""
                    SELECT InnerCode, TradingDay, PrevClosePrice, OpenPrice, HighPrice, LowPrice,
                           ClosePrice, TurnoverVolume, TurnoverValue, NegotiableMV, ChangePCT
                    FROM juyuandb.QT_IndexQuote
                    WHERE InnerCode IN ({q_placeholders})
                """
                q_params = q_inner_list.copy()
                if start_date: q_query += " AND TradingDay >= %s"; q_params.append(pd.to_datetime(start_date_fmt))
                if end_date:   q_query += " AND TradingDay <= %s"; q_params.append(pd.to_datetime(end_date_fmt))

                quotes_df = self._execute_query(q_query, tuple(q_params))
                if not quotes_df.empty:
                    quotes_df['TradingDay'] = pd.to_datetime(quotes_df['TradingDay'])

            # === 5. 获取指数资金流 & 自由流通市值 ===
            index_extra_data = pd.DataFrame()
            if index_code_map:
                inner_list = list(index_code_map.values())
                placeholders = ','.join(['%s'] * len(inner_list))

                # 资金流
                cf_query = f"""
                    SELECT IndexCode, TradingDay, NetBuyValue, BuyValue_XL, SellValue_XL,
                           BuyValue_L, SellValue_L, BuyValue_M, SellValue_M,
                           BuyValue_S, SellValue_S
                    FROM juyuandb.Index_CapitalFlow
                    WHERE IndexCode IN ({placeholders})
                """
                cf_params = inner_list.copy()
                if start_date: cf_query += " AND TradingDay >= %s"; cf_params.append(pd.to_datetime(start_date_fmt))
                if end_date:   cf_query += " AND TradingDay <= %s"; cf_params.append(pd.to_datetime(end_date_fmt))
                capital_flow_df = self._execute_query(cf_query, tuple(cf_params))
                if not capital_flow_df.empty:
                    capital_flow_df['TradingDay'] = pd.to_datetime(capital_flow_df['TradingDay'])

                # 自由流通市值
                mv_query = f"""
                    SELECT IndexCode, TradingDay, FreeFloatMV
                    FROM juyuandb.LC_IndexDerivative
                    WHERE IndexCode IN ({placeholders})
                """
                mv_params = inner_list.copy()
                if start_date: mv_query += " AND TradingDay >= %s"; mv_params.append(pd.to_datetime(start_date_fmt))
                if end_date:   mv_query += " AND TradingDay <= %s"; mv_params.append(pd.to_datetime(end_date_fmt))
                mktval_df = self._execute_query(mv_query, tuple(mv_params))
                if not mktval_df.empty:
                    mktval_df['TradingDay'] = pd.to_datetime(mktval_df['TradingDay'])

                # 合并
                if not capital_flow_df.empty and not mktval_df.empty:
                    index_extra_data = pd.merge(capital_flow_df, mktval_df, on=['IndexCode', 'TradingDay'], how='outer')
                elif not capital_flow_df.empty:
                    index_extra_data = capital_flow_df.copy()
                elif not mktval_df.empty:
                    index_extra_data = mktval_df.copy()
                else:
                    index_extra_data = pd.DataFrame()

            # === 6. 处理指数数据（权重 + 行情）===
            results = {}
            reverse_index_map = {v: k for k, v in index_code_map.items()}

            for inner_code, secu_code in reverse_index_map.items():
                full_code = index_suffix_map.get(secu_code, secu_code)

                # --- 权重 ---
                if not weight_df.empty:
                    idx_w = weight_df[weight_df['IndexCode'] == inner_code].copy()
                    if not idx_w.empty and 'stock_code' in idx_w.columns:
                        pivot = idx_w.pivot(index='EndDate', columns='stock_code', values='Weight').fillna(0)
                        pivot = pivot.reindex(trade_dates, method='ffill')
                        safe_name = full_code.replace('.', '_')
                        w_path = os.path.join(self.data_dir, f"idx_{safe_name}_weight.csv")
                        pivot.to_csv(w_path)
                        self.logger.info(f"✅ 权重已保存: {w_path}")
                        results[full_code] = pivot

                # --- 行情 + 资金流 ---
                if not quotes_df.empty:
                    idx_q = quotes_df[quotes_df['InnerCode'] == inner_code].copy()
                    if not idx_q.empty:
                        idx_q.set_index('TradingDay', inplace=True)
                        if idx_q['ChangePCT'].isna().all():
                            idx_q['ChangePCT'] = (idx_q['ClosePrice'] / idx_q['PrevClosePrice'] - 1) * 100

                        # =======================================================================================
                        # >>> ⭐️ 修复版：安全合并资金流与自由流通市值（核心修复） <<<
                        # =======================================================================================
                        current_extra_df = pd.DataFrame()
                        if not index_extra_data.empty:
                            mask = index_extra_data['IndexCode'] == inner_code
                            current_extra_df = index_extra_data[mask].copy()
                            if not current_extra_df.empty:
                                current_extra_df.set_index('TradingDay', inplace=True)
                                idx_q = idx_q.merge(current_extra_df, left_index=True, right_index=True, how='left')

                        # 确保所有资金流字段存在
                        required_cols = [
                            'NetBuyValue', 'BuyValue_XL', 'SellValue_XL',
                            'BuyValue_L', 'SellValue_L',
                            'BuyValue_M', 'SellValue_M',
                            'BuyValue_S', 'SellValue_S',
                            'FreeFloatMV'
                        ]
                        for col in required_cols:
                            if col not in idx_q.columns:
                                idx_q[col] = np.nan
                        # =======================================================================================
                        # <<< ⭐️ 修复版结束 >>>
                        # =======================================================================================

                        # 构建结果 DataFrame（使用 .get 安全取值）
                        result_df = pd.DataFrame({
                            'index_code': full_code,
                            'prev_close': idx_q['PrevClosePrice'],
                            'open_price': idx_q['OpenPrice'],
                            'high_price': idx_q['HighPrice'],
                            'low_price': idx_q['LowPrice'],
                            'close_price': idx_q['ClosePrice'],
                            'turnover_volume': idx_q['TurnoverVolume'],
                            'turnover_value': idx_q['TurnoverValue'],
                            'negotiable_mv': idx_q['NegotiableMV'],
                            'change_pct': idx_q['ChangePCT'],
                            'index_return': idx_q['ChangePCT'] / 100,

                            # --- 资金流字段（直接从 idx_q 获取，安全）---
                            'net_buy_value': idx_q.get('NetBuyValue', np.nan),
                            'buy_value_xl': idx_q.get('BuyValue_XL', np.nan),
                            'sell_value_xl': idx_q.get('SellValue_XL', np.nan),
                            'buy_value_l': idx_q.get('BuyValue_L', np.nan),
                            'sell_value_l': idx_q.get('SellValue_L', np.nan),
                            'buy_value_m': idx_q.get('BuyValue_M', np.nan),
                            'sell_value_m': idx_q.get('SellValue_M', np.nan),
                            'buy_value_s': idx_q.get('BuyValue_S', np.nan),
                            'sell_value_s': idx_q.get('SellValue_S', np.nan),
                            'free_float_mktval': idx_q.get('FreeFloatMV', np.nan)
                        })

                        safe_name = full_code.replace('.', '_')
                        r_path = os.path.join(self.data_dir, f"idx_{safe_name}_return.csv")
                        result_df.to_csv(r_path)
                        self.logger.info(f"✅ 行情+资金流已保存: {r_path}")
                        if full_code not in results:
                            results[full_code] = result_df

            # === 7. 处理基金数据 ===
            fund_candidates = [code for code in clean_index_codes if code not in index_clean_set]
            for fund_clean in fund_candidates:
                orig_input = next((c for c in index_codes if c.startswith(fund_clean)), fund_clean)
                self.logger.info(f"🔍 尝试获取基金数据: {orig_input}")

                # 获取基金 InnerCode
                inner_df = self._execute_query(
                    "SELECT InnerCode FROM juyuandb.mf_fundarchives WHERE SecuCode = %s",
                    (fund_clean,)
                )
                if inner_df.empty:
                    self.logger.warning(f"⚠️ 基金 {fund_clean} 无档案记录")
                    continue
                inner_code = int(inner_df.iloc[0]['InnerCode'])

                # 查询基金行情
                fund_query = """
                    SELECT TradingDay, PrevClosePrice, OpenPrice, HighPrice, LowPrice, ClosePrice,
                           AvgPrice, ChangeOfPrice, ChangePCT, TurnoverRate, TurnoverVolume,
                           TurnoverValue, VibrationRange, Discount, DiscountRatio
                    FROM juyuandb.qt_fundsperformancehis
                    WHERE InnerCode = %s
                """
                fund_params = [inner_code]
                if start_date: fund_query += " AND TradingDay >= %s"; fund_params.append(pd.to_datetime(start_date_fmt))
                if end_date:   fund_query += " AND TradingDay <= %s"; fund_params.append(pd.to_datetime(end_date_fmt))

                fund_df = self._execute_query(fund_query, tuple(fund_params))
                if fund_df.empty:
                    self.logger.warning(f"⚠️ 基金 {orig_input} 无行情数据")
                    continue

                fund_df['TradingDay'] = pd.to_datetime(fund_df['TradingDay'])
                fund_df.set_index('TradingDay', inplace=True)
                if fund_df['ChangePCT'].isna().all():
                    fund_df['ChangePCT'] = (fund_df['ClosePrice'] / fund_df['PrevClosePrice'] - 1) * 100

                fund_result = pd.DataFrame({
                    'fund_code': orig_input,
                    'prev_close': fund_df['PrevClosePrice'],
                    'open_price': fund_df['OpenPrice'],
                    'high_price': fund_df['HighPrice'],
                    'low_price': fund_df['LowPrice'],
                    'close_price': fund_df['ClosePrice'],
                    'avg_price': fund_df['AvgPrice'],
                    'change_of_price': fund_df['ChangeOfPrice'],
                    'change_pct': fund_df['ChangePCT'],
                    'turnover_rate': fund_df['TurnoverRate'],
                    'turnover_volume': fund_df['TurnoverVolume'],
                    'turnover_value': fund_df['TurnoverValue'],
                    'vibration_range': fund_df['VibrationRange'],
                    'discount': fund_df['Discount'],
                    'discount_ratio': fund_df['DiscountRatio'],
                    'fund_return': fund_df['ChangePCT'] / 100
                })

                safe_fund = orig_input.replace('.', '_')
                f_path = os.path.join(self.data_dir, f"fund_{safe_fund}_return.csv")
                fund_result.to_csv(f_path)
                self.logger.info(f"✅ 基金数据已保存: {f_path}")
                results[orig_input] = fund_result

            return results

        except Exception as e:
            self.logger.error(f"❌ 主流程异常: {e}")
            self.logger.error(traceback.format_exc())
            return {}

    def merge_specified_files(self, file_specs: List[str]):
        """合并指定文件列表"""
        available = []
        for spec in file_specs:
            path = os.path.join(self.data_dir, spec)
            if os.path.exists(path):
                available.append(path)
            else:
                self.logger.warning(f"⚠️ 文件不存在，跳过: {path}")

        if not available:
            self.logger.error("❌ 无可合并文件")
            return

        dfs = []
        for fp in available:
            df = pd.read_csv(fp, index_col=0, parse_dates=True)
            prefix = os.path.basename(fp).replace('_return.csv', '')
            df = df.add_prefix(prefix + '__')
            dfs.append(df)

        merged = pd.concat(dfs, axis=1, join='outer')
        merged.index.name = 'TradingDay'
        output = os.path.join(self.data_dir, "merged_index_fund_returns.csv")
        merged.to_csv(output)
        self.logger.info(f"✅ 合并完成: {output} (shape: {merged.shape})")


# ==================== 使用示例 ====================
if __name__ == "__main__":
    data_dir = '/data/prod_data'
    os.makedirs(data_dir, exist_ok=True)

    fetcher = DataFetcher_jy(data_dir=data_dir)
    try:
        results = fetcher.fetch_a_index_weight_and_return(
            index_codes=[
                '000905.SH',
                '000985',
                '000922.SH',
                '512510',
                '515180'
            ],
            start_date='20201228',
            end_date='20261231'
        )

        # 打印结果概览
        for code, df in results.items():
            print(f"\n📊 {code}: {df.shape} | 列: {list(df.columns)[:5]}...")

        # 合并指定文件
        fetcher.merge_specified_files([
            "idx_000905_SH_return.csv",
            "idx_000922_SH_return.csv",
            "idx_000985_return.csv",
            "fund_512510_return.csv",
            "fund_515180_return.csv"
        ])

    finally:
        fetcher.close()