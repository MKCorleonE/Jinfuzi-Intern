#!/usr/bin/env python
# -*- coding: utf-8 -*-

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

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.data_fetcher import DataFetcher


class DataFetcher_jy:
    """港股数据获取器

    主要功能:
    1. 获取港股指数成份数据
    2. 获取港股证券基本信息
    3. 将成份数据与证券信息匹配并保存到本地
    """

    def __init__(self, data_dir: str, max_retries: int = 3, log_file: str = None):
        """初始化港股数据获取器

        Args:
            data_dir: 数据保存目录
            max_retries: 最大重试次数
        """
        self.data_dir = data_dir
        self.max_retries = max_retries
        self.logger = self._setup_logger()

        # 默认日志文件放在项目 logs 目录下
        if log_file is None:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            log_file = os.path.join(project_root, 'logs', 'data_fetcher_jy.log')
        self.logger = self._setup_logger(log_file=log_file)

        # 数据库连接信息
        # self.db_config = {
        #     'host': '192.168.20.197',
        #     'port': 3316,
        #     'user': 'reader',
        #     'password': '1qazcde3%TGB',
        #     'database': 'jydb'
        # }
        self.db_config = {
            'host': '192.168.20.195',
            'port': 3308,
            'user': 'reader',
            'password': '1qazcde3%TGB',
            'database': 'juyuandb'
        }

        # 指数内部编码映射
        self.index_codes = {
            1071614: "恒生综合大型股指数",
            1071615: "恒生综合中型股指数",
            1071616: "恒生综合小型股指数"
        }

        # 创建数据目录
        os.makedirs(data_dir, exist_ok=True)

        # 初始化数据库连接
        self._setup_database()

    def _setup_logger(self, log_file: str = None):
        """设置日志记录器"""
        logger = logging.getLogger('DataFetcher_jy')
        logger.setLevel(logging.INFO)

        # 避免重复添加 handler
        if logger.handlers:
            logger.handlers.clear()

        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # 设置日志格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)

        # 添加处理器到日志记录器
        logger.addHandler(console_handler)

        # 文件输出
        if log_file:
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            file_handler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.INFO)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        return logger

    def _setup_database(self):
        """初始化数据库连接"""
        try:
            self.conn = pymysql.connect(**self.db_config)
            self.logger.info("数据库连接成功")
        except Exception as e:
            self.logger.error(f"数据库连接失败: {str(e)}")
            raise

    def _execute_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        """执行SQL查询并返回DataFrame

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            查询结果DataFrame
        """

        # 检查连接是否关闭，如果是则重新连接
        if self.conn is None or not self.conn.open:
            self._setup_database()

        # 使用pandas直接查询
        df = pd.read_sql(query, self.conn, params=params)
        return df

    def close(self):
        """关闭数据库连接"""
        try:
            # 检查连接是否存在且处于打开状态
            if hasattr(self, 'conn') and self.conn is not None and self.conn.open:
                self.conn.close()
                self.logger.info("数据库连接已成功关闭")
        except Exception as e:
            self.logger.error(f"关闭数据库连接失败: {str(e)}")

    def fetch_a_index_weight_and_return(self, index_codes: Union[str, List[str]], start_date: str = None,
                                        end_date: str = None) -> Dict[str, pd.DataFrame]:
        """获取A股指数成份股权重数据以及直接从QT_IndexQuote获取的指数收益率数据

        Args:
            index_codes: 指数代码（如'000922.SH'）或指数代码列表（如['000922.SH', '000300.SH']）
            start_date: 开始日期，格式为YYYYMMDD
            end_date: 结束日期，格式为YYYYMMDD

        Returns:
            字典，key为指数代码，value为权重DataFrame
        """
        try:
            # 转换单个指数代码为列表
            if isinstance(index_codes, str):
                index_codes = [index_codes]

            self.logger.info(f"开始获取指数 {', '.join(index_codes)} 的权重数据和指数收益率...")

            # 首先将指数代码转换为不带后缀的形式
            clean_index_codes = [code.split('.')[0] if '.' in code else code for code in index_codes]

            # 获取指数的InnerCode
            placeholders = ','.join(['%s'] * len(clean_index_codes))
            index_query = f"""
                SELECT InnerCode, SecuCode, SecuMarket
                FROM juyuandb.secumain 
                WHERE SecuCode IN ({placeholders})
                AND SecuCategory = 4  -- 指数类别
            """

            index_df = self._execute_query(index_query, tuple(clean_index_codes))
            if index_df.empty:
                self.logger.error(f"未找到指数代码对应的InnerCode")
                return {}

            # 创建指数代码到InnerCode的映射
            index_code_map = dict(zip(index_df['SecuCode'], index_df['InnerCode']))
            market_map = dict(zip(index_df['SecuCode'], index_df['SecuMarket']))

            # 添加后缀到原始指数代码映射
            index_suffix_map = {}
            for idx, code in enumerate(clean_index_codes):
                market = market_map.get(code)
                if market == 83:  # 上海市场
                    suffix = '.SH'
                elif market == 90:  # 深圳市场
                    suffix = '.SZ'
                else:
                    suffix = ''
                index_suffix_map[code] = index_codes[idx] if '.' in index_codes[idx] else f"{code}{suffix}"

            # 构建权重数据查询
            inner_codes = index_df['InnerCode'].tolist()
            placeholders = ','.join(['%s'] * len(inner_codes))
            query = f"""
                SELECT 
                    w.IndexCode,
                    w.EndDate,
                    w.InnerCode,
                    w.Weight,
                    w.UpdateTime
                FROM 
                    juyuandb.LC_IndexComponentsWeight w
                WHERE 
                    w.IndexCode IN ({placeholders})
            """

            params = inner_codes.copy()

            # start_date_fmt = None
            # end_date_fmt = None

            # 添加日期过滤条件
            if start_date:
                query += " AND w.EndDate >= %s"
                # 转换YYYYMMDD为YYYY-MM-DD格式
                start_date_fmt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:]}"
                params.append(pd.to_datetime(start_date_fmt))
            if end_date:
                query += " AND w.EndDate <= %s"
                # 转换YYYYMMDD为YYYY-MM-DD格式
                end_date_fmt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:]}"
                params.append(pd.to_datetime(end_date_fmt))

            # 执行查询
            self.logger.info(f"执行权重数据查询: {query}")
            weight_df = self._execute_query(query, tuple(params))

            if weight_df.empty:
                self.logger.warning(f"未查询到指数权重数据")
                # 继续处理指数行情数据，即使没有权重数据
            else:
                self.logger.info(f"查询到 {len(weight_df)} 条权重记录")
                # 转换日期格式
                weight_df['EndDate'] = pd.to_datetime(weight_df['EndDate'])

            # 获取所有成份股的InnerCode（如果有权重数据）
            component_inner_codes = []
            if not weight_df.empty:
                component_inner_codes = weight_df['InnerCode'].unique().tolist()

                # 查询股票代码
                if component_inner_codes:
                    placeholders = ','.join(['%s'] * len(component_inner_codes))
                    stock_code_query = f"""
                        SELECT InnerCode, SecuCode, SecuMarket  
                        FROM juyuandb.secumain 
                        WHERE InnerCode IN ({placeholders})
                    """

                    stock_df = self._execute_query(stock_code_query, tuple(component_inner_codes))

                    # 创建InnerCode到SecuCode的映射
                    stock_code_map = {}
                    for _, row in stock_df.iterrows():
                        inner_code = row['InnerCode']
                        secu_code = row['SecuCode']
                        market = row['SecuMarket']

                        # 根据市场添加后缀
                        if market == 83:  # 上海市场
                            secu_code = f"{secu_code}"
                        elif market == 90:  # 深圳市场
                            secu_code = f"{secu_code}"

                        stock_code_map[inner_code] = secu_code

                    # 添加股票代码列
                    weight_df['stock_code'] = weight_df['InnerCode'].map(stock_code_map)

            # 获取交易日历
            data_fetcher = DataFetcher(data_dir=self.data_dir)
            trade_cal_df = data_fetcher.trade_cal(
                exchange='XSHG',  # 使用上交所交易日历
                start_date=start_date_fmt if start_date else None,
                end_date=end_date_fmt if end_date else None,
                is_open='1'
            )

            if trade_cal_df.empty:
                self.logger.error("无法获取交易日历数据")
                return {}

            trade_dates = trade_cal_df.index.tolist()
            self.logger.info(f"成功获取交易日历数据，共 {len(trade_dates)} 个交易日")

            # 获取指数行情数据（从QT_IndexQuote表）
            index_quotes_query = f"""
                SELECT 
                    q.ID,
                    q.InnerCode,
                    q.TradingDay,
                    q.PrevClosePrice,
                    q.OpenPrice,
                    q.HighPrice,
                    q.LowPrice,
                    q.ClosePrice,
                    q.TurnoverVolume,
                    q.TurnoverValue,
                    q.TurnoverDeals,
                    q.ChangePCT,
                    q.NegotiableMV,
                    q.XGRQ,
                    q.JSID
                FROM 
                    juyuandb.QT_IndexQuote q
                WHERE 
                    q.InnerCode IN ({','.join(['%s'] * len(index_code_map.values()))})
            """

            quote_params = list(index_code_map.values())

            # 添加日期过滤条件
            if start_date:
                index_quotes_query += " AND q.TradingDay >= %s"
                quote_params.append(pd.to_datetime(start_date_fmt))
            if end_date:
                index_quotes_query += " AND q.TradingDay <= %s"
                quote_params.append(pd.to_datetime(end_date_fmt))

            # 执行指数行情查询
            self.logger.info(f"执行指数行情查询: {index_quotes_query}")
            quotes_df = self._execute_query(index_quotes_query, tuple(quote_params))

            if quotes_df.empty:
                self.logger.warning(f"未查询到指数行情数据")
                # 继续处理权重数据，即使没有行情数据
            else:
                self.logger.info(f"查询到 {len(quotes_df)} 条指数行情记录")
                # 转换日期格式
                quotes_df['TradingDay'] = pd.to_datetime(quotes_df['TradingDay'])

            # 存储每个指数的结果
            results = {}

            # 反向映射InnerCode到指数代码
            reverse_index_map = {v: k for k, v in index_code_map.items()}

            # 对每个指数分别处理
            for index_inner_code, index_code in reverse_index_map.items():
                # 获取带后缀的指数代码
                index_code_with_suffix = index_suffix_map.get(index_code, index_code)

                # 处理权重数据（如果有）
                if not weight_df.empty:
                    # 获取当前指数的权重数据
                    index_weight_df = weight_df[weight_df['IndexCode'] == index_inner_code].copy()

                    if not index_weight_df.empty:
                        # 重塑数据为宽格式 (日期 x 股票)，值为权重
                        weight_pivot = index_weight_df.pivot(
                            index='EndDate',
                            columns='stock_code',
                            values='Weight'
                        )

                        # 处理缺失值
                        weight_pivot = weight_pivot.fillna(0)

                        # 向前填充权重数据到每个交易日
                        weight_pivot = weight_pivot.reindex(trade_dates, method='ffill')

                        # 安全的索引代码格式化
                        safe_index_code = f"idx_{index_code_with_suffix.replace('.', '_')}"

                        # 保存权重数据
                        output_path = os.path.join(self.data_dir, 'index_weight.h5')
                        try:
                            weight_pivot.to_hdf(output_path, key=safe_index_code, mode='a')
                            self.logger.info(
                                f"指数 {index_code_with_suffix} 权重数据已保存到 {output_path} (key: {safe_index_code})")

                            # 输出权重数据统计信息
                            component_counts = (weight_pivot > 0).sum(axis=1)
                            self.logger.info(f"\n指数 {index_code_with_suffix} 权重统计信息:")
                            self.logger.info(
                                f"  - 数据日期范围: {weight_pivot.index.min()} 至 {weight_pivot.index.max()}")
                            self.logger.info(f"  - 总记录日数: {len(weight_pivot)}")
                            self.logger.info(f"  - 平均成份股数量: {component_counts.mean():.2f}")
                            self.logger.info(f"  - 最少成份股数量: {component_counts.min()}")
                            self.logger.info(f"  - 最多成份股数量: {component_counts.max()}")

                            # 添加到结果字典
                            results[index_code_with_suffix] = weight_pivot
                        except Exception as e:
                            self.logger.error(f"保存指数 {index_code_with_suffix} 权重数据失败: {str(e)}")
                    else:
                        self.logger.warning(f"指数 {index_code_with_suffix} 没有查询到权重数据")

                # 处理指数行情数据（如果有）
                if not quotes_df.empty:
                    # 筛选当前指数的行情数据
                    index_quotes = quotes_df[quotes_df['InnerCode'] == index_inner_code].copy()

                    if not index_quotes.empty:
                        # 设置日期为索引
                        index_quotes.set_index('TradingDay', inplace=True)

                        # 计算日收益率（如果ChangePCT字段不可用）
                        if 'ChangePCT' not in index_quotes.columns or index_quotes['ChangePCT'].isna().all():
                            self.logger.info(f"计算指数 {index_code_with_suffix} 的日收益率...")
                            index_quotes['ChangePCT'] = (index_quotes['ClosePrice'] / index_quotes[
                                'PrevClosePrice'] - 1) * 100

                        # 创建结果DataFrame
                        result_df = pd.DataFrame({
                            'index_code': index_code_with_suffix,
                            'prev_close': index_quotes['PrevClosePrice'],
                            'open_price': index_quotes['OpenPrice'],
                            'high_price': index_quotes['HighPrice'],
                            'low_price': index_quotes['LowPrice'],
                            'close_price': index_quotes['ClosePrice'],
                            'turnover_volume': index_quotes['TurnoverVolume'],
                            'turnover_value': index_quotes['TurnoverValue'],
                            'change_pct': index_quotes['ChangePCT'],
                            'negotiable_mv': index_quotes['NegotiableMV'],
                            'index_return': index_quotes['ChangePCT'] / 100
                        })

                        # 保存结果
                        output_path = os.path.join(self.data_dir, f"{safe_index_code}_return.csv")
                        # safe_index_code = f"idx_{index_code_with_suffix.replace('.', '_')}"
                        try:
                            result_df.to_csv(output_path, index=True)
                            # result_df.to_hdf(output_path, key=safe_index_code, mode='a')
                            self.logger.info(
                                f"指数 {index_code_with_suffix} 收益率数据已保存到 {output_path} (key: {safe_index_code})")

                            # 输出行情统计信息
                            self.logger.info(f"\n指数 {index_code_with_suffix} 行情统计信息:")
                            self.logger.info(f"  - 数据日期范围: {result_df.index.min()} 至 {result_df.index.max()}")
                            self.logger.info(f"  - 总交易日数: {len(result_df)}")
                            self.logger.info(f"  - 平均日收益率: {result_df['index_return'].mean() * 100:.4f}%")
                            self.logger.info(
                                f"  - 累计收益率: {((1 + result_df['index_return']).prod() - 1) * 100:.2f}%")

                            # 如果没有权重数据，也将行情数据添加到结果
                            if index_code_with_suffix not in results:
                                results[index_code_with_suffix] = result_df
                        except Exception as e:
                            self.logger.error(f"保存指数 {index_code_with_suffix} 行情数据失败: {str(e)}")
                    else:
                        self.logger.warning(f"未找到指数 {index_code_with_suffix} 的行情数据")


            # =====================================================================
            # ======================== 新增：基金数据处理 ==========================
            # =====================================================================
            # 识别哪些输入代码不是指数（即可能是基金）
            all_clean = set(clean_index_codes)
            index_clean_set = set(index_df['SecuCode'].astype(str))
            fund_candidate_codes = [code for code in clean_index_codes if code not in index_clean_set]

            for fund_code in fund_candidate_codes:
                original_input = next((orig for orig in index_codes if orig.startswith(fund_code)), fund_code)
                self.logger.info(f"尝试获取基金数据: {original_input}")

                # 获取基金 InnerCode
                inner_query = "SELECT InnerCode FROM juyuandb.mf_fundarchives WHERE SecuCode = %s"
                inner_df = self._execute_query(inner_query, (fund_code,))
                if inner_df.empty:
                    self.logger.warning(f"基金 {fund_code} 未找到 InnerCode，跳过")
                    continue
                inner_code = int(inner_df.iloc[0]['InnerCode'])

                # 构建基金行情查询（含你提供的所有字段）
                fund_query = """
                    SELECT 
                        TradingDay,
                        PrevClosePrice,
                        OpenPrice,
                        HighPrice,
                        LowPrice,
                        ClosePrice,
                        AvgPrice,
                        ChangeOfPrice,
                        ChangePCT,
                        TurnoverRate,
                        TurnoverVolume,
                        TurnoverValue,
                        VibrationRange,
                        Discount,
                        DiscountRatio
                    FROM juyuandb.qt_fundsperformancehis
                    WHERE InnerCode = %s
                """
                fund_params = [inner_code]
                if start_date:
                    fund_query += " AND TradingDay >= %s"
                    fund_params.append(pd.to_datetime(start_date_fmt))
                if end_date:
                    fund_query += " AND TradingDay <= %s"
                    fund_params.append(pd.to_datetime(end_date_fmt))

                fund_quotes_df = self._execute_query(fund_query, tuple(fund_params))
                if fund_quotes_df.empty:
                    self.logger.warning(f"基金 {original_input} 无行情数据")
                    continue

                fund_quotes_df['TradingDay'] = pd.to_datetime(fund_quotes_df['TradingDay'])
                fund_quotes_df.set_index('TradingDay', inplace=True)

                # 计算收益率（如果缺失）
                if 'ChangePCT' not in fund_quotes_df.columns or fund_quotes_df['ChangePCT'].isna().all():
                    fund_quotes_df['ChangePCT'] = (fund_quotes_df['ClosePrice'] / fund_quotes_df['PrevClosePrice'] - 1) * 100

                # 构造基金结果 DataFrame（字段完全按你提供）
                fund_result_df = pd.DataFrame({
                    'fund_code': original_input,
                    'prev_close': fund_quotes_df['PrevClosePrice'],          # 昨收盘(元)
                    'open_price': fund_quotes_df['OpenPrice'],               # 开盘价(元)
                    'high_price': fund_quotes_df['HighPrice'],               # 最高价(元)
                    'low_price': fund_quotes_df['LowPrice'],                 # 最低价(元)
                    'close_price': fund_quotes_df['ClosePrice'],             # 收盘价(元)
                    'avg_price': fund_quotes_df['AvgPrice'],                 # 成交均价(元)
                    'change_of_price': fund_quotes_df['ChangeOfPrice'],      # 涨跌(元)
                    'change_pct': fund_quotes_df['ChangePCT'],               # 涨跌幅(%)
                    'turnover_rate': fund_quotes_df['TurnoverRate'],         # 换手率(%)
                    'turnover_volume': fund_quotes_df['TurnoverVolume'],     # 成交量(万份)
                    'turnover_value': fund_quotes_df['TurnoverValue'],       # 成交额(元)
                    'vibration_range': fund_quotes_df['VibrationRange'],     # 振幅(%)
                    'discount': fund_quotes_df['Discount'],                  # 贴水(元)
                    'discount_ratio': fund_quotes_df['DiscountRatio'],       # 贴水率(%),
                    'fund_return': fund_quotes_df['ChangePCT'] / 100         # 收益率（小数）
                })

                # 保存为 CSV
                safe_fund_code = original_input.replace('.', '_')
                fund_output_path = os.path.join(self.data_dir, f"fund_{safe_fund_code}_return.csv")
                try:
                    fund_result_df.to_csv(fund_output_path, index=True)
                    self.logger.info(f"基金 {original_input} 行情数据已保存到 {fund_output_path}")
                    results[original_input] = fund_result_df
                except Exception as e:
                    self.logger.error(f"保存基金 {original_input} 数据失败: {str(e)}")


            return results


        except Exception as e:
            self.logger.error(f"获取A股指数权重和行情数据失败: {str(e)}")
            self.logger.error(traceback.format_exc())
            return {}


# 使用示例
if __name__ == "__main__":
    # 原主程序示例：获取指数权重和收益率
    data_dir = '/data/prod_data'
    jy_fetcher = DataFetcher_jy(data_dir=data_dir)
    start_date = '20091228'
    end_date = '20251231'
    index_codes = [
        # '000016.SH',
        # '000300.SH',
        '000905.SH',
        # '000852.SH',
        '000922.SH',
        # 'H00922.SH',
        # '932000.SH',
        # '980092.SH'
    ]
    results = jy_fetcher.fetch_a_index_weight_and_return(
        index_codes=index_codes,
        start_date=start_date,
        end_date=end_date
    )
    if results:
        for code, df in results.items():
            print(code, df.shape)
    jy_fetcher.close()

    # # 新测试程序：验证红利 ETF 行情抓取
    # data_dir = 'data'
    # jy_fetcher = DataFetcher_jy(data_dir=data_dir)
    # try:
    #     test_start = '20130101'
    #     test_end = datetime.today().strftime('%Y%m%d')

    #     # 新增测试：验证港股指数权重与收益率抓取
    #     hk_index_codes = ['HSI', 'I5025', 'SPAHLVCP', 'HSSCHKY']
    #     hk_results = jy_fetcher.fetch_hk_index_weight_and_return(
    #         index_codes=hk_index_codes,
    #         start_date=test_start,
    #         end_date=test_end
    #     )
    #     if hk_results:
    #         print("港股指数权重与收益率数据概览：")
    #         for code, df in hk_results.items():
    #             print(f"{code} -> 记录数: {len(df)}，日期范围: {df.index.min()} 至 {df.index.max()}")
    #     else:
    #         print("未获取到港股指数权重与收益率数据，请检查数据库配置或指数代码。")

    # etf_df = jy_fetcher.fetch_dividend_etf_data(start_date=test_start, end_date=test_end)
    # if etf_df.empty:
    #     print("未获取到红利 ETF 行情数据，请检查数据库连接或代码映射。")
    # else:
    #     print("红利 ETF 行情数据示例:")
    #     print(etf_df.head())
    #     print(f"共返回 {etf_df['secu_code'].nunique()} 只基金，{len(etf_df)} 条记录。")
    # finally:
    #     jy_fetcher.close()
