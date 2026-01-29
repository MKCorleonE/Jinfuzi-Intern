import os
import tempfile
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Tuple, Any, Callable
import time
import pymysql
from sqlalchemy import create_engine
import logging
import traceback
import sys
from tqdm import tqdm

LOGS_DIR = "./logs"


class DataFetcher:
    """数据获取器

    主要功能:
    1. 获取交易日历数据
    2. 获取行情数据
    3. 获取指数数据
    4. 获取指数权重数据
    """

    # 类属性定义 (排除非交易日期)
    HK_SPECIAL_DATES = [
        pd.to_datetime('2023-07-17'),
        pd.to_datetime('2023-09-01'),
        pd.to_datetime('2023-09-08')
    ]

    def __init__(self, data_dir: str, max_retries: int = 3):
        """初始化数据获取器

        Args:
            data_dir: 数据保存目录
            max_retries: 最大重试次数
        """
        # 基础配置
        self.data_dir = data_dir
        self.max_retries = max_retries

        # 设置日志
        self._setup_logger()

        # 创建数据目录
        os.makedirs(data_dir, exist_ok=True)

        # 数据库配置
        self.db_config = {
            'user': 'reader',
            'password': '1qazcde3%TGB',
            'host': '192.168.20.199',
            'port': 3306,
            'database': 'tongliandb'
        }

        # 创建数据库连接
        self._setup_database()

        # 字段映射配置
        self.field_mapping = {
            'open': 'OPEN_PRICE',
            'high': 'HIGHEST_PRICE',
            'low': 'LOWEST_PRICE',
            'close': 'CLOSE_PRICE',
            'pre_close': 'PRE_CLOSE_PRICE',
            'volume': 'TURNOVER_VOL',
            'amount': 'TURNOVER_VALUE',
            'deal': 'DEAL_AMOUNT',
            'pe': 'PE',
            'pe1': 'PE1',
            'neg_market_value': 'NEG_MARKET_VALUE',
            'market_value': 'MARKET_VALUE',
            'total_ret': 'CHG_PCT',
            'turnover': 'TURNOVER_RATE',
            'pb': 'PB'
        }

        # 添加港股字段映射
        self.hk_field_mapping = {
            'open': 'OPEN_PRICE',
            'high': 'HIGHEST_PRICE',
            'low': 'LOWEST_PRICE',
            'close': 'CLOSE_PRICE',
            'pre_close': 'PRE_CLOSE_PRICE',
            'volume': 'TURNOVER_VOL',
            'amount': 'TURNOVER_VALUE',
            'pe': 'PE',
            'pe1': 'PE1',
            'neg_market_value': 'NEG_MARKET_VALUE',
            'market_value': 'MARKET_VALUE',
            'total_ret': 'CHG_PCT',
            'pb': 'PB'
        }

    def _setup_logger(self):
        """配置日志"""
        self.logger = logging.getLogger('DataFetcher')
        self.logger.setLevel(logging.INFO)

        # 创建控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # 创建文件处理器
        log_file = os.path.join(LOGS_DIR, "data_fetcher.log")

        # 确保日志目录存在
        os.makedirs(LOGS_DIR, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)

        # 设置日志格式
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # 添加处理器
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)

    def _setup_database(self):
        """设置数据库连接"""
        try:
            self.engine = create_engine(
                f"mysql+pymysql://{self.db_config['user']}:{self.db_config['password']}@"
                f"{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            )
            self.logger.info("数据库连接创建成功")
        except Exception as e:
            self.logger.error(f"数据库连接创建失败: {str(e)}")
            raise

    def _execute_query(self, query: str, params: tuple) -> pd.DataFrame:
        """执行SQL查询

        Args:
            query: SQL查询语句
            params: 查询参数

        Returns:
            查询结果DataFrame
        """
        try:
            import time as _t
            t0 = _t.time()
            # 使用流式结果，先返回首批行，降低等待时间
            with self.engine.connect().execution_options(stream_results=True) as conn:
                chunks = pd.read_sql(
                    query,
                    conn,
                    params=params,
                    chunksize=500_000,
                    coerce_float=True,
                )
                frames = []
                n = 0
                first_chunk_s = None
                num_chunks = 0
                for chk in chunks:
                    num_chunks += 1
                    if first_chunk_s is None:
                        first_chunk_s = _t.time() - t0
                    n += len(chk)
                    frames.append(chk)
                df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                t_all = _t.time() - t0
                self.logger.info(
                    f"SQL fetched {n:,} rows in {t_all:.2f}s (first {first_chunk_s or 0:.2f}s, chunks={num_chunks})"
                )
                return df
        except Exception as e:
            self.logger.error(f"执行查询失败: {str(e)}")
            return pd.DataFrame()

    def trade_cal(self, exchange: str = 'XSHG', start_date: str = None,
                  end_date: str = None, is_open: str = '1') -> pd.DataFrame:
        """获取交易日历 (修改为CSV读写)"""
        # 尝试从本地读取数据
        file_path = os.path.join(self.data_dir, 'trade_cal.csv')
        df = pd.DataFrame()

        try:
            if os.path.exists(file_path):
                self.logger.info("从本地读取交易日历数据...")
                # 使用 CSV 读取，并指定日期列
                df = pd.read_csv(file_path, index_col='trade_date', parse_dates=['trade_date'])
                df = df.reset_index()  # 确保 trade_date 仍是列，以便后续处理
                if not df.empty:
                    self.logger.info(
                        f"成功读取本地交易日历数据，日期范围: {df['trade_date'].min()} - {df['trade_date'].max()}")
        except Exception as e:
            self.logger.warning(f"读取本地交易日历数据失败: {str(e)}")
            df = pd.DataFrame()

        # 如果本地数据不存在或为空，从数据库获取
        if df.empty:
            self.logger.info("从数据库获取交易日历数据...")
            query = """
                SELECT 
                    CALENDAR_DATE as trade_date,
                    EXCHANGE_CD as exchange,
                    IS_OPEN as is_open
                FROM md_trade_cal
                """

            # 执行查询获取所有数据
            df = self._execute_query(query, params=())

            if df.empty:
                self.logger.warning("未查询到交易日历数据")
                return pd.DataFrame()

            # 转换日期格式
            df['trade_date'] = pd.to_datetime(df['trade_date'])

            # 根据trade_date排序
            df = df.sort_values('trade_date').reset_index(drop=True)

            # 保存完整的交易日历数据到本地 (CSV保存)
            try:
                os.makedirs(self.data_dir, exist_ok=True)
                df.to_csv(file_path, index=False)
                self.logger.info(f"完整交易日历数据已保存到: {file_path}")
            except Exception as e:
                self.logger.error(f"保存交易日历数据到本地失败: {str(e)}")

        # 根据条件筛选数据
        result_df = df.copy()

        # 筛选交易所
        if exchange:
            result_df = result_df[result_df['exchange'] == exchange]

        # 筛选是否开市
        if is_open:
            # 将is_open转换为整数进行比较
            result_df = result_df[result_df['is_open'] == int(is_open)]

        # 设置日期索引
        result_df = result_df.set_index('trade_date')

        # 筛选日期范围
        if start_date:
            result_df = result_df[result_df.index >= pd.to_datetime(start_date)]
        if end_date:
            result_df = result_df[result_df.index <= pd.to_datetime(end_date)]

        if not result_df.empty:
            self.logger.info(f"筛选后的交易日历数据范围: {result_df.index.min()} - {result_df.index.max()}")
            self.logger.info(f"交易日数量: {len(result_df)}")
        else:
            self.logger.warning("筛选后无符合条件的交易日历数据")

        return result_df

    # ===================== 工具方法 =====================

    def _get_table_columns(self, table_name: str) -> List[str]:
        """Return column names of a MySQL table in current database."""
        try:
            sql = (
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
            )
            cols = pd.read_sql(sql, self.engine, params=(self.db_config['database'], table_name))
            return cols['COLUMN_NAME'].tolist()
        except Exception as e:
            self.logger.warning(f"无法获取表{table_name}的列信息: {e}")
            return []

    def _get_numeric_columns(self, table_name: str, exclude: Optional[List[str]] = None) -> List[str]:
        """Return numeric column names of a table, excluding keys/index-like columns."""
        exclude = set((exclude or []))
        numeric_types = {
            'tinyint', 'smallint', 'mediumint', 'int', 'bigint',
            'float', 'double', 'decimal', 'numeric', 'real'
        }
        try:
            sql = (
                "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
            )
            df = pd.read_sql(sql, self.engine, params=(self.db_config['database'], table_name))
            cols = [
                c for c, t in zip(df['COLUMN_NAME'], df['DATA_TYPE'])
                if t.lower() in numeric_types and c not in exclude
            ]
            return cols
        except Exception as e:
            self.logger.warning(f"无法获取表{table_name}的数值列信息，将退回所有列: {e}")
            # Fallback: return all columns minus exclude; caller can still succeed
            all_cols = self._get_table_columns(table_name)
            return [c for c in all_cols if c not in exclude]

    def _validate_date(self, date_str: str) -> bool:
        """验证日期是否有效"""
        try:
            date = pd.to_datetime(date_str)
            return '19900101' <= date_str <= datetime.now().strftime('%Y%m%d')
        except:
            return False

    def _get_file_path(self, data_type: str, field: str = None) -> str:
        """获取数据文件路径 (CSV格式)"""
        if field:
            return os.path.join(self.data_dir, f"{data_type}_{field}.csv")
        return os.path.join(self.data_dir, f"{data_type}.csv")

    def _is_special_date(self, date: pd.Timestamp, market: str) -> bool:
        """检查是否为特殊日期（如港股台风天等）"""
        if market == 'HK':
            return date in self.HK_SPECIAL_DATES
        return False

    def _read_existing_data(self, file_path: str) -> pd.DataFrame:
        """读取本地已有的处理数据 (CSV读取)

        Args:
            file_path: 数据文件路径

        Returns:
            DataFrame: 已有的处理数据
        """
        if os.path.exists(file_path):
            try:
                # 假设索引是第一列，并解析日期
                return pd.read_csv(file_path, index_col=0, parse_dates=True)
            except Exception as e:
                self.logger.error(f"读取数据失败: {str(e)}")

        return pd.DataFrame()

    def _get_missing_dates(self, required_dates: pd.DatetimeIndex,
                           existing_data: pd.DataFrame,
                           market: str = None) -> pd.DatetimeIndex:
        """获取缺失的日期"""
        if existing_data.empty:
            missing_dates = required_dates
            print("本地无数据，需要获取完整日期范围的数据")
        else:
            missing_dates = required_dates.difference(existing_data.index)

        # 从缺失日期中移除特殊日期
        if market == 'HK':
            original_len = len(missing_dates)
            missing_dates = missing_dates[~missing_dates.isin(self.HK_SPECIAL_DATES)]
            if original_len != len(missing_dates):
                print(f"已从缺失日期中移除 {original_len - len(missing_dates)} 个特殊日期")

        return missing_dates

    def _process_data_update(self, missing_dates: pd.DatetimeIndex,
                             fetch_func: callable,
                             save_params: dict,
                             batch_size: int = 10,
                             sleep_time: float = 0.5) -> None:
        """处理数据更新的通用流程 (此函数已不再使用复杂的HDF5合并逻辑)"""
        raise NotImplementedError("此方法不再适用，请直接调用 data_fetcher.py 中的数据获取函数。")

    def _get_required_dates(self, trade_dates: pd.DatetimeIndex,
                            start_date: str, end_date: str) -> pd.DatetimeIndex:
        """获取需要的日期范围"""
        mask = (trade_dates >= pd.to_datetime(start_date)) & (trade_dates <= pd.to_datetime(end_date))
        return trade_dates[mask]

    # ===================== 行情数据相关 =====================

    def fetch_daily_data(self, start_date: str, end_date: str, fields: Union[str, List[str]] = None) -> Dict[
        str, pd.DataFrame]:
        """获取A股日频行情数据，支持一次性获取多个字段，并增量保存数据 (修改为CSV保存)"""
        # 字段处理
        if fields is None:
            fields_to_fetch = list(self.field_mapping.keys())
        elif isinstance(fields, str):
            fields_to_fetch = [fields]
        else:
            fields_to_fetch = fields

        # 字段检查
        invalid_fields = [f for f in fields_to_fetch if f not in self.field_mapping]
        if invalid_fields:
            self.logger.error(f"不支持的数据字段: {invalid_fields}")
            return {}

        # 构建查询
        field_selects = [
            f"{self.field_mapping[f]} as {f}"
            for f in fields_to_fetch
        ]

        query = f"""
            SELECT 
                TRADE_DATE as trade_date,
                TICKER_SYMBOL as stock_code,
                {', '.join(field_selects)}
            FROM mkt_equd 
            WHERE TRADE_DATE BETWEEN %s AND %s
        """

        try:
            # 执行查询
            df = self._execute_query(query, (start_date, end_date))

            if df.empty:
                self.logger.warning("未找到符合条件的行情数据")
                return {}

            # 数据处理
            df['trade_date'] = pd.to_datetime(df['trade_date'])

            # 为每个字段创建透视表并保存
            result = {}
            for field in fields_to_fetch:
                try:
                    # 创建透视表
                    pivot_df = df.pivot(
                        index='trade_date',
                        columns='stock_code',
                        values=field
                    )

                    # 数据类型转换和清理
                    if field in ['volume', 'amount', 'deal_amount']:
                        pivot_df = pivot_df.astype('float64')
                    elif field in ['pe', 'pe1', 'pb']:
                        pivot_df = pivot_df.replace([np.inf, -np.inf], np.nan)

                    # 读取现有数据
                    file_path = self._get_file_path("mkt_equd", field)  # e.g. mkt_equd_close.csv
                    existing_data = self._read_existing_data(file_path)

                    # 合并数据: CSV模式下，直接合并新旧数据，并覆盖保存
                    if not existing_data.empty:
                        # 使用 combine_first 来实现新数据覆盖旧数据，并保留非重叠部分
                        merged_data = pivot_df.combine_first(existing_data)
                    else:
                        merged_data = pivot_df

                    # 按照索引日期排序
                    merged_data = merged_data.sort_index()

                    # 保存数据 (CSV保存)
                    os.makedirs(self.data_dir, exist_ok=True)
                    merged_data.to_csv(file_path, index=True)

                    result[field] = merged_data

                    self.logger.info(f"成功处理{field}数据，并保存到: {file_path}")
                    self.logger.info(f"- 数据维度: {merged_data.shape}")
                    self.logger.info(
                        f"- 日期范围: {merged_data.index.min().strftime('%Y-%m-%d')} - {merged_data.index.max().strftime('%Y-%m-%d')}")
                except Exception as e:
                    self.logger.error(f"处理{field}数据时发生错误: {str(e)}")
                    continue

            return result

        except Exception as e:
            self.logger.error(f"获取行情数据失败: {str(e)}")
            return {}

    def fetch_missing_data(self, data_type: str, start_date: str = None, end_date: str = None) -> None:
        """检查并补齐本地数据的缺失日期 (针对CSV修改)"""
        # 定义数据类型到文件名的映射
        file_mapping = {
            'daily': 'mkt_equd_close.csv',  # 使用 close 字段的文件来判断缺失日期
            'hk_daily': 'mkt_hkequd_close.csv',
            # ... 其他类型的文件名保持一致
        }

        # 定义数据类型到交易所的映射
        exchange_mapping = {
            'daily': 'XSHG',
            'hk_daily': 'XHKG',
            # ...
        }

        if data_type not in file_mapping:
            print(f"不支持的数据类型: {data_type}")
            return

        file_name = file_mapping.get(data_type, f"{data_type}.csv")
        file_path = os.path.join(self.data_dir, file_name)

        # 如果文件不存在，直接获取整个时间段的数据
        if not os.path.exists(file_path):
            print(f"文件 {file_path} 不存在，将获取完整时间段的数据")
            if start_date is None:
                start_date = '20070101'  # 默认从2007年开始
            if end_date is None:
                end_date = datetime.now().strftime('%Y%m%d')

            self._fetch_data_by_type(data_type, start_date, end_date)
            return

        # 读取本地数据的日期索引
        try:
            local_data = self._read_existing_data(file_path)
            if local_data.empty:
                local_dates = pd.DatetimeIndex([])
            else:
                local_dates = local_data.index
        except Exception as e:
            print(f"读取本地数据失败: {str(e)}")
            return

        # 设置日期范围
        if local_dates.empty or start_date is None:
            # 如果本地数据为空，默认从2007年开始
            if start_date is None:
                start_date = '20070101'
        elif start_date is None:
            # 如果start_date为None，使用本地数据的最早日期
            start_date = local_dates.min().strftime('%Y%m%d')

        if end_date is None:
            end_date = datetime.now().strftime('%Y%m%d')

        # 获取对应市场的交易日历
        exchange = exchange_mapping.get(data_type, 'XSHG')
        calendar = self.trade_cal(
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
            is_open='1'
        )
        if calendar.empty:
            print(f"获取{exchange}交易日历失败")
            return

        # 找出缺失的日期
        required_dates = pd.DatetimeIndex(calendar.index)
        missing_dates = required_dates.difference(local_dates)

        if missing_dates.empty:
            print(f"数据完整，无需补充")
            return

        print(f"\n发现缺失日期 {len(missing_dates)} 天")
        print(f"缺失日期范围: {missing_dates.min().strftime('%Y-%m-%d')} - {missing_dates.max().strftime('%Y-%m-%d')}")

        # 调用相应的数据获取方法
        self._fetch_data_by_type(data_type,
                                 missing_dates.min().strftime('%Y%m%d'),
                                 missing_dates.max().strftime('%Y%m%d'))

    # ... (其他未修改的方法省略，如 _fetch_data_by_type, get_actual_component 等)
    def _fetch_data_by_type(self, data_type, start_date=None, end_date=None):
        """根据数据类型获取数据

        Args:
            data_type: 数据类型
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            dict: 数据字典
        """
        fetch_functions = {
            'daily': self.fetch_daily_data,
            'hk_daily': self.fetch_hk_daily_data,
            # 'cashflow': self.fetch_cashflow_data,
            # 'cashflow_s': self.fetch_cashflow_s_data,
            # 'buyback': self.fetch_buyback_data,
            # 'balance': self.fetch_balance_data,
            # 'income': self.fetch_income_data,
            # 'shares': self.fetch_free_shares_data,
            # 'limit': self.fetch_limit_price_data,
            # 'dividend': self.fetch_dividend_data,
            # 'fst': self.fetch_fst_data,
            # 'con_sec_core': self.fetch_con_sec_core,
        }

        if data_type not in fetch_functions:
            self.logger.warning(f"不支持的数据类型: {data_type}")
            return {}

        # 注意: 这里的调用现在会直接在 fetch_daily_data 中处理数据合并和 CSV 保存
        return fetch_functions[data_type](start_date, end_date)

    def get_actual_component(self, index_code, start_date, end_date):
        new_db_config = {
            'user': 'reader',
            'password': '1qazcde3%TGB',
            'host': '192.168.20.195',
            'port': 3307,
            'database': 'jydb'
        }
        # 创建新的数据库连接
        new_engine = create_engine(
            f"mysql+pymysql://{new_db_config['user']}:{new_db_config['password']}@"
            f"{new_db_config['host']}:{new_db_config['port']}/{new_db_config['database']}")

        result_dict = {}

        for code in index_code:
            # 获取指数的 InnerCode
            index_query = """
                SELECT InnerCode 
                FROM secumain 
                WHERE SecuCode = %s 
                AND SecuCategory = 4  -- 指数类别
            """
            index_df = pd.read_sql(index_query, new_engine, params=(code,))
            if index_df.empty:
                print(f"未找到指数代码 {code} 对应的 InnerCode")
                result_dict[code] = pd.DataFrame()
                continue

            index_inner_code = index_df['InnerCode'].iloc[0]

            # 构建成分股查询
            query = """
                SELECT 
                    ic.InDate,
                    ic.OutDate,
                    sm.SecuCode
                FROM lc_indexcomponent ic
                JOIN secumain sm ON ic.SecuInnerCode = sm.InnerCode
                WHERE ic.IndexInnerCode = %s
                AND sm.SecuCategory = 1  -- 股票类别
                ORDER BY ic.InDate
            """
            components_df = pd.read_sql(query, new_engine, params=(index_inner_code,))

            if components_df.empty:
                print(f"未找到指数 {code} 的成分股数据")
                result_dict[code] = pd.DataFrame()
                continue

            # 转换日期列
            components_df['InDate'] = pd.to_datetime(components_df['InDate'])
            components_df['OutDate'] = pd.to_datetime(components_df['OutDate'])

            result_dict[code] = components_df


# 使用示例
if __name__ == "__main__":
    # # 添加项目根目录到系统路径
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.append(project_root)

    # 初始化数据获取器
    data_dir = '/data/prod_data'
    fetcher = DataFetcher(data_dir)

    # 设置日期范围
    from datetime import datetime, timedelta

    start_date = '2009-12-01'
    end_date = '2025-10-28'

    # # 也可以直接使用fetch_missing_data接口
    fetcher.fetch_missing_data('daily', start_date, end_date)