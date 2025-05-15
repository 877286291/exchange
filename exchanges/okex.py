"""OKEx交易所实现"""
import time
from datetime import datetime
from typing import Dict, List, Tuple

import requests

from conf.config import EXCHANGE_CONFIG
from exchanges.base import BaseExchange
from utils import logger
from utils.helpers import format_symbol


class OKExExchange(BaseExchange):
    """OKEx交易所"""

    def __init__(self):
        super().__init__('okex')
        self.config = EXCHANGE_CONFIG['okex']
        self.base_url = self.config['base_url']
        self.session = requests.Session()
        self.rate_limit_delay = 0.5  # 请求间隔时间(秒)
        self.max_retries = 3  # 最大重试次数

    def _make_request(self, url: str, params: Dict) -> Tuple[bool, Dict]:
        """发送HTTP请求并处理常见错误"""
        retries = 0

        while retries < self.max_retries:
            try:
                response = self.session.get(url, params=params, timeout=10)

                if response.status_code == 429:  # 请求过于频繁
                    wait_time = self.rate_limit_delay * (2 ** retries)
                    logger.warning(f"请求频率限制，等待 {wait_time} 秒后重试")
                    time.sleep(wait_time)
                    retries += 1
                    continue

                if response.status_code != 200:
                    logger.error(f"请求失败: HTTP {response.status_code}, URL: {url}, 参数: {params}")
                    return False, {"error": f"HTTP错误: {response.status_code}"}

                data = response.json()
                if data.get('code') != '0':
                    logger.error(f"API返回错误: {data}, URL: {url}, 参数: {params}")
                    return False, data

                return True, data

            except requests.Timeout:
                logger.warning(f"请求超时，重试 ({retries+1}/{self.max_retries})")
                retries += 1
            except Exception as e:
                logger.error(f"请求异常: {str(e)}, URL: {url}, 参数: {params}")
                import traceback
                logger.error(traceback.format_exc())
                return False, {"error": str(e)}

            # time.sleep(self.rate_limit_delay)

        return False, {"error": "达到最大重试次数"}

    def fetch_klines(self, symbol: str, timeframe: str, start_time: int, end_time: int, market_type: str) -> List:
        """获取OKEx K线数据"""
        formatted_symbol = format_symbol('okex', symbol, market_type)
        tf = self.config['timeframe_map'][timeframe]
        endpoint = self.config['spot_endpoint'] if market_type == 'spot' else self.config['futures_endpoint']
        url = f"{self.base_url}{endpoint}"

        all_data = []
        current_time = end_time
        request_count = 0
        max_requests = 100  # 最大请求次数限制

        logger.info(f"开始获取OKEX {market_type} 数据: {symbol}, 时间范围: {datetime.fromtimestamp(start_time/1000)} - {datetime.fromtimestamp(end_time/1000)}")

        while request_count < max_requests:
            params = {
                'instId': formatted_symbol,
                'bar': tf,
                'after': str(current_time),
                'limit': '100'
            }

            success, data = self._make_request(url, params)
            if not success:
                break

            batch_data = data.get('data', [])
            if not batch_data:
                logger.info("没有更多数据")
                break

            # 过滤有效数据
            valid_data = [d for d in batch_data if start_time <= int(d[0]) <= end_time]
            if valid_data:
                all_data.extend(valid_data)
                logger.info(f"获取到 {len(valid_data)} 条有效数据")

            # 检查是否需要继续请求
            earliest_ts = int(batch_data[-1][0])
            if earliest_ts <= start_time or len(batch_data) < 100:
                break

            current_time = earliest_ts
            request_count += 1
            # time.sleep(self.rate_limit_delay)

        logger.info(f"OKEX {market_type} {symbol} 数据获取完成，共 {len(all_data)} 条")
        return all_data

    def get_symbols(self) -> Dict[str, List[str]]:
        """获取 OKX 的 USDT 交易对"""
        result = {'spot': [], 'perpetual': []}
        endpoint = "/api/v5/market/tickers"

        try:
            # 使用一个通用函数处理不同类型的交易对
            def fetch_symbols(inst_type, result_key):
                url = f"{self.base_url}{endpoint}"
                params = {'instType': inst_type}

                success, data = self._make_request(url, params)
                if not success:
                    return

                symbols = []
                for ticker in data.get('data', []):
                    inst_id = ticker['instId']
                    if inst_type == 'SPOT':
                        parts = inst_id.split('-')
                        if len(parts) == 2 and parts[1] == 'USDT':
                            symbols.append(parts[0])
                    elif inst_type == 'SWAP':
                        if '-USDT-' in inst_id:  # 只获取USDT合约
                            base = inst_id.split('-')[0]
                            symbols.append(base)

                # 去重并排序
                result[result_key] = sorted(list(set(symbols)))
                logger.info(f"OKEX {inst_type} USDT 交易对数量: {len(result[result_key])}")

            # 顺序获取现货和永续合约交易对
            fetch_symbols('SPOT', 'spot')
            fetch_symbols('SWAP', 'perpetual')

        except Exception as e:
            logger.error(f"获取 OKEX 交易对时发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

        return result

    def close(self):
        """关闭会话"""
        if self.session:
            self.session.close()
            logger.debug("OKEX HTTP会话已关闭")

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()