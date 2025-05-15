"""Bybit交易所实现"""
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

import requests

from conf.config import EXCHANGE_CONFIG
from exchanges.base import BaseExchange
from utils import logger
from utils.helpers import format_symbol


class BybitExchange(BaseExchange):
    """Bybit交易所"""

    def __init__(self):
        super().__init__('bybit')
        self.config = EXCHANGE_CONFIG['bybit']
        self.base_url = self.config['base_url']
        self.session = requests.Session()
        self.rate_limit_delay = 0.5  # 请求间隔时间(秒)
        self.max_retries = 3  # 最大重试次数

    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Optional[Dict]:
        """发送API请求并处理响应"""
        url = f"{self.base_url}{endpoint}"
        retries = 0

        while retries < self.max_retries:
            try:
                response = self.session.get(url, params=params, timeout=10)
                logger.info(f"响应状态码: {response.status_code}")

                if response.status_code == 429:  # 请求过于频繁
                    wait_time = self.rate_limit_delay * (2 ** retries)
                    logger.warning(f"请求频率限制，等待 {wait_time} 秒后重试")
                    time.sleep(wait_time)
                    retries += 1
                    continue

                if response.status_code != 200:
                    logger.error(f"请求失败: HTTP {response.status_code}")
                    return None

                data = response.json()

                if data['retCode'] != 0:
                    logger.error(f"Bybit API错误: {data}")
                    return None

                return data

            except requests.Timeout:
                logger.warning(f"请求超时，重试 ({retries+1}/{self.max_retries})")
                retries += 1
            except Exception as e:
                logger.error(f"请求过程中发生错误: {str(e)}")
                logger.error(f"请求URL: {url}")
                logger.error(f"请求参数: {params}")
                import traceback
                logger.error(traceback.format_exc())
                return None

            # time.sleep(self.rate_limit_delay)

        logger.error(f"达到最大重试次数 ({self.max_retries})")
        return None

    def fetch_klines(self, symbol: str, timeframe: str, start_time: int, end_time: int, market_type: str) -> List:
        """获取Bybit K线数据"""
        formatted_symbol = format_symbol('bybit', symbol, market_type)

        logger.info(f"开始获取Bybit {market_type} {symbol} 数据，时间范围: "
                    f"{datetime.fromtimestamp(start_time/1000)} - {datetime.fromtimestamp(end_time/1000)}")

        all_data = []
        current_start = start_time
        max_limit = 500  # Bybit最大允许获取500条K线数据

        while current_start < end_time:
            params = {
                'category': 'linear' if market_type == 'futures' else 'spot',
                'symbol': formatted_symbol,
                'interval': self.config['timeframe_map'][timeframe],
                'start': current_start,
                'end': min(current_start + (max_limit * 60 * 60 * 1000), end_time),  # 限制每次请求的时间范围
                'limit': max_limit
            }

            data = self._make_request(self.config['spot_endpoint'], params)

            if not data or 'result' not in data or 'list' not in data['result']:
                logger.warning(f"没有获取到数据或数据格式错误")
                break

            klines = data['result']['list']
            if not klines:
                logger.info("没有更多数据")
                break

            all_data.extend(klines)
            logger.info(f"获取到 {len(klines)} 条数据")

            if len(klines) < max_limit:
                break

            # 更新开始时间
            # Bybit返回的时间戳通常是字符串格式的毫秒时间戳
            last_timestamp = int(klines[-1][0])  # 假设时间戳在第一个位置
            current_start = last_timestamp + 1

            # 避免请求过于频繁
            # time.sleep(self.rate_limit_delay)

        logger.info(f"Bybit {market_type} {symbol} 数据获取完成，共 {len(all_data)} 条")
        return all_data

    def get_symbols(self) -> Dict[str, List[str]]:
        """获取 Bybit 的 USDT 交易对"""
        result = {'spot': [], 'perpetual': []}
        endpoint = "/v5/market/tickers"

        # 获取现货交易对
        spot_data = self._make_request(endpoint, {'category': 'spot'})
        if spot_data and 'result' in spot_data and 'list' in spot_data['result']:
            for ticker in spot_data['result']['list']:
                symbol = ticker['symbol']
                if symbol.endswith('USDT'):
                    base = symbol[:-4]  # 移除 USDT 后缀
                    result['spot'].append(base)

            # 去重并排序
            result['spot'] = sorted(list(set(result['spot'])))
            logger.info(f"Bybit 现货 USDT 交易对数量: {len(result['spot'])}")

        # 获取永续合约交易对
        perpetual_data = self._make_request(endpoint, {'category': 'linear'})
        if perpetual_data and 'result' in perpetual_data and 'list' in perpetual_data['result']:
            for ticker in perpetual_data['result']['list']:
                symbol = ticker['symbol']
                if symbol.endswith('USDT'):
                    base = symbol[:-4]  # 移除 USDT 后缀
                    result['perpetual'].append(base)

            # 去重并排序
            result['perpetual'] = sorted(list(set(result['perpetual'])))
            logger.info(f"Bybit 永续合约 USDT 交易对数量: {len(result['perpetual'])}")

        return result

    def close(self):
        """关闭会话"""
        if self.session:
            self.session.close()
            logger.debug("Bybit HTTP会话已关闭")

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出"""
        self.close()