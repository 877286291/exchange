"""Bybit交易所实现"""
from datetime import datetime

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

    async def fetch_klines(self, symbol, timeframe, start_time, end_time, market_type):
        """获取Bybit K线数据"""
        formatted_symbol = format_symbol('bybit', symbol, market_type)

        logger.info(f"\n开始获取Bybit数据:")
        logger.info(f"市场类型: {market_type}")
        logger.info(f"原始交易对: {symbol}")
        logger.info(f"格式化后交易对: {formatted_symbol}")
        logger.info(f"时间戳范围: {start_time} - {end_time}")
        logger.info(
            f"对应UTC时间: {datetime.fromtimestamp(start_time / 1000)} - {datetime.fromtimestamp(end_time / 1000)}")

        params = {
            'category': 'linear' if market_type == 'futures' else 'spot',
            'symbol': formatted_symbol,
            'interval': self.config['timeframe_map'][timeframe],
            'start': start_time,
            'end': end_time,
            'limit': 500
        }

        url = f"{self.config['base_url']}{self.config['spot_endpoint']}"
        logger.info(f"\n请求详情:")
        logger.info(f"URL: {url}")
        logger.info(f"完整参数: {params}")

        try:
            response = requests.get(url, params=params)
            logger.info(f"\n响应状态码: {response.status_code}")
            logger.info(f"完整请求 URL: {response.url}")

            response.raise_for_status()
            data = response.json()

            if data['retCode'] == 0 and 'result' in data and 'list' in data['result']:
                klines = data['result']['list']
                logger.info(f"\n获取到 {len(klines)} 条 Bybit {market_type} {symbol} 数据")
                return klines
            else:
                logger.error(f"Bybit API错误: {data}")
                return []

        except Exception as e:
            logger.error(f"获取Bybit数据时发生错误: {str(e)}")
            logger.error(f"请求URL: {url}")
            logger.error(f"请求参数: {params}")
            import traceback
            logger.error(traceback.format_exc())
            return []
