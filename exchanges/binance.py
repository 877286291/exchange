"""Binance交易所实现"""
from typing import Dict, List, Optional, Any

import requests

from conf.config import EXCHANGE_CONFIG
from exchanges.base import BaseExchange
from utils import logger


class BinanceExchange(BaseExchange):
    """Binance交易所"""

    def __init__(self):
        super().__init__('binance')
        self.config = EXCHANGE_CONFIG['binance']
        self.spot_endpoint = self.config['spot_endpoint']
        self.futures_endpoint = self.config['futures_endpoint']
        self.session = requests.Session()

    def fetch_klines(self, symbol: str, timeframe: str, start_time: int, end_time: int, market_type: str) -> List:
        """获取Binance K线数据"""
        try:
            endpoint = self.spot_endpoint if market_type == 'spot' else self.futures_endpoint

            formatted_symbol = symbol.replace('/', '')

            # 获取对应的时间周期格式
            tf = self.config['timeframe_map'][timeframe]

            logger.info(f"获取 Binance {market_type} {symbol} 数据，时间范围: "
                        f"{start_time} - {end_time}")

            params = {
                'symbol': formatted_symbol,
                'interval': tf,
                'startTime': start_time,
                'endTime': end_time,
                'limit': 1000
            }

            logger.info(f"请求URL: {endpoint}")
            logger.info(f"请求参数: {params}")

            response = self.session.get(endpoint, params=params)
            response.raise_for_status()
            data = response.json()

            if data:
                logger.info(f"获取到 {len(data)} 条 Binance {market_type} {symbol} 数据")

                return data
            else:
                logger.warning(f"Binance没有返回 {symbol} 的数据")
                return []

        except Exception as e:
            logger.error(f"获取Binance数据时发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    def get_symbols(self) -> Dict[str, List[str]]:
        """获取 Binance 的 USDT 交易对"""
        result = {'spot': [], 'perpetual': []}

        try:
            # 获取现货交易对
            spot_url = "https://api.binance.com/api/v3/exchangeInfo"
            spot_response = self.session.get(spot_url)
            spot_response.raise_for_status()
            spot_info = spot_response.json()

            if spot_info and 'symbols' in spot_info:
                for symbol in spot_info['symbols']:
                    if (symbol['status'] == 'TRADING' and
                            symbol['quoteAsset'] == 'USDT'):  # 只获取USDT交易对
                        result['spot'].append(symbol['baseAsset'])

                logger.info(f"Binance 现货 USDT 交易对数量: {len(result['spot'])}")

            # 获取永续合约交易对
            futures_url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
            futures_response = self.session.get(futures_url)
            futures_response.raise_for_status()
            futures_info = futures_response.json()

            if futures_info and 'symbols' in futures_info:
                for symbol in futures_info['symbols']:
                    if (symbol['status'] == 'TRADING' and
                            symbol['contractType'] == 'PERPETUAL' and
                            symbol['quoteAsset'] == 'USDT'):
                        result['perpetual'].append(symbol['baseAsset'])

                logger.info(f"Binance 永续合约 USDT 交易对数量: {len(result['perpetual'])}")

        except Exception as e:
            logger.error(f"获取 Binance 交易对时发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())

        return result
