"""Binance交易所实现"""
import ccxt

from exchanges.base import BaseExchange
from utils import logger


class BinanceExchange(BaseExchange):
    """Binance交易所"""

    def __init__(self):
        super().__init__('binance')
        self.spot_client = ccxt.binance()
        self.futures_client = ccxt.binance({'options': {'defaultType': 'future'}})

    async def fetch_klines(self, symbol, timeframe, start_time, end_time, market_type):
        """获取Binance K线数据"""
        try:
            exchange = self.futures_client if market_type == 'futures' else self.spot_client

            logger.info(f"获取 Binance {market_type} {symbol} 数据，时间范围: "
                        f"{start_time} - {end_time}")

            ohlcv = exchange.fetch_ohlcv(
                symbol=symbol,
                timeframe=timeframe,
                since=start_time,
                limit=1000
            )

            if ohlcv:
                filtered_data = [candle for candle in ohlcv if start_time <= candle[0] <= end_time]
                logger.info(f"获取到 {len(filtered_data)} 条 Binance {market_type} {symbol} 数据")
                return filtered_data
            else:
                logger.warning(f"Binance没有返回 {symbol} 的数据")
                return []

        except Exception as e:
            logger.error(f"获取Binance数据时发生错误: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
