"""交易所基类"""
import datetime
from abc import ABC, abstractmethod

from db import models
from utils import logger


class BaseExchange(ABC):
    """交易所基类"""

    def __init__(self, name):
        self.name = name

    @abstractmethod
    async def fetch_klines(self, symbol, timeframe, start_time, end_time, market_type):
        """获取K线数据"""
        pass

    async def download_data(self, symbol, timeframe, start_time, end_time, market_type='spot'):
        """下载并保存数据到数据库"""
        logger.info(f"下载 {self.name} {market_type} {symbol} 数据...")

        # 确保使用UTC时间
        if not start_time.tzinfo:
            start_time = start_time.replace(tzinfo=datetime.timezone.utc)
        if not end_time.tzinfo:
            end_time = end_time.replace(tzinfo=datetime.timezone.utc)

        # 转换为毫秒时间戳
        start_ts = int(start_time.timestamp() * 1000)
        end_ts = int(end_time.timestamp() * 1000)

        # 获取数据
        data = await self.fetch_klines(symbol, timeframe, start_ts, end_ts, market_type)

        if data:
            # 获取exchange_id和pair_id
            exchange_id = await models.get_exchange_id(self.name)
            if not exchange_id:
                logger.error(f"无法获取交易所ID: {self.name}")
                return

            base_asset, quote_asset = symbol.split('/')
            pair_id = await models.get_pair_id(exchange_id, symbol, market_type, base_asset, quote_asset)
            if not pair_id:
                logger.error(f"无法获取交易对ID: {symbol}")
                return

            # 插入数据
            inserted_count = await models.insert_kline_data(exchange_id, pair_id, timeframe, data)
            logger.info(f"成功插入 {inserted_count} 条数据到数据库")
        else:
            logger.warning(f"没有获取到 {self.name} {market_type} {symbol} 的数据")
