"""OKEx交易所实现"""
import time
from datetime import datetime

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

    async def fetch_klines(self, symbol, timeframe, start_time, end_time, market_type):
        """获取OKEx K线数据"""
        formatted_symbol = format_symbol('okex', symbol, market_type)
        tf = self.config['timeframe_map'][timeframe]

        all_data = []
        current_time = end_time
        request_count = 0

        logger.info(f"\n开始获取okex数据:")
        logger.info(f"起始UTC时间: {datetime.fromtimestamp(start_time / 1000)}")
        logger.info(f"结束UTC时间: {datetime.fromtimestamp(end_time / 1000)}")

        while True:
            params = {
                'instId': formatted_symbol,
                'bar': tf,
                'after': str(current_time),
                'limit': '100'
            }

            current_utc = datetime.fromtimestamp(current_time / 1000)
            logger.info(f"\n请求 #{request_count + 1}")
            logger.info(f"请求UTC时间: {current_utc}")
            logger.info(f"请求参数: {params}")

            url = f"{self.config['base_url']}{self.config['spot_endpoint']}"

            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()
                logger.info(f"响应状态码: {response.status_code}")

                if data['code'] == '0' and data['data']:
                    batch_data = data['data']
                    logger.info(f"获取到 {len(batch_data)} 条数据")

                    earliest_ts = int(batch_data[-1][0])

                    valid_data = [d for d in batch_data if start_time <= int(d[0]) <= end_time]
                    if valid_data:
                        all_data.extend(valid_data)

                    if earliest_ts <= start_time or len(batch_data) < 100:
                        break

                    current_time = earliest_ts
                    request_count += 1

                    if request_count >= 100:
                        logger.warning("达到最大请求次数限制，停止请求")
                        break

                else:
                    logger.error(f"API返回错误或空数据: {data}")
                    break

                time.sleep(0.5)

            except Exception as e:
                logger.error(f"获取OKEx数据时发生错误: {str(e)}")
                import traceback
                logger.error(traceback.format_exc())
                break

        logger.info("\n数据统计:")
        logger.info(f"总请求次数: {request_count}")
        logger.info(f"总数据条数: {len(all_data)}")

        return all_data
