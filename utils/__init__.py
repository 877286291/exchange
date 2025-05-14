# 创建默认日志记录器
from datetime import datetime

from utils.logging import setup_logger

logger = setup_logger('exchange_data', f'exchange_data_{datetime.now().strftime("%Y%m%d")}.log')
