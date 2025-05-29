"""配置文件"""

# 数据库配置
DB_CONFIG = {
    'user': 'postgres',
    'password': 'VK2Yquru9gCi',
    'database': 'postgres',
    'host': '127.0.0.1',
    'min_size': 2,
    'max_size': 10
}

# 交易所API配置
EXCHANGE_CONFIG = {
    'okex': {
        'base_url': 'https://www.okx.com',
        'spot_endpoint': '/api/v5/market/history-candles',
        'futures_endpoint': '/api/v5/market/history-candles',
        'timeframe_map': {
            '1m': '1m', '5m': '5m', '15m': '15m',
            '1h': '1H', '4h': '4H', '1d': '1Dutc',
        }
    },
    'bybit': {
        'base_url': 'https://api.bybit.com',
        'spot_endpoint': '/v5/market/kline',
        'futures_endpoint': '/v5/market/kline',
        'timeframe_map': {
            '1m': '1', '5m': '5', '15m': '15',
            '1h': '60', '4h': '240', '1d': 'D'
        }
    },
    'binance': {
        'base_url': 'https://api.binance.com',
        'spot_endpoint': 'https://api.binance.com/api/v3/klines',
        'futures_endpoint': 'https://fapi.binance.com/fapi/v1/klines',
        'timeframe_map': {
            '1m': '1m', '5m': '5m', '15m': '15m',
            '1h': '1h', '4h': '4h', '1d': '1d'
        }
    }
}

# 默认下载配置
DEFAULT_DOWNLOAD_CONFIG = {
    'exchanges': ['binance'],
    'symbols': ['ETH/USDT'],
    'market_types': ['spot', 'futures'],
    'timeframe': '1d',
    'max_concurrent_tasks': 1
}
