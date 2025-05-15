"""交易所模块"""
from exchanges.binance import BinanceExchange
from exchanges.bybit import BybitExchange
from exchanges.okex import OKExExchange


def get_exchange(name):
    """获取交易所实例"""
    exchanges = {
        'binance': BinanceExchange,
        'okex': OKExExchange,
        'bybit': BybitExchange,
    }

    exchange_class = exchanges.get(name.lower())
    if not exchange_class:
        raise ValueError(f"不支持的交易所: {name}")

    return exchange_class()
