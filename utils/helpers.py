"""辅助函数"""
import asyncio


def format_symbol(exchange, symbol, market_type):
    """格式化交易对名称"""
    base, quote = symbol.split('/')

    if exchange == 'okex':
        if market_type == 'futures':
            return f"{base}-{quote}-SWAP"
        return f"{base}-{quote}"
    elif exchange == 'bybit':
        if market_type == 'futures':
            return f"{base}{quote}"  # Bybit linear futures 不需要特殊格式
        return f"{base}{quote}"
    elif exchange == 'binance':
        return f"{base}{quote}"

    return symbol


async def fetch_with_retry(fetch_func, *args, max_retries=3, **kwargs):
    """带重试机制的数据获取函数"""
    retries = 0
    while retries < max_retries:
        try:
            return await fetch_func(*args, **kwargs)
        except Exception as e:
            retries += 1
            if retries >= max_retries:
                print(f"达到最大重试次数 {max_retries}，操作失败: {str(e)}")
                raise
            wait_time = 2 ** retries  # 指数退避策略
            print(f"操作失败，{wait_time}秒后重试 ({retries}/{max_retries}): {str(e)}")
            await asyncio.sleep(wait_time)
    return None
