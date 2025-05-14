"""主程序入口"""
import asyncio
import datetime
from datetime import timezone

from conf.config import DEFAULT_DOWNLOAD_CONFIG
from db.connection import db_manager
from exchanges import get_exchange
from utils import logger


async def download_with_error_handling(exchange, symbol, timeframe, start_time, end_time, market_type):
    """带错误处理的下载包装函数"""
    try:
        await exchange.download_data(
            symbol=symbol,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time,
            market_type=market_type
        )
    except Exception as e:
        logger.error(f"处理 {exchange.name} {market_type} {symbol} 时发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


async def run_download(config=None):
    """运行数据下载任务"""
    if config is None:
        config = DEFAULT_DOWNLOAD_CONFIG

    # 创建数据库连接池
    await db_manager.create_pool()

    try:
        exchanges_list = config.get('exchanges', DEFAULT_DOWNLOAD_CONFIG['exchanges'])
        symbols = config.get('symbols', DEFAULT_DOWNLOAD_CONFIG['symbols'])
        market_types = config.get('market_types', DEFAULT_DOWNLOAD_CONFIG['market_types'])
        timeframe = config.get('timeframe', DEFAULT_DOWNLOAD_CONFIG['timeframe'])
        max_concurrent = config.get('max_concurrent_tasks', DEFAULT_DOWNLOAD_CONFIG['max_concurrent_tasks'])

        start_time = config.get('start_time', datetime.datetime(2023, 1, 1, tzinfo=timezone.utc))
        end_time = config.get('end_time', datetime.datetime.now(timezone.utc))

        # 创建所有下载任务
        tasks = []
        for exchange_name in exchanges_list:
            exchange = get_exchange(exchange_name)
            for market_type in market_types:
                for symbol in symbols:
                    tasks.append(
                        download_with_error_handling(
                            exchange, symbol, timeframe,
                            start_time, end_time, market_type
                        )
                    )

        # 使用信号量限制并发数量
        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_download(coro):
            async with semaphore:
                return await coro

        # 执行所有任务并等待完成
        logger.info(f"开始执行 {len(tasks)} 个下载任务，最大并发数: {max_concurrent}")
        await asyncio.gather(*(bounded_download(task) for task in tasks))
        logger.info("所有下载任务已完成")

    finally:
        # 关闭连接池
        await db_manager.close_pool()


def main():
    """主函数"""
    logger.info("开始执行数据下载程序")

    # 自定义配置示例
    config = {
        'exchanges': ['bybit', 'okex', 'binance'],
        'symbols': ['ETH/USDT', 'BTC/USDT'],
        'market_types': ['spot', 'futures'],
        'timeframe': '1d',
        'start_time': datetime.datetime(2025, 5, 1, tzinfo=timezone.utc),
        'end_time': datetime.datetime(2025, 5, 31, tzinfo=timezone.utc),
        'max_concurrent_tasks': 10
    }

    asyncio.run(run_download(config))
    logger.info("程序执行完毕")


if __name__ == "__main__":
    main()
