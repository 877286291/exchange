"""主程序入口"""
import asyncio
import datetime
import multiprocessing
import os
import platform
import time
from datetime import timezone
from functools import partial

from apscheduler.schedulers.background import BackgroundScheduler

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
        logger.error(f"处理 {exchange.name} {market_type} {symbol} {timeframe} 时发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


async def process_exchange(exchange_name, config):
    """处理单个交易所的所有下载任务"""
    # 创建数据库连接池
    await db_manager.create_pool()

    try:
        market_types = config.get('market_types', DEFAULT_DOWNLOAD_CONFIG['market_types'])
        timeframes = config.get('timeframes', DEFAULT_DOWNLOAD_CONFIG['timeframe'])
        max_concurrent = config.get('max_concurrent_tasks', DEFAULT_DOWNLOAD_CONFIG['max_concurrent_tasks'])
        start_time = config.get('start_time', datetime.datetime(2023, 1, 1, tzinfo=timezone.utc))
        end_time = config.get('end_time', datetime.datetime.now(timezone.utc))

        logger.info(f"进程 {os.getpid()} 开始处理交易所: {exchange_name}")

        # 获取交易所实例
        exchange = get_exchange(exchange_name)

        symbols_dict = exchange.get_symbols()
        logger.info(f"{exchange_name} 获取到 {sum(len(v) for v in symbols_dict.values())} 个交易对")

        # 创建下载任务
        tasks = []
        for market_type in market_types:
            symbol_key = 'perpetual' if market_type == 'futures' else market_type

            for symbol in symbols_dict.get(symbol_key, []):
                for timeframe in timeframes:
                    tasks.append(
                        download_with_error_handling(
                            exchange, f"{symbol}/USDT", timeframe,
                            start_time, end_time, market_type
                        )
                    )

        # 使用信号量限制并发数量
        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_download(coro):
            async with semaphore:
                return await coro

        # 执行所有任务并等待完成
        logger.info(f"{exchange_name} 开始执行 {len(tasks)} 个下载任务，最大并发数: {max_concurrent}")
        await asyncio.gather(*(bounded_download(task) for task in tasks))
        logger.info(f"{exchange_name} 所有下载任务已完成")

    except Exception as e:
        logger.error(f"处理交易所 {exchange_name} 时发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())

    finally:
        # 关闭连接池
        await db_manager.close_pool()


def run_exchange_process(exchange_name, config):
    """在单独的进程中运行交易所处理函数"""
    try:
        logger.info(f"启动进程处理交易所: {exchange_name}")
        asyncio.run(process_exchange(exchange_name, config))
        return f"{exchange_name} 处理完成"
    except Exception as e:
        logger.error(f"进程 {exchange_name} 发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return f"{exchange_name} 处理失败: {str(e)}"


def run_download(config=None):
    """运行数据下载任务，使用多进程并行处理不同交易所"""
    if config is None:
        config = DEFAULT_DOWNLOAD_CONFIG

    exchanges_list = config.get('exchanges', DEFAULT_DOWNLOAD_CONFIG['exchanges'])
    logger.info(f"开始处理 {len(exchanges_list)} 个交易所: {', '.join(exchanges_list)}")

    # 获取时间周期列表
    timeframes = config.get('timeframes', [DEFAULT_DOWNLOAD_CONFIG['timeframe']])
    logger.info(f"将下载以下时间周期的数据: {', '.join(timeframes)}")

    # 确定进程数量，不超过CPU核心数和交易所数量
    cpu_count = multiprocessing.cpu_count()
    process_count = min(len(exchanges_list), cpu_count)
    logger.info(f"系统有 {cpu_count} 个CPU核心，将使用 {process_count} 个进程")

    # 创建进程池
    with multiprocessing.Pool(processes=1) as pool:
        # 使用偏函数固定config参数
        process_func = partial(run_exchange_process, config=config)

        # 提交所有任务到进程池
        results = pool.map(process_func, exchanges_list)

        # 输出结果
        for result in results:
            logger.info(result)


async def get_exchanges_from_db():
    """从数据库获取所有可用的交易所信息"""
    try:
        await db_manager.create_pool()
        async with db_manager.pool.acquire() as conn:
            query = "SELECT exchange_name FROM exchanges"
            rows = await conn.fetch(query)
            exchanges = [row['exchange_name'] for row in rows]
            logger.info(f"从数据库获取到 {len(exchanges)} 个交易所: {', '.join(exchanges)}")
    except Exception as e:
        logger.error(f"从数据库获取交易所信息失败: {str(e)}")
        # 如果数据库查询失败，使用默认值
        exchanges = ['okex', 'binance', 'bybit']
        logger.warning(f"使用默认交易所列表: {', '.join(exchanges)}")
    finally:
        await db_manager.close_pool()
    return exchanges
    # return ['binance']


def scheduled_job():
    """每日执行的定时任务"""
    logger.info("开始执行每日数据下载任务")

    # 设置今天的日期范围
    today = datetime.datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday = today - datetime.timedelta(days=1)

    config = {
        'market_types': ['spot', 'futures'],
        'timeframes': ['15m', '1h', '4h', '1d'],
        'start_time': yesterday,
        'end_time': today,
        'max_concurrent_tasks': 10
    }

    # 从数据库获取交易所列表
    exchanges = asyncio.run(get_exchanges_from_db())
    config['exchanges'] = exchanges

    run_download(config)
    logger.info("每日数据下载任务执行完毕")


def main():
    """主函数"""
    logger.info("启动定时任务程序")

    # 创建后台调度器
    scheduler = BackgroundScheduler()

    # 设置每天0点运行任务
    scheduler.add_job(
        scheduled_job,
        trigger="interval",
        minutes=15,
        id='daily_download',
        name='每日数据下载任务'
    )

    # 启动调度器
    scheduler.start()
    logger.info("调度器已启动，任务将在每天 00:00 执行")

    # 立即运行一次任务（可选）
    scheduled_job()

    try:
        # 保持主线程运行
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        # 关闭调度器
        scheduler.shutdown()
        logger.info("调度器已关闭")


if __name__ == "__main__":
    # 设置多进程启动方法
    if platform.system() == 'Windows':
        multiprocessing.set_start_method('spawn', force=True)
    main()
