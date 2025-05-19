# /root/exchange/.venv/bin/python3 -m scripts.query
import asyncio
from datetime import datetime, timezone
from db.connection import db_manager

async def query_kline_data(exchange_name, timeframe, start_time, end_time):
    """
    查询K线数据
    
    :param exchange_name: 交易所名称
    :param timeframe: 时间周期 (例如 '1h', '4h', '1d')
    :param start_time: 开始时间 (UTC datetime 对象)
    :param end_time: 结束时间 (UTC datetime 对象)
    :return: 查询结果列表
    """
    try:
        await db_manager.create_pool()
        async with db_manager.pool.acquire() as conn:
            query = """
                    select *
                    from exchanges e,
                         kline_data kd,
                         trading_pairs tp
                    where e.exchange_id = kd.exchange_id
                      and kd.pair_id = tp.pair_id
                      and e.exchange_name = $1
                      and kd.timeframe = $2
                      and kd.close_time >= $3
                      and kd.close_time <= $4;
                    """
            rows = await conn.fetch(
                query,
                exchange_name,
                timeframe,
                start_time,
                end_time,
            )
            return rows
    except Exception as e:
        print(f"查询数据时发生错误: {str(e)}")
        return []
    finally:
        await db_manager.close_pool()

async def main():
    # 示例查询
    start_time = datetime.strptime("2025-05-18 00:00:00", "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime("2025-05-19 00:00:00", "%Y-%m-%d %H:%M:%S")
    
    results = await query_kline_data(
        exchange_name='binance',
        timeframe='1h',
        start_time=start_time,
        end_time=end_time,
    )
    
    # 打印结果
    for kline in results:
        kline_dict = dict(kline)
        print({k: str(v) for k, v in kline_dict.items()})
        print('-' * 50)  # 添加分隔线以提高可读性

if __name__ == '__main__':
    asyncio.run(main())

