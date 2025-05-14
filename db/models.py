"""数据库模型和操作"""
import datetime

from db.connection import db_manager


async def get_exchange_id(exchange_name):
    """从数据库的exchange表查询exchange_id"""
    async with db_manager.pool.acquire() as conn:
        query = "SELECT exchange_id FROM exchanges WHERE exchange_name = $1"
        exchange_id = await conn.fetchval(query, exchange_name)

        if exchange_id is None:
            print(f"警告：未找到交易所 '{exchange_name}' 的ID")
            return None

        return exchange_id


async def get_pair_id(exchange_id, symbol, market_type, base_asset, quote_asset):
    """从数据库的trading_pairs表查询pair_id"""
    async with db_manager.pool.acquire() as conn:
        query = "SELECT pair_id FROM trading_pairs WHERE exchange_id = $1 AND symbol = $2"
        pair_id = await conn.fetchval(query, exchange_id, symbol)

        if pair_id is None:
            print(f"警告：未找到交易对 '{symbol}' 的ID")
            insert_query = """
                           INSERT INTO trading_pairs
                               (exchange_id, symbol, market_type, base_asset, quote_asset)
                           VALUES ($1, $2, $3, $4, $5)
                           RETURNING pair_id \
                           """
            pair_id = await conn.fetchval(insert_query, exchange_id, symbol, market_type, base_asset, quote_asset)
            print(f"已创建新的交易对 '{symbol}'，ID为 {pair_id}")

        return pair_id


async def insert_kline_data(exchange_id, pair_id, timeframe, candles):
    """批量插入K线数据"""
    if not candles:
        return 0

    values = []
    for candle in candles:
        close_time = datetime.datetime.fromtimestamp(int(candle[0]) / 1000)
        values.append((
            exchange_id, pair_id, timeframe, close_time,
            float(candle[1]), float(candle[2]), float(candle[3]), float(candle[4]),
            float(candle[5]), float(candle[7]) if len(candle) > 7 else 0,
            int(candle[8]) if len(candle) > 8 else 0,
            float(candle[9]) if len(candle) > 9 else 0,
            float(candle[10]) if len(candle) > 10 else 0
        ))

    async with db_manager.pool.acquire() as conn:
        try:
            query = """
                    INSERT INTO kline_data
                    (exchange_id, pair_id, timeframe, close_time, open, high, low, close,
                     volume, quote_volume, trade_num, taker_buy_base_asset_volume, taker_buy_quote_asset_volume)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    ON CONFLICT (exchange_id, pair_id, timeframe, close_time) DO NOTHING \
                    """
            await conn.executemany(query, values)
            return len(values)
        except Exception as e:
            print(f"插入数据时发生错误: {str(e)}")
            import traceback
            traceback.print_exc()
            return 0
