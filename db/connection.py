"""数据库连接管理"""
import asyncpg

from conf import config


class DatabaseManager:
    """数据库连接管理器"""

    def __init__(self):
        self.pool = None

    async def create_pool(self):
        """创建数据库连接池"""
        if self.pool is None:
            self.pool = await asyncpg.create_pool(**config.DB_CONFIG)
            print("数据库连接池已创建")
        return self.pool

    async def close_pool(self):
        """关闭数据库连接池"""
        if self.pool:
            await self.pool.close()
            self.pool = None
            print("数据库连接池已关闭")

    async def get_connection(self):
        """获取数据库连接"""
        if self.pool is None:
            await self.create_pool()
        return await self.pool.acquire()

    async def release_connection(self, connection):
        """释放数据库连接"""
        await self.pool.release(connection)


# 创建全局数据库管理器实例
db_manager = DatabaseManager()
