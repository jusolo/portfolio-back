import os
from psycopg_pool import AsyncConnectionPool
DB_URL = os.environ["DATABASE_URL"]
pool = AsyncConnectionPool(conninfo=DB_URL, max_size=5)

async def fetchval(sql: str, *args):
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, args)
            row = await cur.fetchone()
            return row[0] if row else None
