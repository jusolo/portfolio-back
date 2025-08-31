import os
from psycopg_pool import AsyncConnectionPool

DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("Falta DATABASE_URL")

TUNED = f"{DB_URL}{'&' if '?' in DB_URL else '?'}application_name=render-api&keepalives=1&keepalives_idle=30&keepalives_interval=10&keepalives_count=3"

POOL = AsyncConnectionPool(
    conninfo=TUNED,
    min_size=0,                 # no abras conexiones hasta la primera query
    max_size=int(os.getenv("PGPOOL_MAX_SIZE", "5")),
    timeout=8,                   # tiempo máx para conseguir conexión del pool
    kwargs={"prepare_threshold": None},
)

async def open_pool():
    await POOL.open()

async def close_pool():
    await POOL.close()