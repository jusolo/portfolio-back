import os
from psycopg_pool import AsyncConnectionPool

DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("Falta DATABASE_URL (usa la cadena de conexión de Supabase/Neon con sslmode=require)")

POOL = AsyncConnectionPool(conninfo=DB_URL, min_size=1, max_size=int(os.getenv("PGPOOL_MAX_SIZE", "5")))

async def open_pool():
    await POOL.open()

async def close_pool():
    await POOL.close()
