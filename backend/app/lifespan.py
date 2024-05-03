import os
from contextlib import asynccontextmanager

import asyncpg
import orjson
from fastapi import FastAPI
from langgraph_sdk.client import LangServeClient, get_client

_pg_pool = None
_langserve = None


def get_pg_pool() -> asyncpg.pool.Pool:
    return _pg_pool


def get_langserve() -> LangServeClient:
    return _langserve


async def _init_connection(conn) -> None:
    await conn.set_type_codec(
        "json",
        encoder=lambda v: orjson.dumps(v).decode(),
        decoder=orjson.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "uuid", encoder=lambda v: str(v), decoder=lambda v: v, schema="pg_catalog"
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pg_pool, _langserve

    _pg_pool = await asyncpg.create_pool(
        database=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        init=_init_connection,
    )
    _langserve = get_client(url=os.environ["LANGSERVE_URL"])
    yield
    await _pg_pool.close()
    await _langserve.http.client.aclose()
    _pg_pool = None
    _langserve = None
