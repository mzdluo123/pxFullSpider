import json

import asyncpg
import uuid


def new_uuid():
    return uuid.uuid1()


class DB:
    pool: asyncpg.pool.Pool

    @classmethod
    async def init_db(cls):
        cls.pool = await asyncpg.create_pool("postgresql://postgres:admin@localhost/pixiv")

    @classmethod
    async def new_tag(cls, tag, en, zh, abstract, parent, pxid, siblings, children):
        async with cls.pool.acquire() as connection:
            connection: asyncpg.Connection
            await connection.set_type_codec(
                'json',
                encoder=json.dumps,
                decoder=json.loads,
                schema='pg_catalog'
            )
            res = await connection.fetch("""SELECT * FROM tag WHERE tag.tag = $1 LIMIT 1""", tag)
            if len(res) == 0:
                await connection.execute("""
                    INSERT INTO tag VALUES ($1,$2,$3,$4,$5,$6,$7,$8::json,$9::json)
                  """, new_uuid(), tag, en, zh, abstract, parent, pxid, siblings, children)

    @classmethod
    async def new_work(cls, pxid, title, create_time, type, caption, user, width, height, view, bookmark, page, url):
        async with cls.pool.acquire() as connection:
            connection: asyncpg.Connection

            res = await connection.fetch("""SELECT * FROM "work" WHERE "work"."pxid" = $1 LIMIT 1""", pxid)
            if len(res) == 0:
                await connection.execute("""
                    INSERT INTO tag VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                  """, new_uuid(), pxid, title, create_time, type, caption, user, width, height, view, bookmark, page,
                                         url)
