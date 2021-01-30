import json

import asyncpg
import uuid
import datetime


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
    async def new_work(cls, pxid, title, create_time, work_type, caption, user, width, height, view, bookmark, page,
                       url):
        async with cls.pool.acquire() as connection:
            connection: asyncpg.Connection
            res = await connection.fetchrow("""SELECT * FROM works WHERE "works"."pxid" = $1 LIMIT 1""", pxid)
            if res is None:
                work_uuid = new_uuid()
                create_time = datetime.datetime.strptime(create_time, "%Y-%m-%dT%H:%M:%S%z")
                await connection.execute("""
                    INSERT INTO works VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) 
                  """, work_uuid, pxid, title, create_time, work_type, caption,
                                         width, height, view, bookmark,
                                         page,
                                         url, user)
                return work_uuid
            else:
                return res["id"]

    @classmethod
    async def new_user(cls, name, pxid, account, comment, avatar, webpage, follow, illusts, manga, novels, bookmarks,
                       twitter, job):
        async with cls.pool.acquire() as connection:
            connection: asyncpg.Connection
            res = await connection.fetchrow("""SELECT * FROM "user" WHERE "user".pxid = $1 LIMIT 1""", pxid)
            if res is None:
                uid = new_uuid()
                await connection.execute(
                    """INSERT INTO "user" VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)""", uid, name, pxid,
                    account, comment, avatar, webpage, follow, illusts, manga, novels, bookmarks,
                    twitter, job)
                return uid
            else:
                return res["id"]

    @classmethod
    async def work_tag(cls, work_uuid, tag):
        async with cls.pool.acquire() as connection:
            connection: asyncpg.Connection
            await connection.execute("""INSERT INTO work_tag VALUES ($1,$2,$3)""", new_uuid(), work_uuid, tag)

    @classmethod
    async def clean_tasks(cls, work_list):
        cleaned = []
        async with cls.pool.acquire() as connection:
            connection: asyncpg.Connection
            if work_list is None:
                return
            for i in work_list:
                if i.type == "work":
                    res = await connection.fetchrow("""SELECT * FROM works WHERE "works"."pxid" = $1 LIMIT 1""",
                                                    i.content)
                    if res is None:
                        cleaned.append(i)
                else:
                    cleaned.append(i)
            return cleaned
