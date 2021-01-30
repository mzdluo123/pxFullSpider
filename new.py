import asyncio
import sys
from queue import Queue
from db import DB
from loguru import logger
from pixivpy3 import ByPassSniApi, AppPixivAPI
import time
import random
from conf import CONF
from utils import async_in_pool, Task

if len(sys.argv) != 1:
    CONF.load_data(sys.argv[1])
    logger.warning(f"已加载配置文件 {sys.argv[1]}")
else:
    CONF.load_data()


async def main():
    page = 1
    while True:

        if await DB.count_task() == 0:
            tasks = await recommend_tasks(page)
            page += 1
            logger.info(f"获取了 {len(tasks)} 个任务")
            for i in await DB.clean_tasks(tasks):
                await DB.submit_task(i)
        bench = []
        logger.info(f"队列剩余{await DB.count_task()}")
        for i in range(3):

            async def __job():
                task, finish = await DB.get_task()
                if task is None:
                    return
                logger.info(f"处理任务 {task.type}")
                if task.type == "related":
                    await related_task(task.pxid)
                    await finish()
                    return
                if task.type == "user":
                    await user_tasks(task.pxid)
                    await finish()
                    return
                if task.type == "work":
                    await process_work(task.content)
                    await finish()

            bench.append(__job())

        results = await asyncio.gather(*bench)
        for i in results:
            if i is None:
                break
            await DB.submit_task(*await DB.clean_tasks(i))


async def user_tasks(uid):
    await asyncio.sleep(random.random())
    tasks = []
    data = []
    user_info = api.user_detail(uid)
    user_db = {
        "name": user_info["user"]["name"],
        "pxid": user_info["user"]["id"],
        "account": user_info["user"]["account"],
        "avatar": user_info["user"]["profile_image_urls"].get('medium', None),
        "comment": user_info["user"]["comment"],
        "webpage": user_info["profile"]["webpage"],
        "follow": user_info["profile"]['total_follow_users'],
        "illusts": user_info["profile"]['total_illusts'],
        "manga": user_info["profile"]['total_manga'],
        "novels": user_info["profile"]['total_novels'],
        "bookmarks": user_info["profile"]['total_illust_bookmarks_public'],
        "twitter": user_info["profile"]['twitter_account'],
        "job": user_info["profile"]['job'],
    }
    await DB.new_user(**user_db)
    logger.info(f"创建用户成功 {user_info['user']['name']}")

    page = await async_call(api.user_bookmarks_illust, uid)
    data.append(page)
    for i in range(3):
        params = api.parse_qs(page["next_url"])
        if params is not None:
            page = await async_call(api.user_bookmarks_illust, **params)
            data.append(page)
        else:
            break

    page = await async_call(api.user_illusts, uid)
    data.append(page)
    for i in range(3):
        params = api.parse_qs(page["next_url"])
        if params is not None:
            page = await async_call(api.user_illusts, **params)
            data.append(page)
        else:
            break
    for i in data:
        for work in i["illusts"]:
            tasks.append(Task("work", content=i))
            tasks.append(Task("related", pxid=work["id"]))
    logger.info(f"发现{len(tasks)}个任务")
    return tasks


async def recommend_tasks(page):
    await asyncio.sleep(random.random())
    tasks = []
    data = await async_call(api.illust_recommended)
    for i in data["illusts"]:
        tasks.append(Task("work", content=i))
        tasks.append(Task("related", pxid=i["id"]))
    return tasks


async def process_work(illust):
    await asyncio.sleep(random.random())
    # data = await async_call(api.illust_detail, ill_id)
    # if data is not None:
    # illust = ill_data["illust"]
    if "id" not in illust:
        return
    logger.info(f"开始处理 {illust['id']} {illust['title']}")
    # ID处理

    for i in illust["tags"]:
        if await DB.tag_exist(i['name']):
            logger.warning(f"跳过tag{i['name']}")
            continue
        tag_data = await async_call(
            lambda: api.requests.get(
                f"https://www.pixiv.net/ajax/search/tags/{i['name']}?lang=zh").json())
        if tag_data is None:
            continue
        body = tag_data["body"]
        en = zh = ''
        if body['tagTranslation']:
            tran = body['tagTranslation'].get(body["tag"], {})
            en = tran.get("en", "")
            zh = tran.get("zh", "")
        if body['pixpedia']:
            tag_db = {"tag": body['tag'],
                      'en': en,
                      'zh': zh,
                      'abstract': body['pixpedia'].get('abstract', None),
                      'parent': body['pixpedia'].get('parentTag', None),
                      'pxid': int(body['pixpedia'].get("id", 0)),
                      'siblings': body['pixpedia'].get('siblingsTags', None),
                      'children': body['pixpedia'].get('childrenTags', None),
                      }
        else:
            tag_db = {"tag": body['tag'],
                      'en': en,
                      'zh': zh,
                      'abstract': None,
                      'parent': None,
                      'pxid': None,
                      'siblings': None,
                      'children': None,
                      }
        await DB.new_tag(**tag_db)
    logger.info(f"处理了{len(illust['tags'])}个tag")
    userid = illust["user"]["id"]
    await DB.submit_task(Task("user", pxid=illust["user"]["id"]))
    work_db = {
        "pxid": illust["id"],
        "title": illust["title"],
        "work_type": illust["type"],
        "caption": illust["caption"],
        "user": userid,
        "width": illust["width"],
        "height": illust["height"],
        "view": illust["total_view"],
        "create_time": illust["create_date"],
        "bookmark": illust["total_bookmarks"],
        "page": illust["page_count"],
        "url": illust["meta_single_page"].get("original_image_url", None)
    }
    uuid = await DB.new_work(**work_db)

    # 链接tag
    for i in illust["tags"]:
        tag_name = i["name"]
        work_tag = {
            "work_uuid": uuid,
            "tag": tag_name
        }
        await DB.work_tag(**work_tag)

    logger.info(f"保存成功 {uuid} {illust['title']}")


async def related_task(ill_id):
    await asyncio.sleep(random.random())
    tasks = []
    data = await async_call(api.illust_related, ill_id)
    for i in data["illusts"]:
        tasks.append(Task("related", pxid=i["id"]))
    for i in range(2):
        params = api.parse_qs(data["next_url"])
        if params is not None:
            page = await async_call(api.illust_related, **params)
            logger.info("获取相关作品成功")
            for i in page["illusts"]:
                tasks.append(Task("work", content=i))
                tasks.append(Task("related", pxid=i["id"]))
        else:
            break
    return tasks


@async_in_pool
def async_call(fun, *args, **kwargs):
    def __wrapper():
        return fun(*args, **kwargs)

    while True:
        try:
            data = __wrapper()
        except Exception as e:
            logger.error(e)
            time.sleep(10)
            continue
        if "error" in data:
            if isinstance(data["error"], bool):
                if data["error"]:
                    return None
                return data
            if "OAuth" in data["error"]["message"]:
                api.login(CONF.PIXIV_USER, CONF.PIXIV_PWD)
                continue
            if data["error"]["user_message"] == "deleted":
                logger.error("作品不存在")
                return
            logger.error(data)
            logger.error("达到频率限制，sleep两分钟")
            time.sleep(160)
            continue
            # api.login(CONF.PIXIV_USER, CONF.PIXIV_PWD)
        return data


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    api = AppPixivAPI()
    loop.run_until_complete(DB.init_db())
    logger.success("数据库初始化成功")
    # api.require_appapi_hosts()
    # api.set_accept_language('zh-cn')
    api.login(CONF.PIXIV_USER, CONF.PIXIV_PWD)
    logger.success("登录成功")
    loop.run_until_complete(main())
