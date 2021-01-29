import asyncio
import sys
from queue import Queue
from db import DB
from loguru import logger
from pixivpy3 import ByPassSniApi, AppPixivAPI
import time
from conf import CONF
from utils import async_in_pool

task_queue = Queue()

if len(sys.argv) != 1:
    CONF.load_data(sys.argv[1])
    logger.warning(f"已加载配置文件 {sys.argv[1]}")
else:
    CONF.load_data()


class Task:
    def __init__(self, task_type, content):
        self.type = task_type
        self.content = content


async def main():
    data = api.illust_recommended()
    print(data)
    tags = data["illusts"][0]["tags"]
    search = api.search_illust(tags[0]['name'])
    print(search)


async def user_tasks(uid):
    tasks = []
    data = []
    page = api.user_bookmarks_illust(uid)
    data.append(page)
    while True:
        params = api.parse_qs(page["next_url"])

        if params is not None:
            page = api.user_bookmarks_illust(**params)
            data.append(page)
        else:
            break
    for i in data:
        for work in i["illusts"]:
            tasks.append(Task("work", work["id"]))

    page = api.user_illusts(uid)
    data.append(page)
    while True:
        params = api.parse_qs(page["next_url"])
        if params is not None:
            page = api.user_illusts(**params)
            data.append(page)
        else:
            break
    for i in data:
        for work in i["illusts"]:
            tasks.append(Task("work", work["id"]))
    return tasks


async def recommend_tasks(page):
    data = api.illust_recommended()


async def works_tasks(ill_id):
    ids = []
    data = await async_call(api.illust_detail, ill_id)
    if data is not None:
        illust = data["illust"]
        for i in illust["tags"]:
            tag_data = await async_call(
                lambda: api.requests.get(
                    f"https://www.pixiv.net/ajax/search/tags/{i['name']}?lang=zh").json())
            body = tag_data["body"]
            tag_db = {"tag": body['tag'],
                      'en': body['tagTranslation'][i['name']]['en'],
                      'zh': body['tagTranslation'][i['name']]['zh'],
                      'abstract': body['pixpedia']['abstract'],
                      'parent': body['pixpedia'].get('parentTag', None),
                      'pxid': int(body['pixpedia']['id']),
                      'siblings': body['pixpedia'].get('siblingsTags', None),
                      'children': body['pixpedia'].get('childrenTags', None),
                      }
            await DB.new_tag(**tag_db)
        logger.info(f"处理了{illust['tags']}个id")
        ids.append(Task("user", illust["user"]["id"]))

        print(data)
        userid = data["illust"]["user"]["id"]
        user_data = await async_call(api.user_detail, userid)
        print(user_data)

        tag_data = await async_call(
            lambda: api.requests.get(
                "https://www.pixiv.net/ajax/search/tags/%E5%A5%B3%E3%81%AE%E5%AD%90?lang=zh").json())
        print(tag_data)


@async_in_pool
def async_call(fun, *args):
    try:
        data = fun(*args)
        if "error" in data and data["error"] == True:
            if data["error"]["user_message"] == "该作品已被删除，或作品ID不存在。":
                logger.error("作品不存在")
                return
            logger.error(data)
            logger.error("达到频率限制，sleep两分钟")
            time.sleep(160)
            api.login(CONF.PIXIV_USER, CONF.PIXIV_PWD)
        return data
    except Exception as e:
        logger.error(e)
        time.sleep(10)


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
    loop.run_until_complete(works_tasks(87204776))
