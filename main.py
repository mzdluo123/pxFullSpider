import asyncio
from pixivpy3 import *
from loguru import logger
from concurrent.futures import ThreadPoolExecutor
from model import *
from typing import Dict, List
import datetime
import time


async def main():
    recommended_params = {}
    while recommended_params is not None:
        data = await loop.run_in_executor(executor, recommended_task, recommended_params)
        await processIllustList(data["illusts"])
        download_related(data["illusts"])
        recommended_params = api.parse_qs(data["next_url"])


def download_related(illusts: List):
    for illust in illusts:
        related_params = {"illust_id": illust["id"]}
        while related_params is not None:
            related_data = await loop.run_in_executor(executor, related_task, related_params)
            related_params = api.parse_qs(related_params["next_url"])
            download_related(related_data["illusts"])


def recommended_task(params: Dict) -> Dict:
    try:
        logger.info("正在下载推荐。。。")
        if len(params) == 0:
            return api.illust_recommended()
        return api.illust_recommended(**params)
    except Exception as e:
        logger.error(f"发生错误 {e} 正在重试")
        time.sleep(20)
        return recommended_task(**params)


def related_task(params: Dict) -> Dict:
    try:
        logger.info("正在下载相关。。。")
        return api.illust_related(**params)
    except Exception as e:
        logger.error(f"发生错误 {e} 正在重试")
        time.sleep(20)
        return related_task(**params)


async def parseWork(illust: Dict) -> Work:
    work = Work()
    work.px_id = illust["id"]
    work.title = illust["title"]
    work.type = illust["type"]
    work.large_download_url = illust["image_urls"]["large"]
    work.create_date = datetime.datetime.strptime(illust["create_date"], "%Y-%m-%dT%H:%M:%S%z")
    work.width = illust["width"]
    work.height = illust["height"]
    work.total_view = illust["total_view"]
    work.total_bookmarks = illust["total_bookmarks"]
    work.page_count = illust["page_count"]
    work.caption = illust["caption"]
    return work


async def parseUser(user: Dict) -> User:
    user_obj = User()
    user_obj.px_id = user["id"]
    user_obj.name = user["name"]
    user_obj.account = user["account"]
    user_obj.profile_img = user["profile_image_urls"]["medium"]
    return user_obj


async def parseTag(tags: Dict) -> List[Tag]:
    return [Tag(name=tag["name"], translated_name=tag["translated_name"]) for tag in tags]


async def processIllustList(illusts: List):
    for i in illusts:
        id = i["id"]
        if Work.get_or_none(Work.px_id == id) is None:
            user = await parseUser(i["user"])
            tags = await parseTag(i["tags"])
            work = await parseWork(i)
            db_user = User.get_or_none(User.px_id == user.px_id)
            if db_user is None:
                user.save()
                db_user = user
                logger.success(f"新建用户 {user.name} 成功")
            tags_id = [Tag.get_or_create(name=tag.name, translated_name=tag.translated_name) for tag in tags]
            work.user = db_user
            work.save()
            logger.success(f"新建作品 {work.title} 成功")
            tags_data = [(work.id, tag[0].id) for tag in tags_id]
            TagLink.insert_many(tags_data, fields=[TagLink.work, TagLink.tag]).execute()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    api = ByPassSniApi()
    executor = ThreadPoolExecutor(32)
    db.create_tables([Work, Tag, User, TagLink])
    logger.success("数据库初始化成功")
    api.require_appapi_hosts()
    api.set_accept_language('zh-cn')
    api.login(CONF.PIXIV_USER, CONF.PIXIV_PWD)
    logger.success("登录成功")
    loop.run_until_complete(main())
