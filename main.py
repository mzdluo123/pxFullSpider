import asyncio
from pixivpy3 import *
from loguru import logger
from concurrent.futures import ThreadPoolExecutor
from model import *
from typing import Dict, List
import datetime
import time
from kf import producer, consumer, TOPIC
import random


async def main():
    for page in range(3):
        data = await loop.run_in_executor(executor, recommended_task, page)
        await processIllustList(data["illusts"])  # 作品放入数据库
        for illust in data["illusts"]:  # 将关联作品放入队列
            sendData(illust['id'])
        await asyncio.sleep(1)
    logger.info("等待数据")
    for data in consumer:
        logger.info(f"开始抓取 {data.value['id']}")
        await asyncio.sleep(random.randint(0,5))
        await download_related(data.value)


def sendData(px_id: int):
    logger.info(f"正在将 {px_id} 放入队列")
    future = producer.send(TOPIC, {"id": px_id})
    logger.info(future.get(timeout=1000))


async def download_related(illust: Dict):
    related_data = await loop.run_in_executor(executor, related_task, illust["id"])
    if "illusts" not in related_data:
        if "error" in related_data:
            logger.error(related_data)
            logger.error("达到频率限制，sleep两分钟")
            await asyncio.sleep(160)
        else:
            logger.error(related_data)
        return
    await processIllustList(related_data["illusts"])
    for illust in related_data["illusts"]:  # 将关联作品放入队列
        sendData(illust["id"])


def recommended_task(page: int) -> Dict:
    try:
        logger.info("正在下载推荐。。。")
        return api.illust_recommended(offset=page)
    except Exception as e:
        logger.error(f"发生错误 {e} 正在重试")
        time.sleep(20)
        return recommended_task(page)


def related_task(work_id: int) -> Dict:
    try:
        logger.info(f"正在下载{work_id}相关。。。")
        return api.illust_related(work_id)
    except Exception as e:
        logger.error(f"发生错误 {e} 正在重试")
        time.sleep(20)
        return related_task(work_id)


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
    if db.is_closed():
        db.connect()
    for i in illusts:
        id = i["id"]
        if Work.get_or_none(Work.px_id == id) is None:
            with db.atomic() as t:
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
                t.commit()
    db.close()


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
