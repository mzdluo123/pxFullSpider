import asyncio
import datetime
import random
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List

from loguru import logger
from pixivpy3 import *

from kf import producer, consumer, TOPIC
from kafka.errors import CommitFailedError
from model import *


async def main():
    for page in range(5):
        data = await loop.run_in_executor(executor, recommended_task, page)
        await processIllustList(data["illusts"])  # 作品放入数据库
        await asyncio.sleep(1)
    logger.info("等待数据")
    while True:
        partition = consumer.poll(max_records=3)
        for k, v in partition.items():
            logger.info(f"获取到Partition {k.partition}")
            logger.debug(f"获取到 {len(v)}个任务")
            for j in v:
                await asyncio.sleep(1)
                loop.create_task(download_related(j.value))
        await asyncio.sleep(random.randint(0, 2))
        await commit()
        logger.debug(f"成功提交")


async def commit():
    try:
        consumer.commit()
    except CommitFailedError as e:
        logger.error(f"提交失败 {e}")
        await asyncio.sleep(4)


def sendData(px_id: int):
    logger.success(f"正在将 {px_id} 放入队列")
    future = producer.send(TOPIC, {"id": px_id})
    logger.info(future.get(timeout=1000))


async def download_related(illust: Dict):
    related_data = await loop.run_in_executor(executor, related_task, illust["id"])
    if "illusts" not in related_data:
        if "error" in related_data:
            logger.error(related_data)
            logger.error("达到频率限制，sleep两分钟")
            time.sleep(160)
            api.login(CONF.PIXIV_USER, CONF.PIXIV_PWD)
        else:
            logger.error(related_data)
        return
    await processIllustList(related_data["illusts"])


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
    if len(illust['meta_single_page']) != 0:
        work.large_download_url = illust['meta_single_page']['original_image_url']
    else:
        work.large_download_url = illust['meta_pages'][0]['image_urls']['original']
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
        px_id = i["id"]
        if Work.get_or_none(Work.px_id == px_id) is None:
            sendData(px_id)
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
        else:
            logger.warning(f"{px_id} 已存在")
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
