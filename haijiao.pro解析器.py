import requests
import json
import re
import time
import pickle
import requests
from requests.adapters import HTTPAdapter
from loguru import logger

adapter = HTTPAdapter(max_retries=3)
session = requests.Session()
session.mount("http://", adapter)


def get_hot_topics(page=1):
    url = f"https://haijiao.pro/api/topic/hot/topics?page={page}&type={10}" # &limit={5}
    headers = {
        "Host": "haijiao.pro",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Connection": "keep-alive",
        "Referer": "https://haijiao.pro/",
    }
    try:
        with requests.get(url=url, headers=headers) as resp:
            data = resp.json()["data"]
            return data
    except Exception as e:
        logger.error(f"请求异常: {e}")
        return {}


def get_person_topic(tid):
    try:
        url = f"https://haijiao.pro/api/topic/{tid}"
        headers = {
            "Host": "haijiao.pro",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": f"https://haijiao.pro/post/details?pid={tid}",
        }
        with session.get(url=url, headers=headers) as resp:
            content = resp.json()
            return content
    except Exception as e:
        logger.error(f"请求异常: {e}")
        return {}

def main():
    posts_data = []
    limit = 5
    flag = True
    total = -1
    for number in range(666, 5000):
        content = get_hot_topics(number)
        if flag:
            total = content['page']['total']
            print(
                f"总共 {content['page']['total']} 条数据，每次获取 {limit} 条，预计 {int(content['page']['total'] / limit + 1)} 次完成")
            flag = False
        if not content:
            total -= number * limit
            logger.warning("程序因异常 提前结束")
            break
        if (content["page"]["page"] * content["page"]["limit"]) > content["page"]["total"] and not content["results"]:
            logger.success("信息已收集完成")
            break
        print(f"开始获取 类型:最新 第{number}页 获取到 {len(content.get('results', []))} 条信息 剩余 {0 if number*limit > total else total-number*limit} 条")
        for di in content["results"]:
            if not di["hasVideo"]:  # 如果没有视频
                continue
            posts_data.append({
                'topicid': str(di["topicId"]),  # 帖子id
                'title': di["title"],  # 帖子标题
                'type': di["type"],  # 帖子付费类型 0普通 1金币|钻石
                'userid': str(di["user"]["id"]),  # 用户id    status为ture有些id不对
                'nickname': di["user"]["nickname"],  # 用户名
                'nodeid': di["node"]["nodeId"],  # 帖子归类
                'status': di["status"],  # 帖子状态 0可用 1禁止
                'createtime': di["createTime"],  # 帖子创建时间
                'isoriginal': di["is_original"],  # 是否原创
            })
        if (number % 300) == 0:
            with open(f"../data/topics pro10 {total}.bak", "w") as fp:
                fp.write(json.dumps(posts_data))

    if posts_data:
        with open(f"../data/topics pro10 {total} {int(time.time())}", "w") as fp:
            fp.write(json.dumps(posts_data))
    logger.success("任务完成")


def pingjie():
    for tid in data:
        content = get_person_topic(tid)
        if not content or not content["data"]:
            logger.warning(f"{tid} 请求出错: {content}")
            break


if __name__ == '__main__':
    # main()
    pingjie()