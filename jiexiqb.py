# -*- coding: utf-8 -*-
import json
import time
import pickle
import boto3
import requests
from requests.adapters import HTTPAdapter
from loguru import logger
from base64 import b64decode
from botocore.config import Config
from sqlalchemy import create_engine, insert, Column, Integer, String, ForeignKey, DateTime, Text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship


adapter = HTTPAdapter(max_retries=3)
session = requests.Session()
session.mount("http://", adapter)

engine = create_engine('',
                       # pool_recycle=3600,  # 每小时回收一次连接
                       pool_pre_ping=True,  # 每次获取连接时进行 ping 操作
                       connect_args={'ssl': {'fake_flag_to_enable_tls': True}})
Session = sessionmaker(bind=engine)
Base = declarative_base()

nodes = {'短视频专区': 900, '伦理之爱': 1288, '销魂视频': 972, '热门': 888, '成人短篇': 269, '讨论杂谈': 205, '海角杂谈': 179, '文化交流': 173, '海角自拍': 115, '泡妞教程': 111, '香港楼凤': 86, '神州楼凤': 77, '同志杂谈': 75, '渣女系列': 56, '海角公告': 14, '海角新闻': 2, '福利姬': 999, '大事记': 16, '收费视频': 1001, '耳目盛宴': 971, '成人连载': 270, '国际观察': 267, '实战交流': 214, '虐恋杂谈': 174, '异国风情': 116, '泡学书籍': 112, '香港娱乐': 87, '神州娱乐': 78, '同志交友': 74, '渣男系列': 57, '讨论': 24, '海角活动': 15, '海角视频': 13, '草榴社区': 2002, '激情时刻': 973, '投诉建议': 283, '性爱技巧': 271, '大事纪实': 258, '换妻换夫': 215, '娱乐八卦': 180, '教学课程': 175, '诱惑模特': 114, '撩妹实战': 113, '澳门博彩': 88, '撩女艳遇': 79, '同志问答': 65, '挽回系列': 58, '头条': 1, '盗版举报': 1000, '图情画意': 962, '颅内高潮': 805, '海角认证': 300, '导师课程': 276, '萝莉诱惑': 272, '女优动态': 265, '绿帽小说': 216, '百姓声音': 181, '寻主招奴': 176, '澳门楼凤': 90, '台湾楼凤': 80, '拉拉杂谈': 72, '私教教学': 59, '人文': 27, '美伦精品': 961, '会员音声': 801, 'AV界大事': 299, '有偿提问': 278, 'COSPLAY': 273, '影视评论': 264, '绿帽影视': 217, '斗鱼虎牙': 208, '直播行业': 207, '影视圈': 206, '投资理财': 183, '虐恋小说': 177, '澳门桑拿': 91, '台湾娱乐': 81, '拉拉交友': 73, '聊天技术': 60, '图片': 31, '娱乐圈大事': 20, '唯美画面': 963, '免费音声': 804, '拉拉问答': 282, '人渣曝光': 279, '制服诱惑': 274, '敢问敢答': 262, '有偿问答': 257, '没话找话': 184, '有偿答问': 178, '澳门会所': 92, '修车记录': 82, '情感天地': 61, '把妹': 30, '猫咪AV': 2022, '原味市场': 978, '网络红人': 275, '莲蓬鬼话': 143, '泰国伴游': 93, '恋爱': 40, '橘子直播': 2001, '偷拍露出': 488, '舞文弄墨': 144, '泰大浴室': 94, '虐恋': 25, '番号大全': 803, '海角线下': 979, '泰国娱乐': 284, '悠悠小说': 145, '图片故事': 98, '绿帽': 4, '大陆新闻': 17, '体育赛事': 980, 'TS/CD': 802, '越南夜游': 285, '真爱': 35, '港澳台新闻': 3, '日本夜游': 286, '神州': 36, '马来夜游': 287, '海外': 33, '韩国夜游': 288, '站务': 6, '美加夜游': 289, '欧洲夜游': 290, '新加坡夜游': 291, '菲律宾夜游': 292, '柬埔寨夜游': 293}
[nodes.pop(_) for _ in ["海角活动", "投诉建议", "海角公告", "盗版举报", "海角认证", "站务"]]
history_count = {'伦理之爱': 27876, '热门': 109096, '成人短篇': 18419, '讨论杂谈': 70483, '海角杂谈': 28488, '海角自拍': 75309, '实战交流': 31115, '虐恋杂谈': 11519, '同志交友': 15733, '讨论': 10000, '换妻换夫': 86814, '海角视频': 6461, '收费视频': 5987, '同志杂谈': 4562, '泡妞教程': 2880, '神州楼凤': 2765, '海角新闻': 1865, '福利姬': 1842, '渣女系列': 979, '文化交流': 974, '成人连载': 3625, '国际观察': 3290, '异国风情': 1926, '香港楼凤': 431, '渣男系列': 699, '激情时刻': 414, '投诉建议': 806, '性爱技巧': 3724, '大事纪实': 889, '娱乐八卦': 4961, '教学课程': 386, '香港娱乐': 295, '耳目盛宴': 293, '神州娱乐': 242, '泡学书籍': 225, '海角公告': 203, '台湾楼凤': 190, '销魂视频': 153, '海角活动': 31, '草榴社区': 29, '大事记': 11, '短视频专区': 1, '诱惑模特': 7536, '撩妹实战': 1729, '澳门博彩': 247, '撩女艳遇': 340, '同志问答': 642, '挽回系列': 674, '头条': 10994, '盗版举报': 188, '图情画意': 122, '颅内高潮': 1383, '海角认证': 585, '导师课程': 1165, '萝莉诱惑': 3503, '女优动态': 470, '绿帽小说': 3877, '百姓声音': 2919, '寻主招奴': 32266, '澳门楼凤': 351, '拉拉杂谈': 706, '私教教学': 1755, '人文': 44794, '美伦精品': 167, '会员音声': 988, 'AV界大事': 1, '有偿提问': 749, 'COSPLAY': 3817, '影视评论': 665, '绿帽影视': 18227, '斗鱼虎牙': 2, '直播行业': 2, '影视圈': 7, '投资理财': 1716, '虐恋小说': 832, '澳门桑拿': 222, '台湾娱乐': 290, '拉拉交友': 962, '聊天技术': 773, '图片': 142956, '娱乐圈大事': 8, '唯美画面': 689, '免费音声': 10551, '拉拉问答': 381, '人渣曝光': 543, '制服诱惑': 4481, '敢问敢答': 634, '有偿问答': 379, '没话找话': 10000, '有偿答问': 187, '澳门会所': 219, '修车记录': 359, '情感天地': 2754, '把妹': 7291, '猫咪AV': 5, '原味市场': 709, '网络红人': 3326, '莲蓬鬼话': 3400, '泰国伴游': 246, '恋爱': 7639, '橘子直播': 6, '偷拍露出': 27201, '舞文弄墨': 532, '泰大浴室': 204, '虐恋': 46169, '番号大全': 477, '海角线下': 114, '泰国娱乐': 296, '悠悠小说': 2161, '图片故事': 15739, '绿帽': 211089, '大陆新闻': 657, '体育赛事': 2160, 'TS/CD': 14451, '越南夜游': 252, '真爱': 37462, '港澳台新闻': 293, '日本夜游': 337, '神州': 4186, '马来夜游': 226, '海外': 4713, '韩国夜游': 287, '站务': 1228, '美加夜游': 213, '欧洲夜游': 221, '新加坡夜游': 227, '菲律宾夜游': 241, '柬埔寨夜游': 198}

types = {
    "默认": 0,
    "最新": 1,    # 最新比默认获取数据更多
    "热门": 2,    # 数据多
    "精华": 3,    # 数据量小
    "悬赏": 4,    # 数据量小
    "出售": 5,
}


class User(Base):
    __tablename__ = 'user'

    userid = Column(String(20), primary_key=True)
    nickname = Column(String(255), nullable=True)
    status = Column(Integer, default=0)

    # 定义与Topic模型的关系
    topics = relationship("Topic", back_populates="user")


class Topic(Base):
    __tablename__ = 'topic'

    topicid = Column(String(20), primary_key=True)
    title = Column(String(255))
    type = Column(Integer)
    moneytype = Column(Integer)
    userid = Column(String(20), ForeignKey('user.userid'), nullable=False)
    nickname = Column(String(255))
    nodeid = Column(String(10))
    parentid = Column(String(10))
    nodename = Column(String(50))
    viewcount = Column(Integer)
    likecount = Column(Integer)
    status = Column(Integer, default=0)
    createtime = Column(DateTime)
    isoriginal = Column(Integer)
    content = Column(Text)
    amount = Column(Integer, default=None)

    # 定义与User模型的关系
    user = relationship("User", back_populates="topics")


class Bucket:
    def __init__(self):
        aws_access_key_id = ''
        aws_secret_access_key = ''
        endpoint_url = 'https://s3'

        self.s3 = boto3.resource(
            service_name='s3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=endpoint_url,
            config=Config(signature_version='s3v4')
        )

    def put(self, bucket_name, key, data):
        return self.s3.Bucket(bucket_name).put_object(Key=key, Body=data)

    def put_json(self, bucket_name, key, data):
        json_data = json.dumps(data)
        return self.put(bucket_name, key, json_data)

    def get(self, bucket_name, key):
        return self.s3.ObjectSummary(bucket_name=bucket_name, key=key).get()['Body'].read()

    def get_json(self, bucket_name, key):
        json_data = json.loads(self.get(bucket_name, key))
        return json_data

    def list(self, bucket_name):
        return [obj.key for obj in self.s3.Bucket(bucket_name).objects.all()]

    def delete(self, bucket_name, key):
        return self.s3.ObjectSummary(bucket_name=bucket_name, key=key).delete()


bucket = Bucket()


def bulk_insert_data(mode, data):
    with Session() as session:
        # 批量插入帖子数据
        try:
            stmt = insert(mode).values(data).prefix_with('IGNORE', dialect='mysql')  # 重复数据忽略
            result = session.execute(stmt.execution_options(stream_results=True))
            logger.info(f"已添加 {result.rowcount} 条数据")
            # session.execute(stmt)
            session.commit()
        except IntegrityError as e:
            # 处理主键冲突，例如回滚事务或记录错误
            session.rollback()
            print(f"插入帖子数据失败: {e}")
        finally:
            session.close()


def get_node_topics(nodeId=nodes["收费视频"], page=1, type=types["出售"], limit=100):
    url = f"https://www.haijiao.com/api/topic/node/topics?page={page}&nodeId={nodeId}&type={type}&limit={limit}"
    headers = {
        "Host": "www.haijiao.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": f"https://www.haijiao.com/article?nodeId={nodeId}"
    }
    try:
        with session.get(url=url, headers=headers) as resp:
            content = resp.json()
            if not content["data"]:
                logger.warning(f"{resp.status_code} 请求出错: {content}")
                return {}
            data = b64decode(b64decode(b64decode(content["data"]).decode()).decode()).decode()
            return json.loads(data)
    except Exception as e:
        logger.error(f"请求异常: {e}")
        return {}


def paqu_node_topics():
    logger.info("开始执行程序")
    users = pickle.loads(bucket.get("typora", "user"))

    limit = 100
    totals = {}
    # '讨论': 10000, 数据有80w+ 太大以后下载 已下载154084 24 类型:2 第1543页 获取到 100 条信息
    dili = {'没话找话': 10000}  # 80w+
    for name in dili:
        print(name, f"https://www.haijiao.com/api/topic/node/topics?page=1000&nodeId={nodes[name]}&type=2&limit=100")
        new_user = []
        flag = True
        nodeId = nodes[name]      # xxx
        # typenum = types["出售"]
        # typenum = types["最新"]
        typenum = types["热门"]
        posts_data = []
        tsu = set()
        total = -1
        for number in range(1, 20000):
            content = get_node_topics(nodeId=nodeId, page=number, type=typenum, limit=limit)
            if flag:
                total = content['page']['total']
                print(f"总共 {content['page']['total']} 条数据，每次获取 {limit} 条，预计 {int(content['page']['total']/limit+1)} 次完成")
                flag = False
            if not content:
                total -= number * limit
                logger.warning("程序因异常 提前结束")
                break
            if (content["page"]["page"] * content["page"]["limit"]) > content["page"]["total"] and not content["results"]:
                logger.success("信息已收集完成")
                break

            print(f"开始获取 {nodeId} 类型:{typenum} 第{number}页 获取到 {len(content.get('results', []))} 条信息 剩余 {0 if number*limit > total else total-number*limit} 条")
            for di in content["results"]:
                tsu.add(str(di["topicId"]))
                if not di["hasVideo"] and not list(filter(lambda x: x["category"] == "video", di["attachments"] or [])):  # 如果没有视频
                    continue
                posts_data.append({
                    'topicid': str(di["topicId"]),  # 帖子id
                    'title': di["title"],  # 帖子标题
                    'type': di["type"],  # 帖子付费类型 0普通 1金币|钻石
                    'moneytype': 1 if di["type"] == 1 and di["money_type"] == 0 else di["money_type"],  # 帖子付费类型 0普通 1金币 2钻石
                    'userid': str(di["user"]["id"]),  # 用户id    status为ture有些id不对
                    'nickname': di["user"]["nickname"],  # 用户名
                    'nodeid': di["node"]["nodeId"],  # 帖子归类
                    'viewcount': di["viewCount"],  # 阅读数量
                    'likecount': di["likeCount"],  # 点赞数量
                    'status': di["status"],  # 帖子状态 0可用 1禁止
                    'createtime': di["createTime"],  # 帖子创建时间
                    'isoriginal': di["is_original"],  # 是否原创
                    'content': '',
                    # 'amount': 100
                })
            # if (number % 300) == 0:
            #     with open(f"data/topics {nodeId} {total}.bak", "w") as fp:
            #         fp.write(json.dumps(posts_data))

        print("热门 topic数量", len(posts_data))
        totals[name] = len(tsu)
        print(name, len(tsu), totals)

        if posts_data:
            bucket.put_json("typora", f"topics {nodeId} {total} {int(time.time())}", posts_data)

        for i in posts_data:
            if i["userid"] in users:
                continue
            new_user.append({
                'userid': i["userid"],  # 用户id
                'nickname': i["nickname"],  # 用户名
                'status': i["status"],  # 帖子状态 0可用 1禁止
            })

        if new_user:
            users.update([i["userid"] for i in new_user])
            logger.info("正在添加新用户")
            bulk_insert_data(User, new_user)
            bucket.put("typora", "user", pickle.dumps(users))
        else:
            logger.info("没有新用户")

        logger.info("正在添加帖子数据")
        if posts_data:
            bulk_insert_data(Topic, posts_data)
        logger.success("任务完成")


def main():
    paqu_node_topics()


if __name__ == '__main__':
    main()

# ["海角活动", "投诉建议", "海角公告", "盗版举报", "海角认证", "站务"]
# 488偷拍露出 27201 {'偷拍露出': 27201} 数据库连接失败 tebi有备份
# 免费音声 10551 {'图片故事': 15739, '免费音声': 10551}
# 图片 142956 {'绿帽': 211089, '图片': 142956}
# 10994 {'头条': 10994}

