import json
import pickle
import boto3
from sqlalchemy import create_engine, insert, Column, Integer, String, ForeignKey, DateTime, Text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from botocore.config import Config
from loguru import logger

# 创建数据库引擎
engine = create_engine('',
                       # pool_recycle=3600,  # 每小时回收一次连接
                       pool_pre_ping=True,  # 每次获取连接时进行 ping 操作
                       connect_args={'ssl': {'fake_flag_to_enable_tls': True}})

# 创建基类
Base = declarative_base()


class Bucket:
    def __init__(self):
        aws_access_key_id = ''
        aws_secret_access_key = ''
        endpoint_url = 'https://s3.'

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


# 定义用户模型
class User(Base):
    """
    CREATE TABLE users (
      userid VARCHAR(20) PRIMARY KEY,
      nickname VARCHAR(255) NOT NULL,
      status INT DEFAULT 0
    );
    """
    __tablename__ = 'user'

    userid = Column(String(20), primary_key=True)
    nickname = Column(String(255), nullable=True)
    status = Column(Integer, default=0)

    # 定义与Topic模型的关系
    topics = relationship("Topic", back_populates="user")


class Topic(Base):
    """
    CREATE TABLE topic (
      topicid VARCHAR(20) PRIMARY KEY,
      title VARCHAR(255),
      type TINYINT,
      moneytype TINYINT,
      userid VARCHAR(20) NOT NULL,
      nickname VARCHAR(255),
      nodeid VARCHAR(10),
      viewcount INT,
      likecount INT,
      status TINYINT DEFAULT 0,
      createtime DATETIME,
      isoriginal TINYINT,
      content TEXT,
      amount INT DEFAULT NULL,
      CONSTRAINT FK_topic_userid FOREIGN KEY (userid) REFERENCES user(userid)
    );
    """
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
    # 定义与Video模型的关系
    videos = relationship("Video", back_populates="topic")


class Video(Base):
    """
    CREATE TABLE video (
        vid VARCHAR(20) PRIMARY KEY,
        topicid VARCHAR(20) NOT NULL,
        remoteurl VARCHAR(255),
        vtlength INT,
        filename VARCHAR(100),
        m3u8head TEXT,
        extinf INT,
        urlparam VARCHAR(255),
        portion INT,
        FOREIGN KEY (topicid) REFERENCES topic(topicid)
    );
    """
    __tablename__ = 'video'

    vid = Column(String(20), primary_key=True)
    topicid = Column(String(20), ForeignKey('topic.topicid'), nullable=False)
    remoteurl = Column(String(255))
    vtlength = Column(Integer)
    filename = Column(String(100))
    m3u8head = Column(Text)
    extinf = Column(String(30))
    urlparam = Column(String(255))
    portion = Column(Integer)

    # 定义与Topic模型的关系
    topic = relationship("Topic", back_populates="videos")


# 创建会话工厂
Session = sessionmaker(bind=engine)

# ============================================================================================================
# 获取所有用户id
def get_users():
    with Session() as session:
        # 查询所有用户 ID
        user_ids = set(i[0] for i in session.query(User.userid).all())
        session.close()
        return user_ids


def get_topics():
    with Session() as session:
        query = session.query(Topic.topicid, Topic.userid, Topic.title).filter(Topic.moneytype == '2')
        topics = [list(i) for i in query.all()]
        session.close()
        return topics


# 批量插入用户数据
def bulk_insert_data(mode, data):
    # mode = User, Topic, Video
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


def bulk_insert_topics2(posts_data):
    user_ids = set(post['userid'] for post in posts_data)
    # topic_data = next((post for post in posts_data if post['userid'] == posts_data[0]["userid"]), None)
    with Session() as session:
        # 检查 userid 是否存在，不存在则添加
        for userid in user_ids:
            user = session.query(User).filter_by(userid=userid).first()
            if not user:
                # 假设 posts_data 中包含 topic 的必要信息
                topic_data = next((post for post in posts_data if post['userid'] == userid), None)
                if topic_data:
                    new_topic = Topic(**topic_data)
                    session.add(new_topic)

        # 批量插入帖子数据
        try:
            stmt = insert(Video).values(posts_data)
            session.execute(stmt)
            session.commit()
        except IntegrityError as e:
            # 处理主键冲突，例如回滚事务或记录错误
            session.rollback()
            print(f"插入帖子数据失败: {e}")

        stmt = insert(Topic).values(topic_data).prefix_with('IGNORE', dialect='mysql')   # 重复数据忽略
        session.execute(stmt)
        session.commit()


def main():
    # save_jsonfile.json 7642 除去{'state': True} 1803
    new_user = []
    name = "topics 98 10000 1725864714"
    bucket = Bucket()
    users = pickle.loads(bucket.get("typora", "user"))
    posts_data = bucket.get_json("typora", name)
    print(len(posts_data))
    if posts_data:
        with open(f"data/{name}", "w") as fp:
            fp.write(json.dumps(posts_data))

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


if __name__ == '__main__':
    main()

# url = f"https://haijiao.com/api/topic/node/topics?page=1&userId=16801025&type=5"
# {15403: '海角官方1号', 160597: '鸭梨梨', 167654502602: 'Q弹E姐', 169: '海角管理员', 19090467: '阿喵', 168377771101: '浅汐浅汐浅汐', 10482878: '叶晓璇', 171247903201: '西西原味儿', 15407: '海角官方5号', 167: '海角管理_饭饭', 149: '海角mingL', 23815160: '重庆原味粉蝴蝶娜', 15601: '海角管理_鱼姐', 15513: '老菜讲坛', 15514: '二郎个狼', 15504: '囡囡科普', 15503: '表姐唠唠', 15523: '岁寒三友', 15524: '辣评情感专业户', 15413: '高盖伦', 15414: '于于淼', 15502: '海角官方解说-昭昭', 533091: '鹤仙人2021', 9937473: '嗲喵喵', 15405: '海角官方3号', 14788: '菠菜—胖乎乎', 15406: '海角官方4号', 145: '海角小助手', 15402: '海角经纪人', 170514804901: '桃桃有D罩杯', 16752: '菠菜鸡米花', 15404: '海角官方2号', 15423: '掉毛哥不嘚瑟', 15424: '小兔队长来啰嗦'}
