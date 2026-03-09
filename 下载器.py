import re
import json
import requests
from base64 import b64encode, b64decode
from requests.adapters import HTTPAdapter
from urllib.parse import urlparse

adapter = HTTPAdapter(max_retries=3)
session = requests.Session()
session.mount("http://", adapter)


def attachment():
    # 已购买帖子没有video_time_length
    # 需要登录  可以下载vip视频
    url = "https://www.haijiao.com/api/attachment"
    data = {"id":8873494,"resource_id":1468798,"resource_type":"topic","line":""}   # 普通贴
    # data = {"id":8920059,"resource_id":1476347,"resource_type":"topic","line":""}   # 普通贴
    # {"id":8920059,"resource_id":1476347,"resource_type":"topic","line":""}
    # data = {"id":228706,"resource_id":131621,"resource_type":"topic","line":""} # v3会员贴
    # m3u8链接 https://ts2.hjbd81.top/hjstore/video/20210517/fa18b2b48c79e3181aa87b382762eaa4/31340.ts?Policy==&Signature=/Y
    # 修改域名后 https://ts2.hjbd81.top/hjstore/video/20210517/fa18b2b48c79e3181aa87b382762eaa4/3134.jpeg.txt
    # 原图片链接 https://test.hjbd80.top/hjstore/video/20210517/fa18b2b48c79e3181aa87b382762eaa4/3134.jpeg.txt
    # data = {"id":452949,"resource_id":76206,"resource_type":"topic","line":"normal2"}  # 账号封禁 + 帖子被删
    # {'isEncrypted': False, 'errorCode': 0, 'message': '请求错误, 主题不存在', 'success': False, 'data': None}
    # https://test.hjbd80.top/hjstore/video/20210803/b3df65cba70e11158daafda89a815b5b/8441.jpeg.txt
    # https://ts2.hjbd81.top/hjstore/video/20210803/b3df65cba70e11158daafda89a815b5b/84410.ts    修改可以下载,但是没有iv
    # https://ts2.hjbd81.top/hjstore/video/20210803/b3df65cba70e11158daafda89a815b5b/enc_452949.key

    headers = {
        # Referer: https://www.haijiao.com/post/details?pid=1476347
        "Host": "www.haijiao.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Content-Type": "application/json",
        "Origin": "https://www.haijiao.com",
        "Alt-Used": "www.haijiao.com",
        "Connection": "keep-alive",
    }
    with requests.post(url=url, headers=headers, json=data) as resp:
        data = resp.json()
        data = b64decode(b64decode(b64decode(data["data"] or ""))).decode("utf-8")
        print(data)

def get_url(url):
    with session.head(url) as resp:
        return resp.status_code


def get_mid(url_param):
    first = 0
    last = 3000
    while True:
        mid = (last + first) // 2
        print(first, last, mid)
        if get_url(url_param + f"{mid}.ts") == 200:
            if abs(last - first) <= 2:
                print("结果：", mid)
                return mid
            else:
                first = mid
        else:
            last = mid


def download():
    vid = "705293"
    url = "https://www.haijiao.com/api/topic/"+vid
    headers = {
        "Host": "www.haijiao.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
        "Alt-Used": "www.haijiao.com",
        "Connection": "keep-alive",
    }
    with requests.get(url=url, headers=headers, verify=False) as resp:
        data = resp.json()
        data = json.loads(b64decode(b64decode(b64decode(data["data"]))).decode("utf-8"))
        print(data)
    video = [i for i in data["attachments"] if i["category"] == "video"][0]

    url = f"https://www.haijiao.com{video['remoteUrl']}"
    with session.get(url) as resp:
        data = resp.text.rstrip("\n")

    m3u8_t_param = re.findall(r'#EXTINF:.*,', data)[0]    # 时间片
    data = data.split("\n")
    m3u8_head = "\n".join(data[:5]) + "\n"

    url = urlparse(data[6])
    url_param = f"{url.scheme}://{url.netloc}{url.path}".replace("0.ts", "")

    data = m3u8_head

    last = get_mid(url_param)
    for i in range(0, last+1):
        data += m3u8_t_param + "\n" + (url_param + f"{i}.ts\n")
    data += "#EXT-X-ENDLIST"
    print(data)
    with open(f"v{vid}.txt", "w") as fp:
        fp.write(data)


if __name__ == '__main__':
    attachment()
    # download()

    # ts_partion = re.findall(r"_i(\w+).ts\?", m3u8_content)  # 提取 ts序号
    # iv = re.search(r'IV=(0x[0-9a-fA-F]+)', m3u8_content).group(1)    # 提取 IV
    # ts_urls = re.findall(r'(https://.*?\.ts\?.*?)\n', m3u8_content)

