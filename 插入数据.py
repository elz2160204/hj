import oss2
import json
import requests
import hashlib
import boto3
from botocore.config import Config
import xml.etree.ElementTree as ET
from datetime import datetime


requests.packages.urllib3.disable_warnings()
false = False
true = True
null = None


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


class Pikpak:
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://mypikpak.com/",
            "Origin": "https://mypikpak.com",
            "Connection": "keep-alive",
        }
        self.data = {}
        self.token = {
            "refresh_token": "os.LyrJq-",
            "x-captcha-token": "ck0.-",
            "access_token": "Bearer e",
        }
        self.params = {}
        self.upurl = None

    def files(self, name, filesize, hashhex):
        url = "https://api-drive.mypikpak.com/drive/v1/files"
        data = {
            "hash": hashhex.upper(),
            "name": name,
            "size": str(filesize),
            "kind": "drive#file",
            "id": "",
            "parent_id": "",
            "upload_type": "UPLOAD_TYPE_RESUMABLE",
            "folder_type": "NORMAL",
            "resumable": {"provider": "PROVIDER_ALIYUN"}
        }
        with requests.post(url=url, headers=dict(self.headers, **{
            "Host": url.split("/")[2],
            "Content-Type": "application/json",
            "Accept-Language": "zh-CN",
            "x-device-id": "",
            "x-captcha-token": self.token["x-captcha-token"],
            "Authorization": self.token["access_token"],
        }), json=data) as resp:
            print(resp.status_code)
            print(resp.text)
            self.data = resp.json()
        params = self.data["resumable"]["params"]
        self.upurl = f"https://{params['bucket']}.{params['endpoint']}/{params['key']}"
        self.params = params

    def upload(self, file_path):
        auth = oss2.StsAuth(self.params['access_key_id'], self.params['access_key_secret'], self.params["security_token"])
        bucket = oss2.Bucket(auth, self.params['endpoint'], self.params['bucket'])
        # a = bucket.put_object_from_file(self.params['key'], file_path)
        a = bucket.put_object(self.params['key'], file_path)
        print(a.headers)
        print(a.status)
        print(a.request_id)

    def upload_tmp(self, ContentType):
        url = f"{self.upurl}?uploads="
        ossdate = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        string_to_sign = f"PUT\n\n\n{ossdate}\n/{self.params['bucket']}/{self.params['key']}"
        print(string_to_sign)
        authorization = f"OSS {self.params['access_key_id']}:signature"
        with requests.post(url=url, headers=dict(self.headers, **{
            "Host": url.split("/")[2],
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "Content-Type": ContentType,
            # "Content-Length": "0",
            "x-oss-date": ossdate,
            "x-oss-user-agent": "aliyun-sdk-js/6.17.1 Firefox 125.0 on Windows 10 64-bit",
            "x-oss-security-token": self.params['security_token'],
            "authorization": authorization,
        })) as resp:
            print(resp.status_code)
            print(resp.headers)
            print(resp.text)
            root = ET.fromstring(resp.text)
            upload_id = root.find('UploadId').text
            return upload_id

    def upload_tmp2(self, content, PartNumber, uploadId):
        url = f"{self.upurl}?partNumber={PartNumber}&uploadId={uploadId}"
        with requests.put(url=url, headers=dict(self.headers, **{
            "Host": url.split("/")[2],
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "x-oss-date": datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT'),
            "x-oss-user-agent": "aliyun-sdk-js/6.17.1 Firefox 125.0 on Windows 10 64-bit",
            "x-oss-security-token": self.params['security_token'],
            "authorization": f"OSS {self.params['access_key_id']}:DTso9XQDGXLS6EvI4hrEJWSFZJY=",
        }), data=content) as resp:
            print(resp.status_code)
            return resp.headers["ETag"]

    def upload_tmp3(self, etag, PartNumber, uploadId):
        url = f"{self.upurl}?uploadId={uploadId}"
        data = """<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUpload>
<Part>
<PartNumber>{}</PartNumber>
<ETag>"{}"</ETag>
</Part>
</CompleteMultipartUpload>""".format(PartNumber, etag)
        with requests.post(url=url, headers=dict(self.headers, **{
            "Host": url.split("/")[2],
            "Content-Type": "application/xml",
            "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
            "x-oss-date": datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT'),
            "x-oss-user-agent": "aliyun-sdk-js/6.17.1 Firefox 125.0 on Windows 10 64-bit",
            "x-oss-security-token": self.params['security_token'],
            "authorization": f"OSS {self.params['access_key_id']}:DTso9XQDGXLS6EvI4hrEJWSFZJY=",
            # "Content-MD5": "QQY+qcpA1xnb3FFeY7ovDw=="
        }), data=data) as resp:
            print(resp.status_code)
            print(resp.text)


def calculate_sha1(filepath, chunk_size=10240):
    sha1 = hashlib.sha1()
    content = b""
    with open(filepath, "rb") as fp:
        while chunk := fp.read(chunk_size):
            sha1.update(chunk)
            content += chunk
    return sha1.hexdigest(), content


def main():
    pikpak = Pikpak()
    filename = filepath.split("\\")[-1]
    print(filename)
    hashhex, content = calculate_sha1(filepath)
    pikpak.files(filename, len(content), hashhex)
    pikpak.upload(content)


    # contentype = "image/png"
    # upload_id = pikpak.upload_tmp(contentype)
    # PartNumber = 1
    # etag = pikpak.upload_tmp2(content, PartNumber, upload_id)
    # pikpak.upload_tmp3(etag, PartNumber, upload_id)


if __name__ == '__main__':
    main()

# domain_key_list = {"https://vod0051-aliyun18-vip-lixian.mypikpak.com",
#                    			"https://vod0037-aliyun17-vip-lixian.mypikpak.com",
#                    			"https://vod0039-aliyun17-vip-lixian.mypikpak.com",
#                    			"https://vod0038-aliyun17-vip-lixian.mypikpak.com",
#                    			"https://vod0049-aliyun18-vip-lixian.mypikpak.com",
#                    			"https://vod0050-aliyun18-vip-lixian.mypikpak.com",
#                    			"https://vod0041-hwyun02-vip-lixian.mypikpak.com",
#                    			"https://vod0042-hwyun02-vip-lixian.mypikpak.com",
#                    			"https://vod0043-hwyun02-vip-lixian.mypikpak.com"}

# url = "https://user.mypikpak.com/v1/shield/captcha/init"
# https://user.mypikpak.com/v1/auth/token

