li = ['requests']
import os, importlib.util
for n in li:
    if importlib.util.find_spec(n) is None:
        os.system("pip install " + n)
import time
import requests
from concurrent.futures import ThreadPoolExecutor

session = requests.session()
headers = {
    "Host": "haijiao.pro",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
}


def test_ip(ip, x=0):
    # 设置代理'', '
    proxies = {
        "http": f"http://{ip}",
        "https": f"https://{ip}",
    }
    time.sleep(x*1.5)
    try:
        print(ip)
        # url = "https://haijiao.pro/api/captcha/request?captchaKey=signInCaptcha"
        # with session.get(url=url, headers=dict(headers, **{"Referer": "https://haijiao.pro/login"}), proxies=proxies) as resp:
        #     pass
        # url = "https://haijiao.pro/api/captcha/request?t=signupCaptcha"
        # with session.get(url=url, headers=dict(headers, **{"Referer": "https://haijiao.pro/register"}), proxies=proxies) as resp:
        #     pass
        user = (str(int(time.time())))
        data = {"username": user, "password": user, "rpassword": user, "referid": None, "locked": None}
        resp = session.post("https://haijiao.pro/api/login/signup", headers=dict(headers, **{
            "Content-Type": "application/json",
            "Origin": "https://haijiao.pro",
            "Referer": "https://haijiao.pro/register",
        }), json=data, verify=False, proxies=proxies, timeout=10)
        print(user)
        print(resp.status_code)
        print(resp.text)
        data = resp.json()
        data["user"] = user
        return True, data
    except requests.exceptions.ProxyError:
        print(1)
        return False, {}
    except requests.exceptions.ReadTimeout:
        print(2)
        return False, {}
    except requests.exceptions.SSLError:
        print(3)
        return False, {}
    except requests.exceptions.ConnectTimeout:
        print(4)
        return False, {}
    except requests.exceptions.ConnectionError:
        print(5)
        return False, {}
    except requests.exceptions.JSONDecodeError:
        print(6)
        return False, {}


if __name__ == '__main__':
    # ips = requests.get("https://raw.githubusercontent.com/parserpp/ip_ports/refs/heads/main/proxyinfo.txt").text
    ips = requests.get("https://raw.githubusercontent.com/Zaeem20/FREE_PROXIES_LIST/refs/heads/master/https.txt").text
    with open("hjtoken", "a") as f:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(test_ip, ip.replace("http://", ""), i, ): ip for i, ip in enumerate(ips.split("\n"))}
            for future in futures:
                flag, data = future.result()
                if flag:
                    token = data["data"]["token"]
                    user = data["user"]
                    print(user, token)
                    f.write(f"{user}@{token}\n")
                    f.flush()
    print("任务完成")

