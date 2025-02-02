# encoding='utf-8

# @Time: 2024-01-14
# @File: %
#!/usr/bin/env
from icecream import ic
import os
from curl_cffi import requests
import random
from fake_useragent import UserAgent
from get_porxy import get_proxy
from curl_cffi.requests import AsyncSession


class GenCookie(object):
    def __init__(self):
        self.headers = {
            'authority': 'www.ozon.ru',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'zh-CN,zh;q=0.9',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "Google Chrome";v="116"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36',
        }
        self.uaa = [
            "edge99",
            "edge101",
            "chrome99",
            "chrome100",
            "chrome101",
            "chrome104",
            "chrome107",
            "chrome110",
            "chrome116",
            "chrome119",
            "chrome120",
            "chrome99_android",
            "safari15_3",
            "safari15_5",
            "safari17_0",
            "safari17_2_ios",
            "chrome120",
            "safari17_0",
            "safari17_2_ios"
        ]
        self.session = requests.Session()
        # self.session.headers.update(self.headers)
    async def gen_cookie(self):
        async with AsyncSession() as session:
            ua = random.choice(self.uaa)
            ic(ua)
            self.headers['user-agent'] = UserAgent().random

            try:
                response = await session.get('https://www.ozon.ru/', impersonate=ua, headers=self.headers, timeout=30)
                response = await session.post('https://www.ozon.ru/abt/result', impersonate=ua, headers=self.headers, timeout=30)

                if "abt_data" not in response.cookies:
                    ua = random.choice(self.uaa)
                    response = await session.get('https://www.ozon.ru/', impersonate=ua, headers=self.headers, timeout=30)
                    if "abt_data" not in response.cookies:
                        print("获取cookie二次失败")
                else:
                    print("获取cookie成功")

                cookies = response.cookies
                ic(cookies)
                return cookies, ua, self.headers
            except Exception as e:
                print(f"获取cookie出错: {e}")
                return None, None, None

