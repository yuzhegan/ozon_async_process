# encoding='utf-8
# @Time: 2023-12-30
# @File: %
#!/usr/bin/env

import asyncio
import multiprocessing
from curl_cffi import requests
from curl_cffi.requests import AsyncSession
from lxml import etree
import logging
from parseData import *
import os
from read_file import AsyncReadFile
import pymongo
import json
from cookie_pool import GenCookie
import random


class Ozon_Spider:
    def __init__(self, url):
        self.url = url
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

        self.db = pymongo.MongoClient('mongodb://localhost:27017/')
        self.db = self.db['ozon']
        self.collection = self.db['ozon_products']
        self.collection_no_image = self.db['ozon_product_unimg']

    async def searchResultsV2(self, cookies, ua, headers, session, row, columns_name, file_path):
        dict_data = {}
        for col in columns_name:
            dict_data[col] = row[col]
        ic(row['ID'])
        response = await session.get(self.url, cookies=cookies, headers=headers, impersonate=ua, timeout=60,
                                     allow_redirects=True)
        html = etree.HTML(response.text)
        # with open('ozon.html', 'w') as f:
        #     f.write(response.text)

        try:
            data = html.xpath('//script[contains(@type,"application")]/text()')
            data = json.loads(data[0])
            img = data['image']
        except Exception as e:
            data = html.xpath('//div[@class="xk1"]/div/div/img/@src')
            img = data[0] if data else ''
            if img == '':
                data = html.xpath('//div[@data-index="0"]//img/@src')
                img = data[0] if data else ''
                if img == '':
                    logging.info(f'Getting ZZZZZZZZZZZZZZZZZZZZZZ')
                    await asyncio.sleep(random.uniform(0.1, 0.3))
                    params = {
                        'deny_category_prediction': 'true',
                        'from_global': 'true',
                        # 'text': 'Корзина плетеная',
                        'product_id': str(row['ID']),
                    }
                    response = await session.get('https://www.ozon.ru/search/', cookies=cookies, headers=headers,params=params, impersonate=ua, timeout=60)
                    # with open('test.html', 'w') as f:
                    #     f.write(response.text)
                    html = etree.HTML(response.text)
                    data = html.xpath('//link[@as="image"]/@href')
                    img = data[0]
                    # logging.info(f'Getting ZZZZZZZZZZZZZZZZZZZZZZ URL: {img}')
                    # logging.info(f'Getting image URL: {img}')

        dict_data['image'] = img
        logging.info(f'Getting image URLAAAAAAAAAAAAAAA: {img}')
        if img != '':  # 有图片
            self.collection.insert_one(dict_data)
            if file_path is None:
                result = self.collection_no_image.delete_many(
                    {"ID": row['ID']})  # 获取到图片的这条数据要在数据库删除
                logging.info(f'MongoDB删除: {result.deleted_count}')
        elif (img == "" and file_path != None):  # 没有图片，且本地读取文件
            self.collection_no_image.insert_one(dict_data)
        # except Exception as e:
        #     logging.error(f'Error getting image URL: {e}')


async def main():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(levelname)s: %(message)s')

    gencookies = GenCookie()
    logging.info('Start getting cookies')
    cookies, ua, headers = await gencookies.gen_cookie()
    logging.info('Finish getting cookies')

    # file_path = r"/home/dav/Github/ozon_async_process/split_csvs/part_1.csv"
    file_path = None
    # file_path = r"./data.csv"
    async_read_file = AsyncReadFile(file_path)
    df = await async_read_file.read_file()

    manager = multiprocessing.Manager()

    async with AsyncSession() as session:
        tasks = []
        for index, row in df.iterrows():
            # await asyncio.sleep(0.2)
            await asyncio.sleep(random.uniform(0.1, 0.5))
            url = row['商品链接']
            ozon_spider = Ozon_Spider(url)
            task = asyncio.create_task(ozon_spider.searchResultsV2(
                cookies, ua, headers, session, row, df.columns, file_path))
            tasks.append(task)

        image_urls = await asyncio.gather(*tasks)

if __name__ == '__main__':
    asyncio.run(main())
