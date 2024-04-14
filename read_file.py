# encoding='utf-8

# @Time: 2024-03-30
# @File: %
#!/usr/bin/env

import asyncio
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import pymongo

class AsyncReadFile:
    def __init__(self, file_path):
        self.file_path = file_path
        self.db = pymongo.MongoClient('mongodb://localhost:27017/')
        self.db = self.db['ozon']
        self.collection = self.db['ozon_product_unimg']  #存储未匹配图片的商品

        
    async def read_file(self):
        loop = asyncio.get_event_loop()
        if self.file_path is not None:
            if self.file_path.endswith('.csv'):
                self.df = await loop.run_in_executor(None, pd.read_csv, self.file_path)
            elif self.file_path.endswith('.xlsx'):
                self.df = await loop.run_in_executor(None, pd.read_excel, self.file_path)
        else:
            docs = list(self.collection.find({},{"_id":0}).limit(30000).skip(0))  #获取图片链接为空的数据 不带_id
            self.df = pd.DataFrame(docs)
            print(len(self.df))
        return self.df

            

# if __name__ == '__main__':
#     file_path = ''
#     async_read_file = AsyncReadFile(file_path)
#     df = asyncio.run(async_read_file.read_file())
