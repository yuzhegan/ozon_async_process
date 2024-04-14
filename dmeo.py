# encoding='utf-8

# @Time: 2024-04-14
# @File: %
#!/usr/bin/env
import polars as pl
from pymongo import MongoClient

# 连接到MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['ozon']  # 替换为你的数据库名
collection = db['ozon_products']  # 替换为你的集合名


def remove_duplicates():
    processed_ids = set()
    duplicates_count = 0

    # 遍历集合中的每个文档
    for document in collection.find({}):
        # print(processed_ids)
        # print(document)
        document_id = document['ID']  # 假设ID是文档中的唯一标识符字段

        # 检查ID是否已处理
        if document_id in processed_ids:
            # 如果已处理，删除重复的文档
            collection.delete_one({'_id': document['_id']})
            duplicates_count += 1
        else:
            # 如果未处理，则添加ID到已处理集合中
            processed_ids.add(document_id)

    return duplicates_count

# duplicates_removed = remove_duplicates()
# print(f"删除了 {duplicates_removed} 个重复的文档。")

# 获取集合中所有文档的"ID", "Imageurl"


def get_ids_and_image_urls():
    results = collection.find({}, {'_id': 0, 'ID': 1, 'Imageurl': 1}
                              )
    df_csv = pl.read_csv('/home/dav/Github/ozon_mangodb/ozon_test_output.csv',
                         dtypes={
                             "可用性 (%)": pl.Float64,
                             "因缺货而错过的订单金额（₽）": pl.Utf8,
                         }

                         )
    mongo_df = pl.DataFrame(results)
    print(len(mongo_df))
    df = df_csv.join(mongo_df, how='left', on='ID',
                     left_on='ID', right_on='ID', suffix='_x')
    df = df.filter((pl.col('Imageurl').is_null())
                   )

    df.write_csv('./drop_noimg_duplicates.csv')
    print("Done!")


get_ids_and_image_urls()
