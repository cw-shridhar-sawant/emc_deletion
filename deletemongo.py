#!/usr/bin/env python
import sys
import os
import pandas as pd
import pymongo
import json

def import_content(filepath):

    # SERVER CRDENTIALS TO RUN
    #link of proxy DB add here
    mng_client = pymongo.MongoClient({{link_proxy_mongodb}})#link to mongodb connection
    mng_db = mng_client['cloudwalkerdbapp']#Database bname to connect
    collection_name = 'authwalls'#collection name from where to remove docs

    db_cm = mng_db[collection_name]
    cdir = os.path.dirname(__file__)
    file_res = os.path.join(cdir, filepath)sssss

    data = pd.read_csv(file_res)
    data_json = json.loads(data.to_json(orient='records'))

    print("LOOP SATART----------------->")
    print('Number of colums in Dataframe : ', len(data.columns))
    print('Number of rows in Dataframe : ', len(data.index))
    for i in range(0, len(data.index)):
        print('EMAC==>'+data.iloc[i, 1]+"  mboard===>"+data.iloc[i, 4] + "  brand==>"+data.iloc[i, 0])
        query = {"emac": {"$regex": data.iloc[i, 1]}, "mboard": data.iloc[i, 4], "brand": data.iloc[i, 0]}
        d = db_cm.delete_one(query)
        print(d.deleted_count, " documents deleted !!")
        print(i, " NO documents deleted !!")


if __name__ == "__main__":
    directory_path = os.getcwd()
    print("My current directory is : " + directory_path)
    folder_name = os.path.basename(directory_path)
    print("My directory name is : " + folder_name)
    filepath = os.getcwd()+'/deletion_csv/{{filename_with_csv_ext}}'
    import_content(filepath)
