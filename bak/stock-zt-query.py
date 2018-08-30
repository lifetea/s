
import random

__name__ = 'stock-detail-scrapy'
import datetime
import time
import tushare as ts
from bs4 import BeautifulSoup
import requests
from pyjsparser import PyJsParser
import json
import pymongo
import pandas as pd
import re
from multiprocessing.dummy import Pool as ThreadPool
import base

#数据库信息
client      = pymongo.MongoClient('localhost',27017)
db          = client['stock']
collection  = db['zt']

def zt_scrapy(date):
    # 另外一种随机设置请求头部信息的方法
    my_headers = base.Base.get_headers()
    header = random.choice(my_headers)    #random.choice()可以从任何序列，比如list列表中，选取一个随机的元素返回，可以用于字符串、列表、元组

    url = 'http://www.iwencai.com/stockpick/load-data?typed=0&preParams=&ts=1&f=1&qs=result_original&selfsectsn=&querytype=stock&searchfilter=&tid=stockpick&w='+date+'+%E6%B6%A8%E5%81%9C&queryarea='

    cookies = {
        "PHPSESSID":"63aa05bad945b37d7d74843939aceb25",
        "cid":"63aa05bad945b37d7d74843939aceb251520148300",
        "ComputerID":"63aa05bad945b37d7d74843939aceb251520148300",
        "v":"AlpvI_i61M4eU1hiNEcC5yVcrQt4i9zSUANSD2TFBzu4nPS9TBsudSCfoIY3"
    }
    r = requests.get(url=url, headers= {'User-Agent': header},cookies=cookies)

    demo = r.text

    res = json.loads(demo)
    # print(res["data"]["result"]["result"])
    stock_list = res["data"]["result"]["result"]

    for s in stock_list:
        print(s[0])
        item={
            'code':s[0][0:6],
            'date':date,
            'name':s[1],
            'price':s[2],
            'zf':s[3],
            'sczt':s[5],
            'zzzt':s[6],
            'detail':s[7],
            'count':s[8],
            'reason':s[9],
            'fdl':s[10],
            'fde':s[11],
            'fcb':s[12],
            'flb': s[13],
            'kbs':s[14],
            'ssts':s[15]
        }
        if collection.find_one({"code": item['code'], "date": item['date']}) == None:
            collection.save(item)
            print("写入成功")
        else:
            print("已经存在")
            break

    pass




def get_yin_list(date,code):
    c = pymongo.MongoClient('localhost', 27017)
    db = c['stock']
    con = db['yin']
    print(code)
    query={
        'code':code,
        'date':{
            '$gte':date
        }
    }
    res = con.find(query)
    yin_list = list(res)

    return yin_list
    # yin_list = list(con.find({
    #     # 'date': {
    #     #     '$gte': time_start,
    #     #     '$lte': time_end
    #     # },
    #     'code': s['code']
    # }))
    # # print(yin_list)

    pass
def get_zt_l1ist(date):
    stocks = pd.DataFrame()
    query={
        'date':date
    }
    stock_list = list(collection.find(query))
    # print(stock_list)
    for s in stock_list:
        item = {
            '0代码': s['code'],
            '0名称': s['name'],
            '0日期': date,
            '0价格': s['price'],
            '0涨停天数':s['count'],
            '0涨停原因': s['reason'],
            '0封单量': s['fdl'],
            '0封单额': s['fde'],
            '0封成比': s['fcb'],
            '0封流比': s['flb'],
            '0开板数': s['kbs'],
            '0上市天数': s['ssts'],
            # '1主单': j['zhu'],
            # '2当天涨幅': j['zf'],
        }
        yin_list = get_yin_list(date,s['code'])
        for yin in yin_list:
            print(yin)
            item[yin['date']+'-price'] = yin['close']
            item[yin['date'] + '-zf'] = yin['zf']
            item[yin['date'] + '-zhu'] = yin['zhu']
            item[yin['date'] + '-yin'] = yin['yin']
            item[yin['date'] + '-volume'] = yin['volume']
            item[yin['date'] + '-low'] = yin['low']


        stocks = stocks.append(item, ignore_index=True)
    file_name = './zt/' + date + '.xlsx'
    writer = pd.ExcelWriter(file_name)
    stocks.to_excel(writer, 'Sheet1')
    writer.save()

# zt_scrapy("2018-02-26")
zt_scrapy("2018-03-01")