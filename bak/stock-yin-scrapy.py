
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
collection  = db['yin']
Detail   = {
    'count':0,
    'total':0
}
code            = '002463'



def stock_update_info(code,date):
    history = ts.get_k_data(code,start=date,end=date)
    if history.empty:
        return
    volume = history.volume.values
    close  = history.close.values
    low    = history.low.values
    open   = history.open.values
    collection.update({'code': code,'date':date}, {
        '$set':
            {
                'volume': volume[0],
                'close' : close[0],
                'low'   : low[0],
                'open'  : open[0],
            }
    })

    pass


def stock_detail_scrapy(code):
    if code[0] == '0':
        type = 'SZ'
    if code[0] == '3':
        type = 'SZ'
    if code[0] == '6':
        type = 'SH'

    # 另外一种随机设置请求头部信息的方法
    my_headers = base.Base.get_headers()
    header = random.choice(my_headers)    #random.choice()可以从任何序列，比如list列表中，选取一个随机的元素返回，可以用于字符串、列表、元组

    url = 'http://www.bestgo.com/topview/'+type+code+'.html'

    r = requests.get(url=url, headers= {'User-Agent': header})
    r.encoding = r.apparent_encoding
    # print(url)


    demo = r.text

    # print(demo)
    print('开始爬取'+code)
    #
    soup = BeautifulSoup(demo,'html.parser')    #解析器：html.parser

    items = []
    tbodys = soup.select(".tabPanel .panes tbody")
    trs_1 = tbodys[0].select("tr")
    trs_2 = tbodys[1].select("tr")
    trs_3 = tbodys[2].select("tr")
    name = soup.select("#stock_name a")[0].get_text(strip=True)
    for i,d in  enumerate(trs_1):
        tds = d.select("td")
        t    = tds[0].get_text(strip=True).split("-")

        date = datetime.datetime(2018, int(t[0]), int(t[1])).strftime("%Y-%m-%d")
        zf  = float(tds[2].get_text(strip=True))
        yin = float(tds[3].get_text(strip=True))

        item = {
            'date':date,
            'yin':yin,
            'zf':zf,
            'code':code,
            'name':name
        }
        items.append(item)

    for i, d in enumerate(trs_2):
        tds = d.select("td")
        zhu = float(tds[3].get_text(strip=True))
        items[i]["zhu"] = zhu

    for i, d in enumerate(trs_3):
        tds = d.select("td")
        zj = float(tds[3].get_text(strip=True))
        items[i]["zj"] = zj
    # print(items)

    for item in items:
        if collection.find_one({"code": item['code'], "date": item['date']}) == None:
            collection.save(item)
            stock_update_info(item['code'], item['date'])
            print("写入成功")
        else:
            print("已经存在")
            break
    Detail['count'] += 1
    print(str(Detail['count'])+'/'+str(Detail['total'])+' 结束爬取 ' + code)

    pass


def get_stock_list():
    stock_list = ts.get_stock_basics()
    return stock_list
    pass

def stock_batch_scrapy():
    stock_list = get_stock_list()

    s_list = stock_list.index

    # print(s_list)
    Detail['total'] = len(s_list)
    pool = ThreadPool(10)#创建10个容量的线程池并发执行
    pool.map(stock_detail_scrapy, s_list)
    pool.close()
    pool.join()
    # for s in s_list:
    #     stock_detail_scrapy(s)
    #     n = n -1
    #     print(n)
    pass

batch_flag = True

if batch_flag == True:
    stock_batch_scrapy()
else:
    stock_detail_scrapy("000615")
    pass



time_start      = datetime.datetime(2018, 2, 1).strftime("%Y-%m-%d")
time_end        = datetime.datetime(2018, 3, 9).strftime("%Y-%m-%d")
def stock_update_info(code):
    history = ts.get_k_data(code,start=time_start,end=time_end)
    volume = history.volume.values
    close  = history.close.values
    low    = history.low.values
    open   = history.open.values
    print(history)
    for i,date in enumerate(history.date):
        collection.update({'code': code,'date':date}, {
            '$set':
                {
                    # 'name': history.name[i],
                    'volume': volume[i],
                    'close' : close[i],
                    'low'   : low[i],
                    'open'  : open[i],
                }
        })
        # ma5 = history.close.values[0:4].mean()
        # print(ma5)
    pass

# stock_update_info("000615")