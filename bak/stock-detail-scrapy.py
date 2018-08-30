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
collection  = db['detail']
Detail   = {
    'count':0,
    'total':0
}
time_start      = datetime.datetime(2018, 2, 2).strftime("%Y-%m-%d")
time_end        = datetime.datetime(2018, 3, 9).strftime("%Y-%m-%d")
code            = '002008'


def stock_detail_scrapy(code):
    page_size = str(10000)
    token = '70f12f2f4f091e459a279469fe49eca5'

    if code[0] == '0' or code[0]== '3':
        type = 'HSGTSHHDDET'
    else:
        type = 'HSGTHHDDET'

    # 另外一种随机设置请求头部信息的方法
    my_headers = base.Base.get_headers()
    header = random.choice(my_headers)    #random.choice()可以从任何序列，比如list列表中，选取一个随机的元素返回，可以用于字符串、列表、元组

    url = 'http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?type='+type+'&token='+token+'&st=HDDATE,SHAREHOLDPRICE&sr=3&p=1&ps=' + page_size + '&js=var%20wMshbIOE={pages:(tp),data:(x)}&filter=(SCODE=%27' + code + '%27)(HDDATE%3E=^' + time_start + '^%20and%20HDDATE%3C=^' + time_end + '^)&rt=50633638'

    r = requests.get(url=url, headers= {'User-Agent': header})

    # print(url)


    demo = r.text

    # print(demo)
    print('开始爬取'+code)

    soup = BeautifulSoup(demo,'html.parser')    #解析器：html.parser
    #
    content = soup.text
    pattern = re.compile(r'^var .*={pages:1,data:')   # 查找数字
    #
    content = re.sub(pattern,"",content)

    content = content[1:-2]

    pattern = re.compile(r'({.+?})')

    list = re.findall(pattern,content)


    global date_list

    date_list = []

    for val in list:
        ele = json.loads(val)
        # print(ele)
        date        =  ele['HDDATE'][0:10]
        code        =  ele['SCODE']
        pCode       =  ele['PARTICIPANTCODE']
        hold        =  ele['SHAREHOLDSUM']
        holdRate    =  ele['SHARESRATE']
        closePrice  =  ele['CLOSEPRICE']
        dict = {
            'code'  : code,
            'name'  : ele['SNAME'],
            'pCode' : pCode,
            'pName' : ele['PARTICIPANTNAME'],
            'date'  : date,
            'hold'  : hold,
            'price' : closePrice,
            'holdRate':holdRate
        }
        # print(collection.find_one({"code": code, "date": date,'pCode':pCode}))
        if collection.find_one({"code": code, "date": date,'pCode':pCode}) == None:
            collection.save(dict)
        else:
            print("已经存在")
            break
    Detail['count'] += 1
    print(str(Detail['count'])+'/'+str(Detail['total'])+' 结束爬取 ' + code)

    pass


def get_stock_list():
    collection = db['fund']
    query = {
        'date':"2018-03-09",
        'rate':{
            '$gte': 0.4
        },
        # 'code':re.compile('^6.*')
    }

    stock_list = list(collection.find(query).distinct('code'))
    return stock_list
    pass

def stock_batch_scrapy():
    stock_list = get_stock_list()


    s_list = stock_list

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

if batch_flag == False:
    stock_batch_scrapy()
else:
    stock_detail_scrapy("002035")
