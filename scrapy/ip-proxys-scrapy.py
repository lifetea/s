import random

import datetime
import time
from http.client import RemoteDisconnected

import numpy as np
import tushare as ts
from bs4 import BeautifulSoup
import requests
from pyjsparser import PyJsParser
import json
import pymongo
import pandas as pd
import re
from multiprocessing.dummy import Pool as ThreadPool

from requests import HTTPError, ConnectTimeout
from requests.exceptions import ChunkedEncodingError
from urllib3.exceptions import ConnectTimeoutError, ProxyError, MaxRetryError, NewConnectionError

import base

#数据库信息
client      = pymongo.MongoClient('localhost',27017)
db          = client['stock']
collection  = db['proxy']
Detail   = {
    'count':0,
    'total':0
}

def ip_scrapy():
    # 另外一种随机设置请求头部信息的方法
    my_headers = base.Base.get_headers()
    header = random.choice(my_headers)    #random.choice()可以从任何序列，比如list列表中，选取一个随机的元素返回，可以用于字符串、列表、元组

    url = 'http://www.xicidaili.com/nn/'

    r = requests.get(url=url, headers= {'User-Agent': header})

    # print(url)


    demo = r.text

    # print(demo)
    # print('开始爬取'+code)

    soup = BeautifulSoup(demo,'html.parser')    #解析器：html.parser

    trs = soup.select("#ip_list  tr")

    for tr in trs:
        tds = tr.select("td")
        # print(tds)

        if len(tds) == 0:
            continue
        ip      = tds[1].get_text(strip=True)
        port    = tds[2].get_text(strip=True)
        type    = tds[5].get_text(strip=True).lower()
        speed   = float(tds[7].select('div')[0].get('title')[0:-1])
        if speed > 3:
            continue
        item = {
            'ip':ip,
            'port':port,
            'type':type,
            'speed':speed,
            'status':1
        }
        print(item)
        if collection.find_one({"ip": ip, "port": port,'type':type}) == None:
            collection.save(item)
        else:
            print("已经存在")
            break

    pass




def get_proxy_list():
    query = {
        'status':1
    }
    ip_list = np.array(list(collection.find(query)))
    return ip_list
    pass

def detect_proxy(proxy):

    type = proxy['type'].lower()
    if type == 'https':
        url = 'https://icanhazip.com/'
    else:
        url = 'http://icanhazip.com/'

    proxys = {
        type:type + '://' + proxy['ip'] + ':' + proxy['port']
    }
    print(url)
    print(proxys)
    try:
        r = requests.get(url=url,proxies=proxys,timeout=20)
        r.encoding = r.apparent_encoding
        # print(obj)
    except (TimeoutError,ConnectTimeoutError,ConnectTimeout,NewConnectionError,requests.exceptions.ProxyError):
        print('出错')
        collection.update({'ip': proxy['ip'],'port':proxy['port'],'type':proxy['type']}, {
            '$set':
                {
                    'status': 0,
                }
        })
    except Exception as e:
        print(e.__class__)
        print(e)
    else:
        # 您的IP地址
        print(r.status_code)
        print(proxy['ip'])
        print(r.text)
        if re.match(r"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$", r.text):
            print("可用")
        else:
            collection.update({'ip': proxy['ip'], 'port': proxy['port'], 'type': proxy['type']}, {
                '$set':
                    {
                        'status': 0,
                    }
            })
            print(proxy['ip']+"不可用")

    pass

def batch_detect_proxy():
    ip_list = get_proxy_list()
    pool = ThreadPool(10)#创建10个容量的线程池并发执行
    pool.map(detect_proxy, ip_list)
    pool.close()
    pool.join()
    pass

# ip_scrapy()

batch_detect_proxy()

# detect_proxy()