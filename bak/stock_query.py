import tushare as ts
from bs4 import BeautifulSoup
import requests
from pyjsparser import PyJsParser
import json
import pymongo
import pandas as pd
import datetime
import re
import numpy as np
import matplotlib.pyplot as plt
import openpyxl

#数据库信息
client      = pymongo.MongoClient('localhost',27017)
db          = client['stock']
collection  = db['fund']


time_start      = datetime.datetime(2017, 10, 1).strftime("%Y-%m-%d")
time_end        = datetime.datetime(2018, 3, 9).strftime("%Y-%m-%d")

global stocks     
stocks      = pd.DataFrame(columns=('代码', '名称'))


def get_max_hold_name(code):
    c = db['detail']
    query = {
        'code': code
    }
    sort = [('date', -1)]
    res = c.find(query, sort=sort, limit=10)




    # print(list(res))

    data = pd.DataFrame(list(res))

    if data.empty:
        pName = '缺失'
        print(code)
    else:
        del data['_id']
        data = data.sort_values(by=['date'], ascending=False)
        pName = data.pName[0]

    return  pName





def query_stock(code,date):

    query = {
        'date': {
            '$gte': time_start,
            '$lte': time_end
        },
        'code':code
    }

    res  = collection.find(query)
    data = list(res)
    # data = pd.DataFrame(list(collection.find(query)))

    # del data['_id']

    # data = data.set_index(['date','rate'])

    # data = data.sort_values(by=['date'], ascending=False)

    # print(data.rate.values[0:5].mean())

    # rate_5 = data.rate.values[5].mean()

    # rate_m5  = data.rate.values[0:5].mean()

    # rate_m10 = data.rate.values[0:10].mean()

    # if rate_m5 > rate_5:
    # print(data[0:30])
    # print(data[30:60])
    # print(data[60:90])
    # print(data[90:120])
    return  data

def query_stock_list(code):

    query = {
        'date': {
            '$gte': time_start,
            '$lte': time_end
        },
        'code':code
    }

    data = pd.DataFrame(list(collection.find(query)))

    del data['_id']

    # data = data.set_index(['date','rate'])

    data = data.sort_values(by=['date'], ascending=False)

    # print(data.rate.values[0:5].mean())

    # rate_5 = data.rate.values[5].mean()

    # rate_m5  = data.rate.values[0:5].mean()

    # rate_m10 = data.rate.values[0:10].mean()

    # if rate_m5 > rate_5:
    return  data

# query_stock("600531")

# l = ["600216","601888","600009","601595","600531","601899","603816"]
#
#
# for i in l:
#     query_stock(i)
#


def query_detail(code):
    global stocks
    list  = query_stock_list(code)
    rates = list.rate.values
    holds = list.hold.values
    dates = list.date.values
    names = list.name.values

    # print(dates)
    pName   =    get_max_hold_name(code)

    item = {
        '代码': code,
        '名称': names[0],
        '主力': pName
    }
    # print(code)
    if "close" in list:
        # print(code)
        close = list.close.values
        # print(close)
    else:
        print(code+'不存在close')

    for i,d in enumerate(dates):
        item[d] = rates[i]
        if i<= 2 and "close" in list:
            # print(close[i])
            item['close'+d] = close[i]

    s  = pd.Series(item)

    stocks = stocks.append(s, ignore_index=True)
    # print(stocks)
    pass




#获取stock list
def get_stock_list():
    query = {
        'date': {
            '$gte': time_start,
            '$lte': time_end
        },
        'rate':{
            '$gte': 0.3
        }
    }
    res = collection.find(query).distinct('code')
    stock_list = list(res)
    # print(stock_list)

    return stock_list
    pass


def batch_stock_query(stock_list):
    total = len(stock_list)
    for i,code in enumerate(stock_list):
        query_detail(code)
        print(str(i)+"/"+str(total))

    file_name = '' + time_start + '|' + time_end + '.xlsx'
    writer = pd.ExcelWriter(file_name)
    stocks.to_excel(writer, 'Sheet1')
    writer.save()

bacth_flag = True

if bacth_flag == True:
    stock_list = get_stock_list()
    batch_stock_query(stock_list)
else:
    query_stock_list("603816")




