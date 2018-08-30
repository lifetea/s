import random

import os
import tushare as ts
from bs4 import BeautifulSoup
import requests
import json
import pymongo
import pandas as pd
import datetime
import re
from scrapy.conf import Conf
from scrapy.Base import Base
from multiprocessing.dummy import Pool as ThreadPool


class HGT:
    # 爬虫模式初始化 end为指定时间
    # 查询模式初始化
    def __init__(self,start=None,end=None):
        #header 信息
        self.headers = Conf.get_headers()
        # 数据库信息
        self.client     = pymongo.MongoClient('localhost', 27017)
        self.db         = self.client['stock']
        self.collection = self.db['fund']
        self.start      = start
        self.end        = end
        self.stocks     = pd.DataFrame(columns=('代码', '名称'))
        self.index      = 0
        self.total      = 0

        #沪港通列表
        date_array = self.end.split("-")
        self.form_data = {
            'ddlShareholdingDay': date_array[2],
            'ddlShareholdingMonth': date_array[1],
            'ddlShareholdingYear': date_array[0],
            '__VIEWSTATE': 'me5FDlt6v6yMfIg1wmifVy5QInLwvkMzU3yuVRJA9xtEDTfqbtXrXs22JZEyrlf1SXAQATd+silmY3tacgNujZV7i7c8VL4qYIurWHKmYsgXeM1zV5zGt9EKZgJHxDP74Yrdooj/8+NOSrnSl332l6rGVS8/FV3grrxV+o5cWp5+aWksBEuXybYabMT95NGq6DDGwg==',
            '__VIEWSTATEGENERATOR': 'EC4ACD6F',
            '__EVENTVALIDATION': 'tKjcTCN0J4XchUr2eevWU2RB1CVoMOqYGVjtQolJjuvMwrN+ovZzs2YK4gYO5EkAtvCjq7MT4kDFzcoIxXzyn4xq6pP/so8J2Jzas4f6Cj3pRobr3btKPl33l//8qDmOQa3ksi0upVynjukeV85yJalguExYDQIxh3GELA35EURWGQ9B9M/T7ZgI8CJtiWtiOIvdRKyTFUv0ktS/cvgN8p00O8HxZILs7l/JiPY+SEKzHmx7UC7bO1ixQEkVetqAUbAmdDNVsiDldUgM8wI5cJEcOYn7lnpvm1HkSx40hdKZn+SjGYFIj1Z4hI8NfD/FS30J9KjKKiHMnZvBgRfdFfiY2kw3VT5JBPYpSmRx60nQ8D9htYubzEH7uaXyAdyhbLLcFPkDaF6QejwKBpZSvw6XJjfJHLvPdivXzFOGqLiiv9F/JGxRBMZgpwD+bfnuZ0b22aaoztGJaIHHkJ1q1oG0yAaBSivFnoSm2wdt8277Y3OfTA3OfMXUc2xVelEF+PQff9rwWVyu1tmKDeEXb82YJ2aAw/G5rnlv2ipoV7RXIWx+jAWLmUohdLOPv0S+S0Cro6PckhBCZrpvrfzn7gJLLFx3IRkgc/BrePiA2PxxqFrN1/pTGVw18hRTiZYbliVqV0w8IoVQXDtDmc+eCODUUAge/nHv4umlUCrvzp9MBZEzDP2w1vRSnh+idyBCk/A1kRGvK9FRqDFfXcpA6jq+wie0/TuJlAsf63KpvxkzvMBCEuEKXjA5867K0m8/KDBO6HTTjuYLyQOx1AOyYTKgZ6Bz1pMkI6ZxfuZDt6/hOtLWNJKJMnQPPQlf4i5JQFtlaXEhkGIXsTSitEmafC8jKu5wsukxxxnF8BopAfVsIcMtBcf3CXpAtyu1cfjNmp4BpWmyQjIgGOz34y/NfkWw9fLxT5H9J06Qzi/GFZpn6HEWPwLVZgDgWEuC4TlN8DsFj84eJiikHVnnA8tyoHBcTYAdq3lNagat7rHwxLRHmDkjMTFj17NPkzV/6N/eeqJ5ZpqeEOaTV/Jysc/SuK7ybKUnGnxbq+zynTg4rc0oCrh6GMRRurxWvKxh2rl4BWKKlqVB4ZyFrSTNLPRipo5s97DwR7gAIhaa+jVWKIfUKB2W5LG0otaQtWIObF98BXnwn99WOJeg79/li7n92hVwTGk=',
            'sortBy': '',
            'alertMsg': '',
            'btnSearch.x': 26,
            'btnSearch.y': 6
        }
        # 沪港通明细
        self.token      = '70f12f2f4f091e459a279469fe49eca5'
        self.collection_detail = self.db['detail']
        self.page_size  = str(10000)
        pass

    # 爬虫模式初始化
    # def __init__(self,start_time,end_time):
    #     # 数据库信息
    #     self.client     = pymongo.MongoClient('localhost', 27017)
    #     self.db         = self.client['stock']
    #     self.collection = self.db['detail']
    #     self.start      = start_time
    #     self.end        = end_time
    #     self.token      = '70f12f2f4f091e459a279469fe49eca5'
    #     self.page_size  = str(10000)
    #     self.headers    = Conf.get_headers()
    #     self.total      = 0
    #     self.count      = 1
    #     self.stocks = pd.DataFrame(columns=('代码','机构'))
    #     pass


    #爬取数据
    def scrapy_stock_tasks(self):
        self.url = 'http://sc.hkexnews.hk/TuniS/www.hkexnews.hk/sdw/search/mutualmarket_c.aspx?t='
        tasks = ['sh','sz']
        pool = ThreadPool(2)  # 创建10个容量的线程池并发执行
        pool.map(self.scrapy_stock, tasks)
        pool.close()
        pool.join()
        pass

    def scrapy_stock(self,type):
        url = self.url+type
        header = random.choice(self.headers)
        r = requests.post(url=url, headers={'User-Agent': header},data=self.form_data)
        demo = r.text
        soup = BeautifulSoup(demo, 'html.parser')  # 解析器：html.parser

        trs = soup.select("#pnlResult .result-table tr.row0,#pnlResult .result-table tr.row1")
        items = []
        for ele in trs:
            tds = ele.select("td")
            code = tds[0].get_text(strip=True)
            if code[0] == "9":
                code = "60" + code[1:]
            if code[0] == "7" and code[1] == "7":
                code = "300" + code[2:]
            if code[0] == "7" and code[1] != "7":
                code = "00" + code[1:]
            name = tds[1].get_text(strip=True)
            hold = float(tds[2].get_text(strip=True).replace(",", ""))
            rate = float(tds[3].get_text(strip=True).replace("%", ""))
            item = {
                'code': code,
                'name': name,
                'hold': hold,
                'rate': rate,
                'date': self.end
            }
            items.append(item)
        try:
            self.collection.insert_many(items,ordered=True)
        except Exception as e:
            print(e)
        else:
            print("爬取"+type+"成功" + self.end)
        pass

    #获取单只股票
    def query_single(self,code):
        query = {
            'date': {
                '$gte': self.start,
                '$lte': self.end
            },
            'code': code
        }

        data = pd.DataFrame(list(self.collection.find(query)))

        del data['_id']

        data = data.sort_values(by=['date'], ascending=False)
        return data
        pass

    #获取股票列表
    def get_stock_list(self):
        query = {
            'date': {
                '$gte': self.start,
                '$lte': self.end
            },
            'rate':{
                '$gte': 1.2
            }
        }
        stock_list = list(self.collection.find(query).distinct('code'))
        return stock_list
        pass

    #获取多只股票
    def query_multiple(self):
        file_name = '../files/hgt/' + self.start + '|' + self.end + '.xlsx'
        if not os.path.exists(file_name):
            base = Base()
            industry = base.get_industry_list()
            stock_list = self.get_stock_list()
            self.total =  len(stock_list)
            for i,code in enumerate(stock_list):
                list = self.query_single(code)
                rates = list.rate.values
                holds = list.hold.values
                dates = list.date.values
                names = list.name.values
                pName = self.get_max_hold_name(code)
                if str(code) in industry.industry:
                    pass
                else:
                    print('行业里没有'+str(code))
                    continue
                item = {
                    '代码': code,
                    '名称': names[0],
                    '主力': pName,
                    '行业': industry.industry[str(code)],
                    '概念': industry.concept[str(code)]
                }

                for i, d in enumerate(dates):
                    item[d] = rates[i]
                self.index += 1
                print(str(self.index)+"/"+str(self.total))
                s = pd.Series(item)
                self.stocks = self.stocks.append(s, ignore_index=True)
            file_name = '../files/hgt/' + self.start + '|' + self.end + '.xlsx'
            writer = pd.ExcelWriter(file_name)
            self.stocks.to_excel(writer, 'Sheet1')
            writer.save()
        else:
            print("存在")
            self.stocks = pd.read_excel(file_name,dtype={'代码':str})
            # self.stocks = self.stocks.set_index(['代码'])
        return self.stocks
        pass


    # 这个是沪港通持股明细

    #获得持有最多的
    def get_max_hold_name(self,code):
        client     = pymongo.MongoClient('localhost', 27017)
        db         = client['stock']
        collection = db['detail']
        query = {
            'code': code
        }
        sort = [('hold',-1),('date', -1)]
        res = collection.find(query, sort=sort,limit=10)


        data = pd.DataFrame(list(res))

        if data.empty:
            pName = '缺失'
        else:
            del data['_id']
            data = data.sort_values(by=['date'], ascending=False)
            pName = data.pName[0]
        return  pName


    # 这个是沪港通持股明细


    def scrapy_detail(self,code):

        if code[0] == '0' or code[0] == '3':
            type = 'HSGTSHHDDET'
        else:
            type = 'HSGTHHDDET'

        # 另外一种随机设置请求头部信息的方法

        header = random.choice(self.headers)  # random.choice()可以从任何序列，比如list列表中，选取一个随机的元素返回，可以用于字符串、列表、元组

        url = 'http://dcfm.eastmoney.com//em_mutisvcexpandinterface/api/js/get?type=' + type + '&token=' + self.token + '&st=HDDATE,SHAREHOLDPRICE&sr=3&p=1&ps=' + self.page_size + '&js=var%20wMshbIOE={pages:(tp),data:(x)}&filter=(SCODE=%27' + code + '%27)(HDDATE%3E=^' + self.start + '^%20and%20HDDATE%3C=^' + self.end + '^)&rt=50633638'
        try:
            r = requests.get(url=url, headers={'User-Agent': header})

            demo = r.text

            # print(demo)
            print('开始爬取' + code)

            soup = BeautifulSoup(demo, 'html.parser')  # 解析器：html.parser
            #
            content = soup.text
            pattern = re.compile(r'^var .*={pages:1,data:')  # 查找数字
            #
            content = re.sub(pattern, "", content)

            content = content[1:-2]

            pattern = re.compile(r'({.+?})')

            item_list = re.findall(pattern, content)

            items = []

            for val in item_list:
                ele = json.loads(val)
                # print(ele)
                date = ele['HDDATE'][0:10]
                code = ele['SCODE']
                pCode = ele['PARTICIPANTCODE']
                hold = ele['SHAREHOLDSUM']
                holdRate = ele['SHARESRATE']
                closePrice = ele['CLOSEPRICE']
                item = {
                    'code': code,
                    'name': ele['SNAME'],
                    'pCode': pCode,
                    'pName': ele['PARTICIPANTNAME'],
                    'date': date,
                    'hold': hold,
                    'price': closePrice,
                    'holdRate': holdRate
                }
                items.append(item)
            self.collection_detail.insert_many(items, ordered=False)
        except Exception as e:
            print(e)
            print('出错' + code)
        else:
            pass
        finally:
            self.index += 1
            print(str(self.index) + '/' + str(self.total) +'  '+ code + ' 结束爬取 ' )

        pass

    def get_active_stock_list(self):
        client     = pymongo.MongoClient('localhost', 27017)
        db         = client['stock']
        collection = db['fund']
        query = {
            'date':self.end,
            'rate':{
                '$gte': 0.8
            },
            # 'code':re.compile('^6.*')
        }

        stock_list = list(collection.find(query).distinct('code'))
        return stock_list
        pass

    def scrapy_detail_tasks(self,stock_list):
        self.total = len(stock_list)
        pool = ThreadPool(10)#创建10个容量的线程池并发执行
        pool.map(self.scrapy_detail, stock_list)
        pool.close()
        pool.join()
        pass


    def get_participant_list(self,code):
        query = {
            'date': {
                '$gte': self.start,
                '$lte': self.end
            },
            'code': code
        }
        res = self.collection_detail.find(query,sort=[('date', 1)]).distinct('pCode')
        p_list = list(res)
        return p_list
        pass

    def query_hold_detail(self,code):
        file_name = '../files/detail/' + code + '|' + self.start + '-' + self.end + '.xlsx'
        if not os.path.exists(file_name):
            p_list = self.get_participant_list(code)
            for i in p_list:
                query = {
                    'date': {
                        '$gte': self.start,
                        '$lte': self.end
                    },
                    'pCode': i,
                    'code': code
                }
                res = self.collection_detail.find(query)
                res_list = list(res)
                item = {
                    '代码': i,
                }
                for j in res_list:
                    item[j['date']] = j['hold']
                    item['名称'] = j['pName']
                    # print(j)
                    pass
                global stocks
                self.stocks = self.stocks.append(item, ignore_index=True)
            file_name = '../files/detail/' + code + '|' + self.start + '-' + self.end + '.xlsx'
            writer = pd.ExcelWriter(file_name)
            self.stocks.to_excel(writer, 'Sheet1')
            writer.save()
        else:
            print("存在")
            self.stocks = pd.read_excel(file_name, dtype={'代码': str})
            # self.stocks = self.stocks.set_index(['代码'])
        return self.stocks
        pass




