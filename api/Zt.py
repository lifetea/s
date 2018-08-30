import pandas as pd
from scrapy.Base import base
import pymongo
# 股票追踪
class Zt:
    def __init__(self):
        self.client = pymongo.MongoClient('localhost', 27017)
        self.db = self.client['stock']
        self.collection = self.db['zt']
        pass
    def save(self,item):
        collection = self.collection
        try:
            res = collection.find_one({'code': item['code'],'date': item['date']})
            print(res)
            if res != None:
                collection.update({'code': item['code'],'date': item['date']},item)
            else:
                collection.save(item)
        except Exception as e:
            print(e)
        else:
            print("成功")
        pass

    def clear_zt_list(self,startDate,endDate):
        collection = self.collection
        stocks = pd.DataFrame(columns=('代码', '名称', '日期', '备注', '状态'))
        zts = base.get_zt_list(startDate=startDate,endDate=endDate)
        industry = base.get_industry_list()
        # print(zts)
        for code in zts.index:
            item = {
                '代码': code,
                '名称': zts.name[code],
                '日期': zts.date[code],
                # '隐单': j['yin'],
                # '主单': j['zhu'],
                '原因': zts.reason[code],
                '状态': zts.status[code],
                '备注': zts.remark[code],
                '行业': industry.industry[code],
                '概念': industry.concept[code]
            }

            if 'ST' in zts.name[code]:
                item = {
                    'code': code,
                    'date': zts.date[code],
                    'status': -1,
                    'remark': 'ST'
                }
                try:
                    collection.update({'code': item['code'], 'date': item['date']}, {"$set": item})
                except Exception as e:
                    print(e)
                else:
                    print("ST")
                pass

            if zts.date[code] == endDate:
                pass
            else:
                s = base.get_stock_base(code=code,startDate=zts.date[code],endDate=endDate)
                s = s.set_index(['日期'])
                # print(code)
                if len(s['成交额'].values) >= 2:
                    # print(s['成交额'])
                    pre = s['成交额'].values[-1]
                    next = s['成交额'].values[-2]
                    zf = s['涨幅'].values[-2]
                    # print(pre, next, zf)
                    if float(zf) < 9 and float(next)/float(pre) >= 1.6:
                        print(code,'第二天放量')
                        item={
                            'code': code,
                            'date': zts.date[code],
                            'status': -1,
                            'remark': '放量'
                        }
                        try:
                            collection.update({'code': item['code'], 'date': item['date']}, {"$set":item})
                        except Exception as e:
                            print(e)
                        else:
                            print("成功")
                        pass
            stocks = stocks.append(item, ignore_index=True)
        pass
zt = Zt()
# zt.clear_zt_list(startDate='2018-06-29',endDate='2018-07-10')
