from scrapy.HGT import HGT
from scrapy.Yin import yin
from scrapy.HS import hs
from scrapy.WenCai import WenCai
from scrapy.LH import lh
class Spider:
    def __init__(self):
        # self.client     = pymongo.MongoClient('localhost', 27017)
        # self.db         = None
        pass

    def spider_base_list(self,endDate):
        wencai = WenCai(end=endDate)
        wencai.scrapy_base_tasks()

    def spider_hgt_list(self,endDate):
        hgt = HGT(end=endDate)
        hgt.scrapy_stock_tasks()

    def spider_hgt_detail(self,startDate,endDate,stock_list):
        hgt = HGT(start=startDate,end=endDate)
        hgt.scrapy_detail_tasks(stock_list)

    def spider_yin_list(self,endDate):
        yin.scrapy_yin_tasks(endDate)

    # 爬取隐单
    def spider_yin(self,code):
        yin.stock_yin_scrapy(code=code)
    # 爬取涨停
    def scrapy_zt(self, endDate):
        wencai = WenCai(end=endDate)
        wencai.scrapy_zt()
    # 区间涨停数据
    def scrapy_yin_zt_tasks(self, startDate,endDate):
        yin.scrapy_yin_zt_tasks(startDate=startDate,endDate=endDate)

    # 区间涨停资金数据
    def scrapy_zt_hs_tasks(self, startDate,endDate):
        hs.scrapy_zt_hs_tasks(startDate=startDate,endDate=endDate)

    #异动
    def spider_yd(self,endDate):
        wencai = WenCai()
        wencai.scrapy_yd(date=endDate)
    #爬取行业
    def spider_industry(self):
        wencai = WenCai()
        wencai.scrapy_industry_tasks()

    # 爬取龙虎
    def spider_hl_list(self,endDate):
        lh.stock_lh_list_scrapy(date=endDate)



spider = Spider()