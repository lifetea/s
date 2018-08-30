import pymongo
from api.Observe import observe
from api.Zt import zt

class Save:
    def __init__(self):
        self.client     = pymongo.MongoClient('localhost', 27017)
        self.db         = None
        pass


    def save_observe(self,item):
        observe.save(item)

    def save_zt(self,item):
        zt.save(item)

    def clear_zt(self,startDate,endDate):
        zt.clear_zt_list(startDate=startDate,endDate=endDate)


    def save_calendar(self,item):
        client     = pymongo.MongoClient('localhost', 27017)
        db         = client['stock']
        collection = db['日历']
        print(item)

        try:
            res = collection.find_one({'date': item['date']})
            if res != None:
                collection.update({'code': item['code']},item)
            else:
                collection.save(item)
        except Exception as e:
            print(e)
        else:
            print("成功")
        pass


save = Save()