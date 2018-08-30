import os
import pymongo

class Delete:
    def __init__(self):
        pass

    def delete_file(self,file_name):
        # file_name = '../files/detail/' + code + '|' + self.start + '-' + self.end + '.xlsx'
        try:
            if  os.path.exists(file_name):
                os.remove(file_name)
        except Exception as e:
            print(e)

        pass

    def delete_db(self,db_name,query):
        client     = pymongo.MongoClient('localhost', 27017)
        db         = client['stock']
        collection = db[''+db_name+'']
        try:
            collection.remove(query)
        except Exception as e:
            print(e)

        pass


delete = Delete()