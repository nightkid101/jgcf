import xlrd
from elasticsearch import Elasticsearch, exceptions
from pymongo import MongoClient
from init import config_init, logger_init
from bson import ObjectId

config = config_init()
es = Elasticsearch([config['Aliyun_ES']['host']], timeout=30)

logger = logger_init('校验完成数据 导入数据库')

if config['mongodb']['dev_mongo'] == '1':
    db = MongoClient(config['mongodb']['ali_mongodb_url'], username=config['mongodb']['ali_mongodb_username'],
                     password=config['mongodb']['ali_mongodb_password'],
                     port=int(config['mongodb']['ali_mongodb_port']))[
        config['mongodb']['ali_mongodb_name']]
else:
    db = MongoClient(
        host=config['mongodb']['mongodb_host'],
        port=int(config['mongodb']['mongodb_port']),
        username=None if config['mongodb']['mongodb_username'] == '' else config['mongodb']['mongodb_username'],
        password=None if config['mongodb']['mongodb_password'] == '' else config['mongodb']['mongodb_password'])[
        config['mongodb']['mongodb_db_name']]

count = 0
for each_document in db.announcement.find({'oss_file_id': ObjectId("5bb09705c66384b5ead3b9f1")}):
    if db.announcement.find(
            {
                'litigant': each_document['litigant'],
                'facts': each_document['facts'],
                'announcementCode': each_document['announcementCode'],
                'announcementDate': each_document['announcementDate']
            }).count() > 1:
        count += 1
        print(each_document['_id'])
        document_id = each_document['_id']
        db.announcement.delete_one({'_id': each_document['_id']})
        print('delete document from db')
        es.delete(index='punishment_dev', doc_type='document', id=str(document_id))
        print('delete document from es')
print(count)
