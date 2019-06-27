from pymongo import MongoClient
from init import logger_init, config_init

logger = logger_init('上交所 数据抓取')
config = config_init()
if config['mongodb']['dev_mongo'] == '1':
    db = MongoClient(config['mongodb']['ali_mongodb_url'], username=config['mongodb']['ali_mongodb_username'],
                     password=config['mongodb']['ali_mongodb_password'],
                     port=int(config['mongodb']['ali_mongodb_port']))[config['mongodb']['ali_mongodb_name']]

    dev_db = MongoClient(config['mongodb']['ali_mongodb_url'], username=config['mongodb']['ali_mongodb_username'],
                         password=config['mongodb']['ali_mongodb_password'],
                         port=int(config['mongodb']['ali_mongodb_port']))[
        config['mongodb']['dev_mongodb_db_name']]

    touzhiwang_db = MongoClient(config['mongodb']['ali_mongodb_url'],
                                username=config['mongodb']['ali_mongodb_username'],
                                password=config['mongodb']['ali_mongodb_password'],
                                port=int(config['mongodb']['ali_mongodb_port']))[
        config['mongodb']['szse_mongodb_db_name']]
else:
    db = MongoClient(
        host=config['mongodb']['mongodb_host'],
        port=int(config['mongodb']['mongodb_port']),
        username=None if config['mongodb']['mongodb_username'] == '' else config['mongodb']['mongodb_username'],
        password=None if config['mongodb']['mongodb_password'] == '' else config['mongodb']['mongodb_password'])[
        config['mongodb']['mongodb_db_name']]

    dev_db = MongoClient(
        host=config['mongodb']['dev_mongodb_host'],
        port=int(config['mongodb']['dev_mongodb_port']),
        username=None if config['mongodb']['dev_mongodb_username'] == '' else config['mongodb']['dev_mongodb_username'],
        password=None if config['mongodb']['dev_mongodb_password'] == '' else config['mongodb'][
            'dev_mongodb_password'])[
        config['mongodb']['dev_mongodb_db_name']]

    touzhiwang_db = MongoClient(
        host=config['mongodb']['dev_mongodb_host'],
        port=int(config['mongodb']['dev_mongodb_port']),
        username=None if config['mongodb']['dev_mongodb_username'] == '' else config['mongodb']['dev_mongodb_username'],
        password=None if config['mongodb']['dev_mongodb_password'] == '' else config['mongodb'][
            'dev_mongodb_password'])[
        config['mongodb']['szse_mongodb_db_name']]

# 抓取数据存入sse_data这个collection
db.sse_data.create_index([('url', 1)])


def sse_crawler():
    for document in dev_db.sse_mongo.find(
            {'$or': [{'fileSubCategory': '监管措施'}, {'fileCategory': '债券监管', 'fileSubCategory': '纪律处分'}]}):

        announcement_url = str(document['adjunctUrl']).replace('.PDF', '.pdf')
        announcement_title = document['announcementTitle']

        if document.get('anounceType', '') != '':
            announcement_type = document.get('anounceType', '')
        else:
            if len(document.get('types', [])) > 0:
                announcement_type = '，'.join(document.get('types', []))
            else:
                announcement_type = document['fileSubCategory']

        if 'fileCheckSum' in document.keys():
            file_check_sum = document['fileCheckSum']
            if db.sse_data.find({'fileCheckSum': file_check_sum}).count() == 0 and \
                    db.sse_data.find({'url': announcement_url}).count() == 0:
                db.sse_data.insert_one({
                    'title': announcement_title,
                    'url': announcement_url,
                    'type': announcement_type,
                    'publishDate': document['webStorageTime'],
                    'origin': '上交所',
                    'fileCheckSum': file_check_sum,
                    'status': 'not parsed'
                })
                logger.info('上交所 新数据：' + announcement_url)
            else:
                if db.sse_data.find({'url': announcement_url}).count() == 0:
                    db.sse_data.insert_one({
                        'title': announcement_title,
                        'url': announcement_url,
                        'type': announcement_type,
                        'publishDate': document['webStorageTime'],
                        'origin': '上交所',
                        'fileCheckSum': file_check_sum,
                        'status': 'ignored'
                    })
                    logger.info('上交所 重复数据：' + announcement_url)
                else:
                    if db.sse_data.find({'fileCheckSum': file_check_sum}).count() == 0:
                        db.sse_data.update_one({'url': announcement_url},
                                               {
                                                   '$set': {'fileCheckSum': file_check_sum}
                                               })
                        logger.info('上交所 重复数据 更新fileCheckSum：' + announcement_url)
                    else:
                        continue
        else:
            if db.sse_data.find({'url': announcement_url}).count() == 0:
                db.sse_data.insert_one({
                    'title': announcement_title,
                    'url': announcement_url,
                    'type': announcement_type,
                    'publishDate': document['webStorageTime'],
                    'origin': '上交所',
                    'status': 'not parsed'
                })
                logger.info('上交所 新数据：' + announcement_url)
            else:
                continue


if __name__ == "__main__":
    sse_crawler()
