from pymongo import MongoClient
from init import logger_init, config_init

logger = logger_init('深交所 数据抓取')
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

# 抓取数据存入szse_data这个collection
db.szse_data.create_index([('url', 1)])


def szse_crawler():
    for each_document in dev_db.szse_mongo.find(
            {'$or': [{'fileSubCategory': '监管措施'}, {'fileCategory': '公司监管', 'fileSubCategory': '中介机构监管'},
                     {'fileCategory': '债券监管', 'fileSubCategory': '纪律处分'}]}):
        if 'adjunctUrl' in each_document.keys():
            announcement_url = str(each_document['adjunctUrl']).replace('.PDF', '.pdf')
            announcement_title = each_document['announcementTitle']

            if each_document['fileSubCategory'] == '中介机构监管':
                if '通报批评' in announcement_title:
                    announcement_type = '通报批评'
                elif '公开谴责' in announcement_title:
                    announcement_type = '公开谴责'
                elif '监管函' in announcement_title:
                    announcement_type = '监管措施'
                else:
                    announcement_type = ''
            else:
                if each_document.get('anounceType', '') != '':
                    announcement_type = each_document.get('anounceType', '')
                else:
                    if len(each_document.get('types', [])) > 0:
                        announcement_type = '，'.join(each_document.get('types', []))
                    else:
                        announcement_type = each_document['fileSubCategory']

            if 'fileCheckSum' in each_document.keys():
                file_check_sum = each_document['fileCheckSum']
                if db.szse_data.find({'fileCheckSum': file_check_sum}).count() == 0 and \
                        db.szse_data.find({'url': announcement_url}).count() == 0:
                    post = {
                        'title': announcement_title,
                        'url': announcement_url,
                        'type': announcement_type,
                        'publishDate': each_document['webStorageTime'],
                        'origin': '深交所',
                        'fileCheckSum': file_check_sum,
                        'status': 'not parsed'
                    }
                    if each_document.get('number', '') != '':
                        post['number'] = each_document.get('number', '')
                    db.szse_data.insert_one(post)
                    logger.info('深交所 新数据：' + announcement_url)
                else:
                    if db.szse_data.find({'url': announcement_url}).count() == 0:
                        post = {
                            'title': announcement_title,
                            'url': announcement_url,
                            'type': announcement_type,
                            'publishDate': each_document['webStorageTime'],
                            'origin': '深交所',
                            'fileCheckSum': file_check_sum,
                            'status': 'ignored'
                        }
                        if each_document.get('number', '') != '':
                            post['number'] = each_document.get('number', '')
                        db.szse_data.insert_one(post)
                        logger.info('深交所 重复数据：' + announcement_url)
                    else:
                        if db.szse_data.find({'fileCheckSum': file_check_sum}).count() == 0:
                            db.szse_data.update_one({'url': announcement_url},
                                                    {
                                                        '$set': {'fileCheckSum': file_check_sum}
                                                    })
                            logger.info('深交所 重复数据 更新fileCheckSum：' + announcement_url)
                        else:
                            continue
            else:
                if db.szse_data.find({'url': announcement_url}).count() == 0:
                    db.szse_data.insert_one({
                        'title': announcement_title,
                        'url': announcement_url,
                        'type': announcement_type,
                        'publishDate': each_document['webStorageTime'],
                        'origin': '深交所',
                        'status': 'not parsed'
                    })
                    logger.info('深交所 新数据：' + announcement_url)
                else:
                    continue
        else:
            szse_document = touzhiwang_db.szse.find({'_id': each_document['_id']})[0]
            publish_date = szse_document['gkxx_gdrq'].split('-')
            real_publish_date = str(int(publish_date[0])) + '年' + str(int(publish_date[1])) + '月' + str(
                int(publish_date[2])) + '日'

            result_map = {
                'announcementTitle': '关于对' + szse_document['gkxx_gsjc'].replace(' ', '').strip() + '的监管函',
                'announcementOrg': '深交所',
                'announcementDate': real_publish_date,
                'announcementCode': '',
                'facts': szse_document['gkxx_jgsy'].strip(),
                'defenseOpinion': '',
                'defenseResponse': '',
                'litigant': '公司代码：' + szse_document['gkxx_gsdm'] + '\n' + '公司简称：' + szse_document['gkxx_gsjc'] +
                            ('\n涉及对象：' + szse_document['gkxx_sjdx'] if szse_document['gkxx_sjdx'] != '' else ''),
                'punishmentBasement': '',
                'punishmentDecision': '采取监管措施',
                'type': szse_document['gkxx_jgcs'],
                'oss_file_id': ''
            }
            if db.announcement.find(result_map).count() == 0:
                result_map['status'] = 'checked'
                db.announcement.insert_one(result_map)
                logger.info('深交所 数据解析 ' + ' -- 无链接数据导入完成')
            else:
                logger.info('深交所 数据解析 ' + ' -- 无链接数据已经存在')


if __name__ == "__main__":
    szse_crawler()
