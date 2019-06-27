import xlrd
from pymongo import MongoClient
from init import config_init, logger_init
from bson import ObjectId

config = config_init()

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


def xlsx_to_db(xlsx_name):
    workbook = xlrd.open_workbook('./xlsx_file/punishment/' + xlsx_name)
    book_sheet = workbook.sheet_by_index(0)
    for row in range(book_sheet.nrows):
        if row == 0:
            continue

        row_value = book_sheet.row_values(row)
        result_operation = row_value[15]
        announcement_id = row_value[13]
        if result_operation == '删除':
            if db.announcement.find({'_id': ObjectId(announcement_id)}).count() > 0:
                db.announcement.delete_one({'_id': ObjectId(announcement_id)})
                logger.info('删除 announcement : %s' % str(announcement_id))
            elif db.parsed_data.find({'_id': ObjectId(announcement_id)}).count() > 0:
                db.parsed_data.update_one({'_id': ObjectId(announcement_id)}, {'$set': {'parsed': True}})
                logger.info('删除 parsed_data : %s' % str(announcement_id))
            else:
                logger.error('Something error')
        elif result_operation == '跳过':
            if db.parsed_data.find({'_id': ObjectId(announcement_id)}).count() > 0:
                db.parsed_data.update_one({'_id': ObjectId(announcement_id)}, {'$set': {'parsed': False}})
                logger.info('跳过 parsed_data : %s' % str(announcement_id))
            else:
                logger.error('Something error')
        else:
            if db.announcement.find({'_id': ObjectId(announcement_id)}).count() > 0:
                logger.info('已解析数据 更新数据 : ' + announcement_id)
                if type(row_value[2]) != float:
                    publish_date = row_value[2]
                else:
                    publish_date = str(xlrd.xldate_as_tuple(row_value[2], workbook.datemode)[0]) + '年' \
                                   + str(xlrd.xldate_as_tuple(row_value[2], workbook.datemode)[1]) + '月' \
                                   + str(xlrd.xldate_as_tuple(row_value[2], workbook.datemode)[2]) + '日'
                new_map = {
                    'announcementTitle': row_value[0].strip(),
                    'announcementOrg': row_value[9].strip(),
                    'announcementDate': publish_date.strip(),
                    'announcementCode': row_value[1].strip(),
                    'facts': row_value[4].strip(),
                    'defenseOpinion': row_value[5].strip(),
                    'defenseResponse': row_value[6].strip(),
                    'litigant': row_value[3].strip(),
                    'punishmentBasement': row_value[7].strip(),
                    'punishmentDecision': row_value[8].strip(),
                    'type': row_value[10].strip(),
                    'status': 'checked'
                }
                db.announcement.update_one({'_id': ObjectId(announcement_id)}, {'$set': new_map})
                logger.info('已解析数据 更新数据 完成')
            else:
                if db.parsed_data.find({'_id': ObjectId(announcement_id)}).count() > 0:
                    logger.info('未解析数据 更新数据 : ' + announcement_id)
                    if type(row_value[2]) != float:
                        publish_date = row_value[2]
                    else:
                        publish_date = str(xlrd.xldate_as_tuple(row_value[2], workbook.datemode)[0]) + '年' \
                                       + str(xlrd.xldate_as_tuple(row_value[2], workbook.datemode)[1]) + '月' \
                                       + str(xlrd.xldate_as_tuple(row_value[2], workbook.datemode)[2]) + '日'
                    new_map = {
                        'announcementTitle': row_value[0].strip(),
                        'announcementOrg': row_value[9].strip(),
                        'announcementDate': publish_date.strip(),
                        'announcementCode': row_value[1].strip(),
                        'facts': row_value[4].strip(),
                        'defenseOpinion': row_value[5].strip(),
                        'defenseResponse': row_value[6].strip(),
                        'litigant': row_value[3].strip(),
                        'punishmentBasement': row_value[7].strip(),
                        'punishmentDecision': row_value[8].strip(),
                        'type': row_value[10].strip(),
                        'oss_file_id': ObjectId(announcement_id),
                        'status': 'checked'
                    }
                    db.announcement.insert_one(new_map)
                    logger.info('未解析数据 更新数据 插入数据 完成')
                    db.parsed_data.update_one({'_id': ObjectId(announcement_id)}, {'$set': {'parsed': True}})
                    logger.info('未解析数据 更新数据 更新parsed')
                    logger.info('未解析数据 更新数据 完成')
                else:
                    logger.error('Something error')
        logger.info('\n')


if __name__ == "__main__":
    xlsx_to_db('finish_punishment_20190305.xls')
