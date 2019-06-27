import xlwt
from pymongo import MongoClient
from datetime import datetime
from init import config_init, logger_init


logger = logger_init('导出数据到xlsx')
config = config_init()

if config['mongodb']['dev_mongo'] == '1':
    db = MongoClient(config['mongodb']['ali_mongodb_url'], username=config['mongodb']['ali_mongodb_username'],
                     password=config['mongodb']['ali_mongodb_password'],
                     port=int(config['mongodb']['ali_mongodb_port']))[config['mongodb']['ali_mongodb_name']]
else:
    db = MongoClient(
        host=config['mongodb']['mongodb_host'],
        port=int(config['mongodb']['mongodb_port']),
        username=None if config['mongodb']['mongodb_username'] == '' else config['mongodb']['mongodb_username'],
        password=None if config['mongodb']['mongodb_password'] == '' else config['mongodb']['mongodb_password'])[
        config['mongodb']['mongodb_db_name']]


def get_xlsx(sheet_name):
    workbook = xlwt.Workbook(encoding='ascii')

    worksheet = workbook.add_sheet(sheet_name)
    worksheet.write(0, 0, label='发文名称')
    worksheet.write(0, 1, label='文号')
    worksheet.write(0, 2, label='处罚日期')
    worksheet.write(0, 3, label='当事人')
    worksheet.write(0, 4, label='违法违规事实')
    worksheet.write(0, 5, label='申辩意见')
    worksheet.write(0, 6, label='申辩意见反馈')
    worksheet.write(0, 7, label='监管机构认定意见')
    worksheet.write(0, 8, label='处罚决定')
    worksheet.write(0, 9, label='发布机构')
    worksheet.write(0, 10, label='处罚类型')
    worksheet.write(0, 11, label='原始URL')
    worksheet.write(0, 12, label='附件URL')
    worksheet.write(0, 13, label='id')
    worksheet.write(0, 14, label='是否ocr')
    worksheet.write(0, 15, label='数据库操作')

    count = 1

    for each_not_parsed_data in db['parsed_data'].find({'parsed': False}):
        if each_not_parsed_data['origin_url'] in ['http://www.amac.org.cn/xxgs/bydjjg/393777.shtml']:
            continue
        logger.info('Parsed Data To XLSX. Id: ' + str(each_not_parsed_data['_id']))
        db['parsed_data'].update_one({'_id': each_not_parsed_data['_id']}, {'$set': {'parsed': True}})
        worksheet.write(count, 0, label=each_not_parsed_data['oss_file_name'])
        worksheet.write(count, 1, label='')
        worksheet.write(count, 2, label='')
        worksheet.write(count, 3, label='')
        worksheet.write(count, 4, label='')
        worksheet.write(count, 5, label='')
        worksheet.write(count, 6, label='')
        worksheet.write(count, 7, label='')
        worksheet.write(count, 8, label='')
        worksheet.write(count, 9, label='')
        worksheet.write(count, 10, label='')
        worksheet.write(count, 11, label=each_not_parsed_data['origin_url'])
        worksheet.write(count, 12, label=each_not_parsed_data['oss_file_origin_url'])
        worksheet.write(count, 13, label=str(each_not_parsed_data['_id']))
        worksheet.write(count, 14, label=str(each_not_parsed_data.get('if_ocr', '')))
        count += 1

    for each_data in db.announcement.find({'status': 'not checked'}).sort([("oss_file_id", 1)]):
        if db.parsed_data.find({'_id': each_data['oss_file_id']}).count() > 0:
            origin_url = db.parsed_data.find_one({'_id': each_data['oss_file_id']})['origin_url']
            oss_file_origin_url = db.parsed_data.find_one({'_id': each_data['oss_file_id']})['oss_file_origin_url']
            try:
                if_ocr = db.parsed_data.find_one({'_id': each_data['oss_file_id']})['if_ocr']
            except Exception as e:
                logger.warning(e)
                if_ocr = ''
        else:
            origin_url = ''
            oss_file_origin_url = ''
            if_ocr = ''

        logger.info('Announcement Data To XLSX. Id: ' + str(each_data['_id']))
        db['announcement'].update_one({'_id': each_data['_id']}, {'$set': {'status': 'checking'}})

        worksheet.write(count, 0, label=each_data['announcementTitle'])
        worksheet.write(count, 1, label=each_data['announcementCode'])
        worksheet.write(count, 2, label=each_data['announcementDate'])
        worksheet.write(count, 3, label=each_data['litigant'])
        worksheet.write(count, 4, label=each_data['facts'])
        worksheet.write(count, 5, label=each_data['defenseOpinion'])
        worksheet.write(count, 6, label=each_data['defenseResponse'])
        worksheet.write(count, 7, label=each_data['punishmentBasement'])
        worksheet.write(count, 8, label=each_data['punishmentDecision'])
        worksheet.write(count, 9, label=each_data['announcementOrg'])
        worksheet.write(count, 10, label=each_data['type'])
        worksheet.write(count, 11, label=origin_url)
        worksheet.write(count, 12, label=oss_file_origin_url)
        worksheet.write(count, 13, label=str(each_data['_id']))
        worksheet.write(count, 14, label=str(if_ocr))
        count += 1

    workbook.save('/Users/austinzy/Desktop/' + sheet_name + '.xls')


if __name__ == "__main__":
    today_date = datetime.now().date()
    get_xlsx('监管处罚新数据' + today_date.strftime("%Y年%m月%d日"))
