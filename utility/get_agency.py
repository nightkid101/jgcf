import xlwt
from pymongo import MongoClient
from init import config_init

config = config_init()

db = MongoClient(
    host=config['mongodb']['mongodb_host'],
    port=int(config['mongodb']['mongodb_port']),
    username=None if config['mongodb']['mongodb_username'] == '' else config['mongodb']['mongodb_username'],
    password=None if config['mongodb']['mongodb_password'] == '' else config['mongodb']['mongodb_password'])[
    config['mongodb']['mongodb_db_name']]

keywords_list = ['会计师事务所', '注册会计师', '事务所审计', '审计*部', '审计项目负责人', '审计机构', '会计师事务有限公司', '提供审计服务', '审计服务机构', '签字律师', '经办律师',
                 '专职律师', '聘用律师', '执业律师', '律师*事务所', '律师事务所', '法律服务机构', '评估*部', '评估咨询', '评估师', '评估部', '资产评估', '土地评估',
                 '证券公司', '证券营业人员', '证券*营业部', '证券股份有限公司', '证券有限责任公司', '证券交易营业部', '证券营业部', '证券业务部', '证券有限公司',
                 '承销保荐有限责任公司']


def get_xlsx(sheet_name, collection_name, org_name, org_url):
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
    worksheet.write(0, 10, label='原始URL')
    worksheet.write(0, 11, label='附件URL')
    worksheet.write(0, 12, label='id')

    count = 1

    for each_data in db[collection_name].find({'litigant': {'$regex': '.*(' + '|'.join(keywords_list) + ').*'}}):

        if each_data['oss_file_id'] != '':
            origin_url = db.parsed_data.find_one({'_id': each_data['oss_file_id']})['origin_url']
            oss_file_origin_url = db.parsed_data.find_one({'_id': each_data['oss_file_id']})['oss_file_origin_url']
        else:
            origin_url = ''
            oss_file_origin_url = ''

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
        worksheet.write(count, 10, label=origin_url)
        worksheet.write(count, 11, label=oss_file_origin_url)
        worksheet.write(count, 12, label=str(each_data['_id']))
        count += 1

    workbook.save('/Users/austinzy/Desktop/' + sheet_name + '.xls')


get_xlsx('中介机构', 'announcement', '中介机构', 'safe')