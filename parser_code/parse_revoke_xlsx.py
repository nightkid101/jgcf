import xlrd
from xlrd import xldate_as_tuple
from pymongo import MongoClient
from init import config_init, logger_init
import requests
from oss_utils import init_ali_oss, oss_add_file

config = config_init()

logger = logger_init('拟吊销数据 导入数据库')
ali_bucket = init_ali_oss()
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


def process_revoke():
    workbook = xlrd.open_workbook('./xlsx_file/circ/' + 'revoke.xls')
    book_sheet = workbook.sheet_by_index(0)
    litigant_keywords_list = ['组织机构代码', '兼业代理人名称', '许可证编号', '营业地址', '机构负责人',
                              '联系电话', '资格审核日期', '代理险种']

    announcement_url = 'http://shanxi.circ.gov.cn/web/site31/tab3452/info113330.htm'
    r = requests.get(announcement_url)
    origin_url_id = db.circ_data.find_one({'url': announcement_url})['_id']
    if db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
        oss_file_map = {
            'origin_url': announcement_url,
            'oss_file_origin_url': announcement_url,
            'origin_url_id': origin_url_id,
            'oss_file_type': 'html',
            'oss_file_name': '中国保监会山西监管局行政处罚决定书（晋保监罚字[2009]17号）',
            'oss_file_content': r.text.encode(r.encoding).decode('utf-8').replace(
                '/Portals/31/attachments/拟吊销保险兼业代理业务许可证资格机构名单（共1497家）.xls',
                'https://cdn.touzhiwang.com/punishment/拟吊销保险兼业代理业务许可证资格机构名单（共1497家）.xls'),
            'parsed': False
        }
        insert_response = db.parsed_data.insert_one(oss_file_map)
        file_id = insert_response.inserted_id
        oss_add_file(ali_bucket, str(file_id) + '/' + '中国保监会山西监管局行政处罚决定书（晋保监罚字[2009]17号）' + '.html',
                     r.text.encode(r.encoding).decode('utf-8').replace(
                         '/Portals/31/attachments/拟吊销保险兼业代理业务许可证资格机构名单（共1497家）.xls',
                         'https://cdn.touzhiwang.com/punishment/拟吊销保险兼业代理业务许可证资格机构名单（共1497家）.xls'))
        db.circ_data.update_one({'_id': origin_url_id}, {'$set': {'status': 'parsed'}})
    else:
        db.circ_data.update_one({'_id': origin_url_id}, {'$set': {'status': 'parsed'}})
        file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                           'oss_file_origin_url': announcement_url})['_id']

    for row in range(book_sheet.nrows):
        if row == 0:
            continue
        row_value = book_sheet.row_values(row)
        litigant = ''
        litigant_value_list = [row_value[1], row_value[2], row_value[3], row_value[4], row_value[5], row_value[6],
                               row_value[7], row_value[8]]
        for index, each_litigant_value in enumerate(litigant_value_list):
            if each_litigant_value != '':
                if index == 6 and type(each_litigant_value) == float:
                    publish_date = xldate_as_tuple(each_litigant_value, workbook.datemode)
                    publish_date = str(publish_date[0]) + '年' + str(publish_date[1]) + '月' + str(
                        publish_date[2]) + '日'
                    litigant += litigant_keywords_list[index] + '：' + publish_date + '\n'
                elif index == 6 and '-' in each_litigant_value:
                    date_list = str(each_litigant_value).split('-')
                    litigant += litigant_keywords_list[index] + '：' + \
                                date_list[0] + '年' + date_list[1] + '月' + date_list[2] + '日' + '\n'
                else:
                    litigant += litigant_keywords_list[index] + '：' + str(each_litigant_value) + '\n'
        litigant = litigant.strip()

        result_map = {
            'announcementTitle': '中国保监会山西监管局行政处罚决定书（晋保监罚字[2009]17号）',
            'announcementOrg': '山西保监局',
            'announcementDate': '2009年9月4日',
            'announcementCode': '晋保监罚字[2009]17号',
            'facts': '在2008年清理整顿保险兼业代理市场工作中，我局于2007年12月27日和2008年5月23日两次在《山西日报》发布要求缴清监管费的公告后，该机构拒不缴纳监管费',
            'defenseOpinion': '',
            'defenseResponse': '',
            'litigant': litigant,
            'punishmentBasement': '违反了《关于调整保险业务监管费收费标准和收费办法的通知》（保监发[2006]13号）'
                                  '“对从事保险兼业代理的机构，按每年每家机构500元定额收取保险业务监管费。”'
                                  '和《中华人民共和国保险法》第一百零九条“保险监督管理机构有权检查保险公司的业务状况、'
                                  '财务状况及资金运用状况，有权要求保险公司在规定的期限内提供有关的书面报告和资料。'
                                  '保险公司依法接受监督检查。”的规定',
            'punishmentDecision': '依据《关于调整保险业务监管费收费标准和收费办法的通知》（保监发[2006]13号）'
                                  '“中国保监会和保监局有权检查保险业务监管费的缴纳情况，'
                                  '对违反规定迟缴、少缴保险业务监管费的，可以责令其补缴，'
                                  '并按照一定比例缴纳滞纳金；对拒不缴纳保险业务监管费的，'
                                  '可以依据保险法第一百四十七条的规定，给予行政处罚。”'
                                  '的精神和《中华人民共和国保险法》第一百四十七条“违反本法规定，'
                                  '有下列行为之一，构成犯罪的，依法追究刑事责任；尚不构成犯罪的，'
                                  '由保险监督管理机构责令改正，处以十万元以上五十万元以下的罚款；'
                                  '情节严重的，可以限制业务范围、责令停止接受新业务或者吊销经营保险业务许可证：'
                                  '（一）提供虚假的报告、报表、文件和资料的；（二）拒绝或者妨碍依法检查监督的。”规定，'
                                  '我局认为在两次公告催缴后，仍拒缴纳，违规情节严重，'
                                  '决定对该机构作出吊销《保险兼业代理业务许可证》的行政处罚。',
            'type': '行政处罚决定',
            'oss_file_id': file_id,
            'status': 'checked'
        }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': '中国保监会山西监管局行政处罚决定书（晋保监罚字[2009]17号）',
                                 'oss_file_id': file_id, 'litigant': litigant}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('保监局 数据解析 ' + '山东保监局' + ' -- 数据导入完成')
        else:
            logger.info('保监局 数据解析 ' + '山东保监局' + ' -- 数据已经存在')
    db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
    logger.info('保监局 数据解析 ' + '山东保监局' + ' -- 修改parsed完成')


process_revoke()
