import requests
from bs4 import BeautifulSoup as bs
from xlrd import open_workbook
from pymongo import MongoClient
from init import logger_init, config_init
from utility import format_date

logger = logger_init('外汇处罚 数据解析')
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


def exchange_parse():
    s = requests.Session()
    s.get('http://www.safe.gov.cn/safe/whxzcfxxcx/index.html')

    excel_data = open_workbook('./xlsx_file/exchange/exchange_punishment_company.xlsx')

    for sheet_number in range(9):
        sheet = excel_data.sheets()[sheet_number]
        for i in range(sheet.nrows):
            if sheet.cell(i, 0).value != '' and \
                    sheet.cell(i, 0).value not in ['L\'occitane International S.A.',
                                                   'K W Nelson Interior Design and Contracting Group Limited']:
                logger.info('公司名称：' + sheet.cell(i, 0).value)
                data_json = {
                    'irregularityname': sheet.cell(i, 0).value,
                    'irregularityno': ''
                }
                r = s.post('http://www.safe.gov.cn/www/punish/punishQuery', data=data_json)
                # time.sleep(2)
                soup = bs(r.text, "lxml")
                try:
                    if len(soup.find(id='bbstab').find_all('tr')[1:]) > 0:
                        for each_tr in soup.find(id='bbstab').find_all('tr')[1:]:
                            td_list = each_tr.find_all('td')
                            litigant = ''
                            if td_list[1].text.strip() != '':
                                litigant += '违规主体名称：' + td_list[1].text.strip() + '\n'
                            if td_list[2].text.strip() != '':
                                litigant += '注册地：' + td_list[2].text.strip() + '\n'
                            if td_list[3].text.strip() != '':
                                litigant += '机构代码：' + td_list[3].text.strip()
                            litigant = litigant.strip()

                            announcement_code = td_list[5].text.strip()
                            if td_list[8].text.strip() in ['8/16/16']:
                                announcement_date = '20' + td_list[8].text.strip().split('/')[2] + '年' + \
                                                    td_list[8].text.strip().split('/')[0] + '月' + \
                                                    td_list[8].text.strip().split('/')[1] + '日'
                            else:
                                announcement_date = format_date(td_list[8].text.strip())
                            announcement_org = td_list[4].text.strip()
                            if announcement_org == '西藏分局':
                                announcement_org = '国家外汇管理局西藏分局'
                            announcement_title = '外汇行政处罚信息公示表（' + announcement_code + '）' \
                                if announcement_code != '' else '外汇行政处罚信息公示表'
                            facts = td_list[6].text.strip()
                            if td_list[7].text.strip() != '':
                                if td_list[7].text.strip().startswith('依据') or td_list[7].text.strip().startswith('根据'):
                                    punishment_decision = td_list[7].text.strip() + '，' + td_list[9].text.strip()
                                else:
                                    punishment_decision = '依据' + td_list[7].text.strip() + '，' + td_list[9].text.strip()
                            else:
                                punishment_decision = td_list[9].text.strip()
                            punishment_decision = punishment_decision.replace('。，', '，').replace('，，', '，') \
                                .replace(';，', '，')
                            facts = facts.replace(',', '，').replace(';', '。')
                            punishment_decision = punishment_decision.replace(',', '，').replace(';', '。')
                            result_map = {
                                'announcementTitle': announcement_title,
                                'announcementOrg': announcement_org,
                                'announcementDate': announcement_date,
                                'announcementCode': announcement_code,
                                'facts': facts,
                                'defenseOpinion': '',
                                'defenseResponse': '',
                                'litigant': litigant,
                                'punishmentBasement': '',
                                'punishmentDecision': punishment_decision,
                                'type': '行政处罚决定',
                                'oss_file_id': '',
                                'status': 'checked'
                            }
                            logger.info(result_map)
                            if db.announcement.find(
                                    {'announcementTitle': announcement_title, 'litigant': litigant}).count() == 0:
                                db.announcement.insert_one(result_map)
                                logger.info('外汇处罚 数据解析 ' + ' -- 数据导入完成')
                            else:
                                logger.info('外汇处罚 数据解析 ' + ' -- 数据已经存在')
                    else:
                        logger.warning('没有处罚数据～')
                except Exception as e:
                    logger.error(e)
                    continue


def parse():
    exchange_parse()


if __name__ == "__main__":
    parse()
