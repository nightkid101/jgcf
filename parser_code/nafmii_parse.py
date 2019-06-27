import re

from pymongo import MongoClient
from utility import format_date, request_site_page
from bs4 import BeautifulSoup as bs
from init import logger_init, config_init
from oss_utils import init_ali_oss, oss_add_file

logger = logger_init('中国银行间市场交易商协会 数据解析')
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
ali_bucket = init_ali_oss()


def nafmii_parse():
    for each_nafmii_document in db.nafmii_data.find({'status': {'$nin': ['ignored']}}):

        announcement_url = each_nafmii_document['url']
        announcement_title = each_nafmii_document['title']

        if '交易商协会组织召开自律处分体系讨论会' in announcement_title or \
                '协会召开' in announcement_title or \
                '后续管理督查纠正工作有效开展' in announcement_title:
            logger.warning(announcement_url + ' ' + announcement_title + ' 与监管处罚无关')
            db.nafmii_data.update_one({'_id': each_nafmii_document['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('交易商协会' + ' 无关数据' + ' -- 修改status完成')
            continue

        if db.nafmii_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('Url to parse: ' + announcement_url)

        response = request_site_page(announcement_url)
        if response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        soup = bs(response.content, 'lxml') if response else bs('', 'lxml')

        if db.parsed_data.find(
                {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_nafmii_document['_id'],
                'oss_file_type': 'html',
                'oss_file_name': announcement_title,
                'oss_file_content': response.text.encode(response.encoding).decode('utf-8'),
                'parsed': False
            }
            insert_response = db.parsed_data.insert_one(oss_file_map)
            file_id = insert_response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                         response.text.encode(response.encoding).decode('utf-8'))
            db.nafmii_data.update_one({'_id': each_nafmii_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.nafmii_data.update_one({'_id': each_nafmii_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']

        if '“430”违规机构' not in announcement_title and \
                '“831”和“1031”违规机构' not in announcement_title and \
                '“1031”信息披露违规企业及信用增进机构' not in announcement_title and \
                '“430”信息披露违规企业及信用增进机构' not in announcement_title and \
                '协会对2011年年报及2012年一季报披露不合规机构进行自律处理' not in announcement_title and \
                '协会对曲江文投等机构进行自律处分' not in announcement_title and \
                '加强市场自律规范与管理,维护市场平稳健康发展--协会对特别会员山东海龙进行自律处分' not in announcement_title and \
                '诫勉谈话' not in announcement_title:
            publish_date = format_date(each_nafmii_document['publishDate'])

            if len(soup.find_all(class_='Section1')) > 0:
                content_text = soup.find(class_='Section1').text.strip()
            else:
                content_text = soup.find(class_='TRS_PreAppend').text.strip()

            if re.search(r'(\d{4}年第\d+次自律处分会议)', content_text):
                document_code = re.search(r'(\d{4}年第\d+次自律处分会议)', content_text).group(1).strip()
            else:
                document_code = ''

            if document_code != '':
                litigant1 = re.search(
                    r'\n(.*?)(作为(相关)?债务融资工具(发行人|主承销商|担保人)|'
                    r'两家债务融资工具发行人在债务融资工具存续期间|'
                    r'在债务融资工具|作为“12沈公用MTN1”的牵头主承销商及簿记管理人|在注册发行债务融资工具的过程中|'
                    r'在银行间债券市场提供债务融资工具中介服务时)',
                    content_text).group(1).strip()
                litigant = re.split('[—-]', announcement_title)[-1]
            else:
                if announcement_url == 'http://www.nafmii.org.cn/zlgl/zwrz/zlcf/201212/t20121203_18735.html':
                    litigant1 = '在2012年第三季度财务信息披露工作中，内蒙古高等级公路建设开发有限责任公司和陕西煤业化工集团有限责任公司披露延迟'
                    litigant = '内蒙古高等级公路建设开发有限责任公司和陕西煤业化工集团有限责任公司'
                else:
                    litigant1 = re.search(
                        r'^.*?\n(.*?)(（|\(|作为(相关)?债务融资工具(发行人|主承销商|担保人)|两家债务融资工具发行人在债务融资工具存续期间|'
                        r'在债务融资工具|作为“12沈公用MTN1”的牵头主承销商及簿记管理人|在注册发行债务融资工具的过程中|'
                        r'在银行间债券市场提供债务融资工具中介服务时)',
                        content_text).group(1).strip()
                    litigant = re.split('[—-]', announcement_title)[-1]

            if re.search(r'(\d{4}\.\d{2}\.\d{2})', content_text):
                publish_date = re.search(r'(\d{4}\.\d{2}\.\d{2})', content_text).group(1).strip()
                publish_date = format_date(publish_date)

            truth = re.search(r'\n(' + litigant1 + r'[\s\S]*?)\n?(依据|根据).*?规定', content_text).group(1).strip()
            punishment_decision = re.search(truth.replace(r'(', r'\(').replace(r')', r'\)') +
                                            r'([\s\S]*?)$', content_text).group(1).strip()

            result_map = {
                'announcementTitle': announcement_title,
                'announcementOrg': '交易商协会',
                'announcementDate': publish_date,
                'announcementCode': document_code,
                'facts': truth,
                'defenseOpinion': '',
                'defenseResponse': '',
                'litigant': litigant,
                'punishmentBasement': '',
                'punishmentDecision': punishment_decision,
                'type': '行政处罚决定',
                'oss_file_id': file_id,
                'status': 'not checked'
            }
            logger.info(result_map)
            if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
                db.announcement.insert_one(result_map)
                logger.info('交易商协会 数据解析 ' + ' -- 数据导入完成')
            else:
                logger.info('交易商协会 数据解析 ' + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('交易商协会 数据解析 ' + ' -- 修改parsed完成')


def parse():
    nafmii_parse()


if __name__ == "__main__":
    parse()