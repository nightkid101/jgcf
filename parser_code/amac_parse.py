import re

from pymongo import MongoClient
from init import logger_init, config_init
from oss_utils import init_ali_oss, oss_add_file
from bs4 import BeautifulSoup as bs
from utility import request_site_page, get_year, cn2dig

logger = logger_init('中国基金业协会 数据解析')
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


# 中国基金业协会 数据解析
def amac_parse():
    for each_amac_document in db.amac_data.find({'status': {'$nin': ['ignored']}}):
        announcement_url = each_amac_document['url']
        announcement_title = each_amac_document['title']
        announcement_type = each_amac_document['type']

        if '关于注销期间届满未提交专项法律意见书私募基金管理人登记的公告' in announcement_title:
            logger.warning('无关数据')
            db.amac_data.update_one({'_id': each_amac_document['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('证券业协会' + ' 重复数据' + ' -- 修改status完成')
            continue

        if db.amac_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and \
                db.parsed_data.find({'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
                                     'parsed': True}).count() == 1:
            continue

        logger.info('Url to parse: ' + announcement_url)

        response = request_site_page(announcement_url)
        if response is None:
            logger.error('网页请求错误')
            continue

        while response.status_code == 404:
            response = request_site_page(announcement_url)
            if response is None:
                logger.error('网页请求错误')
                continue

        content_soup = bs(response.content, 'lxml') if response else bs('', 'lxml')

        if db.parsed_data.find(
                {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_amac_document['_id'],
                'oss_file_type': 'shtml',
                'oss_file_name': announcement_title,
                'oss_file_content': response.text.encode(response.encoding).decode('utf-8'),
                'parsed': False
            }
            insert_response = db.parsed_data.insert_one(oss_file_map)
            file_id = insert_response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.shtml',
                         response.text.encode(response.encoding).decode('utf-8'))
            db.amac_data.update_one({'_id': each_amac_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.amac_data.update_one({'_id': each_amac_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']

        if announcement_type == '纪律处分':
            td_content = content_soup.find(class_='iRight')
            title = td_content.find(class_='ldT').text.strip()
            content_text = td_content.find(class_='ldContent').text.strip()
            publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', content_text)[-1]
            m = re.match("([0-9零一二两三四五六七八九十〇]+年)?([0-9一二两三四五六七八九十]+)月?"
                         "([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
            real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
            defense_opinion = defense_response = announcement_code = ''

            if '纪律处分决定书' in title:
                litigant = re.search(r'当事人[：:]([\s\S]*?)(根据|经查明|\n\d{4}年\d+月)', content_text).group(2).strip()
                if re.search(r'一、基本事实[\s\S]*二、', content_text):
                    truth = re.search(r'一、基本事实([\s\S]*)二、', content_text).group(1).strip().replace('\n+', '\n')
                    decision = re.sub('、(纪律)?处分决定', '',
                                      re.search(r'、(纪律)?处分决定[\s\S]*?恢复.*?资产(支持专项|管理)计划备案。',
                                                content_text).group(0)).strip()
                else:
                    if re.search(r'根据中国证监会.*的通报.*应当负有主要责任。', content_text):
                        truth = re.search(r'根据中国证监会.*的通报.*应当负有主要责任。',
                                          content_text).group(0).strip().replace('\n+', '\n')
                        if re.search(r'鉴于本次纪律处分案的事实已经.*书面确认.*[协我]会决定作出以下纪律处分：'
                                     r'[\s\S]*需要重新参加基金从业资格考试', content_text):
                            decision = re.search(r'鉴于本次纪律处分案的事实已经.*书面确认.*[协我]会决定作出以下纪律处分：'
                                                 r'[\s\S]*需要重新参加基金从业资格考试',
                                                 content_text).group(0)
                        else:
                            decision = re.search(r'鉴于本次纪律处分案的事实已经.*书面确认.*协会决定作出以下纪律处分：.*',
                                                 content_text).group(0)
                    else:
                        if re.search(r'经查明.*存在以下(违法)?违规(情形|事实)：[\s\S]*上述行为违反了', content_text):
                            truth = re.sub('\n.*上述行为违反了', '',
                                           re.search(r'经查明.*存在以下(违法)?违规(情形|事实)：[\s\S]*上述行为违反了',
                                                     content_text).group(0))
                            decision = re.search(r'[本我]会决定：[\s\S]*上述纪律处分记入资本市场诚信档案。',
                                                 content_text).group(0)
                        else:
                            truth = re.search(r'我会据此直接作出纪律处分。([\s\S]*)三、', content_text).group(1).strip()
                            decision = re.search(r'、纪律处分决定([\s\S]*上述纪律处分记入资本市场诚信档案。)',
                                                 content_text).group(1).strip()
                punishment_basement = re.search(r'[。，]([^。，]*?情形违反.*?负有主要责任。)', content_text).group(2).strip()
            else:
                if '纪律处分复核决定书' in title:
                    announcement_code = re.search(r'(中基协复核〔\d+〕\d+号)\s*?申请人', content_text).group(1).strip()
                    litigant = re.search(r'(申请人：[\s\S]*?)申请人不服', content_text).group(1).strip()
                    truth = re.search(r'一、纪律处分情况([\s\S]*?)二、申辩意见', content_text).group(1).strip()
                    defense_opinion = re.search(r'二、申辩意见([\s\S]*?)三、复核意见', content_text).group(1).strip()
                    defense_response = re.search(r'三、复核意见([\s\S]*?)四、复核决定', content_text).group(1).strip()
                    decision = re.search(r'四、复核决定([\s\S]*?)\n\s*?中国证券投资基金业协会', content_text).group(1).strip()
                    punishment_basement = ''
                else:
                    litigant = re.search(r'关于注销(.*)私募基金管理人登记的公告', title).group(1).strip()
                    truth = re.search(r'现将有关情况公告如下：([\s\S]*)三、', content_text).group(1).strip()
                    punishment_basement = re.search(r'[。，]([^。，]*?情形违反.*?负有主要责任。)', content_text).group(2) \
                        .strip()
                    decision = re.search(r'、处理(决定|措施)([\s\S]*)特此公告。', content_text).group(2).strip()

            result_map = {
                'announcementTitle': title,
                'announcementDate': real_publish_date,
                'announcementOrg': '基金业协会',
                'announcementCode': announcement_code,
                'litigant': litigant,
                'facts': truth,
                'punishmentDecision': decision,
                'defenseOpinion': defense_opinion,
                'defenseResponse': defense_response,
                'type': '纪律处分',
                'punishmentBasement': punishment_basement,
                'oss_file_id': file_id,
                'status': 'not checked'
            }
            logger.info(result_map)
            if db.announcement.find({'announcementTitle': title, 'oss_file_id': file_id}).count() == 0:
                db.announcement.insert_one(result_map)
                logger.info('基金业协会 纪律处分 数据解析 ' + ' -- 数据导入完成')
            else:
                logger.info('基金业协会 纪律处分 数据解析 ' + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('基金业协会 纪律处分 数据解析 ' + ' -- 修改parsed完成')
        elif announcement_type == '不予登记':
            # 不予登记
            title = content_soup.find(class_='ldT').text.strip()
            content_tr = content_soup.find(class_='ldContent').find('tbody').find_all('tr')
            count = 0
            for each_tr in content_tr:  # 对表格每一行进行解析
                count += 1
                if count <= 2:  # 表格第一行为表格标题，第二行为表格各列表示的内容
                    continue
                else:  # 真实数据从第三行开始
                    all_info = each_tr.find_all('td')
                    litigant = all_info[0].text.strip() + "\n机构组织机构代码：" + all_info[1].text.strip()
                    publish_date = all_info[2].text.strip()
                    truth = all_info[3].text.strip() + "\n出具法律意见书的律师事务所：" \
                            + all_info[4].text.strip() + "\n经办律师姓名：" \
                            + all_info[5].text.strip().replace('\n', '、')
                    truth = truth.replace('\xa0', '').replace('\t', '')
                    m = re.search(r'(\d+)[-/](\d+)[-/](\d+)', publish_date)
                    real_publish_date = str(m.group(1)) + '年' + str(m.group(2)) + '月' + str(m.group(3)) + '日'

                result_map = {
                    'announcementTitle': title,
                    'announcementDate': real_publish_date,
                    'announcementOrg': '基金业协会',
                    'announcementCode': '',
                    'litigant': litigant,
                    'facts': truth,
                    'punishmentDecision': '不予登记',
                    'defenseOpinion': '',
                    'defenseResponse': '',
                    'type': '不予登记',
                    'punishmentBasement': '',
                    'oss_file_id': file_id,
                    'status': 'not checked'
                }
                logger.info(result_map)
                if db.announcement.find({'litigant': litigant, 'announcementDate': real_publish_date}).count() == 0:
                    db.announcement.insert_one(result_map)
                    logger.info('基金业协会 不予登记 数据解析 ' + ' -- 数据导入完成')
                else:
                    logger.info('基金业协会 不予登记 数据解析 ' + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': False}})
            logger.info('基金业协会 不予登记 数据解析 ' + ' -- 修改parsed完成')
        else:
            # 黑名单
            response = request_site_page(announcement_url)
            if response is None:
                logger.error('网页请求错误')
                continue
            base_soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
            content_tr = base_soup.find(class_='ldContent').find('tbody').find_all('tr')
            title = base_soup.find(class_='ldT').text.strip()

            count_tr = 0
            for each_tr in content_tr:
                count_tr += 1
                if count_tr < 2:  # 第一行为表头，第二行后面才是数据
                    continue
                else:
                    count = 0  # count用于计算表格的列数
                    all_td = each_tr.find_all('td')

                    for each_td in all_td:
                        count += 1

                    publish_date = all_td[0].text.strip()
                    announcement_code = all_td[-1].text.strip()
                    m = re.search(r'(\d+)[-/](\d+)[-/](\d+)', publish_date)
                    real_publish_date = str(m.group(1)) + '年' + str(m.group(2)) + '月' + str(m.group(3)) + '日'

                    if count == 3:  # 表格有三列，说明无当事人姓名
                        litigant = all_td[1].text.strip()
                    else:  # 表格有四列，有当事人姓名
                        litigant = all_td[1].text.strip() + '，' + all_td[2].text.strip()

                result_map = {
                    'announcementTitle': title,
                    'announcementDate': real_publish_date,
                    'announcementOrg': '基金业协会',
                    'announcementCode': announcement_code,
                    'litigant': litigant,
                    'facts': '',
                    'punishmentDecision': '',
                    'defenseOpinion': '',
                    'defenseResponse': '',
                    'type': '黑名单',
                    'punishmentBasement': '',
                    'oss_file_id': file_id,
                    'status': 'not checked'
                }
                logger.info(result_map)
                if db.announcement.find({'litigant': litigant, 'oss_file_id': file_id}).count() == 0:
                    db.announcement.insert_one(result_map)
                    logger.info('基金业协会 黑名单 数据解析 ' + ' -- 数据导入完成')
                else:
                    logger.info('基金业协会 黑名单 数据解析 ' + ' -- 数据已经存在')
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('基金业协会 黑名单 数据解析 ' + ' -- 修改parsed完成')


def parse():
    amac_parse()


if __name__ == "__main__":
    parse()
