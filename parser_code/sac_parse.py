import re

from pymongo import MongoClient
from bs4 import BeautifulSoup as bs
from init import logger_init, config_init
from utility import get_year, cn2dig, request_site_page, format_date, remove_special_char
from oss_utils import init_ali_oss, oss_add_file

logger = logger_init('中国证券业协会 数据解析')
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


def sac_parse():
    for each_sac_document in db.sac_data.find({'status': {'$nin': ['ignored']}}):

        announcement_url = each_sac_document['url']
        announcement_title = each_sac_document['title']
        announcement_type = each_sac_document['type']

        title = announcement_title
        announcement_date = ''
        announcement_code = ''
        facts = ''
        defense_opinion = ''
        defense_response = ''
        litigant = ''
        punishment_basement = ''
        punishment_decision = ''

        if db.sac_data.find({'title': announcement_title}).count() >= 2 and each_sac_document['type'] == '监管措施':
            logger.warning(announcement_url + ' ' + announcement_title + ' 在通知公告与信息公示中一起出现')
            db.sac_data.update_one({'_id': each_sac_document['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('证券业协会' + ' 重复数据' + ' -- 修改status完成')
            continue

        if db.sac_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('Url to parse: ' + announcement_url)

        response = request_site_page(announcement_url)
        if response is None:
            logger.error('网页请求错误')
            continue
        content_soup = bs(response.text.encode(response.encoding).decode('utf-8'), 'lxml')

        if db.parsed_data.find(
                {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_sac_document['_id'],
                'oss_file_type': 'html',
                'oss_file_name': announcement_title,
                'oss_file_content': response.text.encode(response.encoding).decode('utf-8'),
                'parsed': False
            }
            insert_response = db.parsed_data.insert_one(oss_file_map)
            file_id = insert_response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                         response.text.encode(response.encoding).decode('utf-8'))
            db.sac_data.update_one({'_id': each_sac_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.sac_data.update_one({'_id': each_sac_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']

        if announcement_type == '监管措施':
            if re.search(r'公告', announcement_title):
                title = content_soup.find(class_='xl_cen').find(class_='xl_h').text.strip()
                announcement_code_raw = re.search(r'.*?(.{4}第.*号).*?$', title)
                if announcement_code_raw is not None:
                    announcement_code = announcement_code_raw.group(1).strip()
                else:
                    announcement_code = ''
                article = content_soup.find(class_='TRS_Editor').text.strip()
                find_result = re.search(r'.*处理(结果|情况)公告如下[:：]([\s\S]*?)特此公告', article)
                if find_result is not None:
                    punishment_decision = re.search(r'.*处理(结果|情况)公告如下[:：]([\s\S]*?)特此公告', article).group(2).strip()
                else:
                    if re.search(r'([\s\S]*?)特此公告', article) is not None:
                        punishment_decision = re.search(r'([\s\S]*?)特此公告', article).group(1).strip()
                    else:
                        punishment_decision = re.search(r'公告如下[:：](([\s\S]*)诚信信息管理系统。)', article).group(1).strip()
                publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', article)[-1].strip()
                m = re.match("([0-9零一二两三四五六七八九十〇○]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
                announcement_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
            else:
                td_content = content_soup.find(class_='xl_cen')
                title = td_content.find(class_='xl_h').text.strip()
                content_text = td_content.find(class_='hei14').text.strip()
                content_text = re.sub(r'.TRS_Editor.*font-size:12pt;}\n', '', content_text)
                content_text_list = content_text.split('\n')
                announcement_code = content_text_list[0].strip()
                announcement_date = format_date(content_text_list[-1].strip())
                # mechanism = content_text_list[-2]  该变量定义后并未使用
                litigant_raw = content_text_list[1].replace('：', '').replace(':', '')
                litigant = litigant_raw[:-1].strip() if litigant_raw[-1] == '：' or litigant_raw[
                    -1] == ':' else litigant_raw.strip()
                facts = re.sub('你|，(既)?违反', '', re.search(r'你(.*?)(。|，(既)?违反)', content_text_list[2]).group(0))
                if re.search(r'我会认为.*。', content_text):
                    punishment_basement = re.search(r'我会认为.*。', content_text).group(0)
                else:
                    punishment_basement = re.search(r'(既)?违反了.*。', content_text).group(0)
                punishment_decision = re.search(r'(依据.*?规定.*?)\n你对本决定如有异议', '\n'.join(content_text_list[3:])) \
                    .group(1).strip()
        elif announcement_type == '自律惩戒':
            article = content_soup.find_all(attrs={"class": 'TRS_Editor'})[-1].text.strip()
            title = content_soup.find(attrs={"class": 'xl_cen'}).find(attrs={"class": 'xl_h'}).text.strip()
            multi_announce = re.search(r'关于对.*名证券从业人员.*', title)
            announcement_code_raw = re.search(r'.*(.{4}第.*号).$', title)
            if announcement_code_raw is not None:
                announcement_code = announcement_code_raw.group(1).strip()
            elif re.search(r'(.*号)', article) is not None:
                announcement_code = re.search(r'(.*号)', article).group(1).strip()

            publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', article)[-1].strip()
            m = re.match("([0-9零一二两三四五六七八九十〇○]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
            announcement_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
            if multi_announce is None:  # 单人处罚
                litigant = re.search(r'.*号([\s\S]*)[:：]([\s\S]*)根据', article).group(1).strip()
                defense_opinion_raw = re.search(r'.*(我会认为，.*规定。)', article)
                if defense_opinion_raw is not None:
                    defense_opinion = defense_opinion_raw.group(1).strip()
                elif re.search(r'[:：]([\s\S]*)规定。', article) is not None:
                    defense_opinion = re.search(r'[:：]([\s\S]*规定。)', article).group(1).strip()
                elif re.search(r'(我会认为([\s\S]*))(综上|依据)', article):
                    defense_opinion = re.search(r'(我会认为([\s\S]*))(综上|依据)', article).group(1).strip()
                punishment_decision_raw = re.search(r'规定。(([\s\S]*)申请。)', article)
                if punishment_decision_raw is not None:
                    punishment_decision = punishment_decision_raw.group(1).strip()
                else:
                    punishment_decision = re.search(
                        r'((依据|根据).*?(规定|现决定对).*?)\s*　*(你对本决定如有异议|本(自律惩戒)?决定书为最终决定|中国证券业协会|对本决定如有异议)',
                        article).group(1).strip()

            else:  # 多人处罚
                litigant_raw = re.search(r'取消(.*).人的从业资格考试成绩', article)  # 对于多个段落的公告，仅提取了第一个段落的当事人，其余当事人在处罚决定中
                if litigant_raw is not None:
                    litigant = litigant_raw.group(1).strip()
                punishment_decision_raw = re.search(r'具体内容如下[:：]([\s\S]*)特此公告。', article)
                if punishment_decision_raw is not None:
                    punishment_decision = punishment_decision_raw.group(1).strip()
        elif announcement_type == '公开谴责':
            title = content_soup.find(attrs={"class": 'xl_cen'}).find(attrs={"class": 'xl_h'}).text.strip()
            article = content_soup.find(attrs={"class": 'TRS_Editor'}).text.strip()
            litigant = re.search(r'(.*)[:：]', article).group(1).strip()
            facts = re.search(r'(根据中国证监会.*有关规定。)', article).group(1).strip()
            defense_opinion = re.search(r'(我会认为.*抵制协会的调查。)', article).group(1).strip()
            punishment_decision = re.search(r'(为了严肃协会自律规则.*信息记录。)', article).group(1).strip()
            publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', article)[-1].strip()
            m = re.match("([0-9零一二两三四五六七八九十〇○]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
            announcement_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
        elif announcement_type == '纪律处分':
            title = content_soup.find(attrs={"class": 'xl_cen'}).find(attrs={"class": 'xl_h'}).text.strip()
            article = content_soup.find(attrs={"class": 'TRS_Editor'}).text.strip()
            announcement_code = re.search(r'(.*号)', article).group(1).strip()
            litigant = re.search(r'当事人[:：](.*)[,，]', article).group(1).strip()
            defense_opinion = re.search(r'(作为.*条。)', article).group(1).strip()
            facts = re.search(r'(深圳.*元。)', article).group(1).strip()
            punishment_decision = re.search(r'(依据.*注册申请。)', article).group(1).strip()
            publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', article)[-1].strip()
            m = re.match("([0-9零一二两三四五六七八九十〇○]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
            announcement_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
        result_map = {
            'announcementTitle': announcement_title,
            'announcementOrg': '证券业协会',
            'announcementDate': announcement_date,
            'announcementCode': announcement_code,
            'facts': facts,
            'defenseOpinion': defense_opinion,
            'defenseResponse': defense_response,
            'litigant': litigant,
            'punishmentBasement': punishment_basement,
            'punishmentDecision': remove_special_char(punishment_decision),
            'type': announcement_type,
            'oss_file_id': file_id,
            'status': 'not checked'
        }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('证券业协会 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('证券业协会 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('证券业协会 数据解析 ' + ' -- 修改parsed完成')


def parse():
    sac_parse()


if __name__ == "__main__":
    parse()