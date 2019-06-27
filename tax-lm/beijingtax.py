import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

from init import logger_init, config_init
from utility import request_site_page

# 北京市税务局
# http://beijing.chinatax.gov.cn/bjswj/bsfw/sscx/xzcf/
# http://beijing.chinatax.gov.cn/bjsat/office/jsp/zdsswfaj/wwquery.jsp
first_url = 'http://beijing.chinatax.gov.cn/bjswj/bsfw/sscx/xzcf/'
second_url = 'http://beijing.chinatax.gov.cn/bjsat/office/jsp/zdsswfaj/wwquery'
second_url_with_id = 'http://beijing.chinatax.gov.cn/bjsat/office/jsp/zdsswfaj/wwidquery'
gov_name = '北京市税务局'
collection_name = 'tax_data'

address_list = [
    '东城', '西城', '朝阳', '海淀', '丰台', '石景山', '门头沟', '房山', '通州', '顺义',
    '大兴', '昌平', '平谷', '怀柔', '密云', '延庆', '开发区', '西站', '燕山'
]
data = {
    'dq': '东城',
    'bz': 'dq',
    'dqy': 1,
    'orgCode': '11100000000'
}
data_with_id = {
    'dq': '东城',
    'bz': 'dq',
    'dqy': 1,
    'orgCode': '11100000000',
    'id': '000000000000000631'
}

logger = logger_init(gov_name)
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

db[collection_name].create_index([('url', 1)])


def crawler():
    result_list = []
    base_response = request_site_page(first_url)
    base_response.encoding = base_response.apparent_encoding
    if base_response is None:
        logger.error('网页请求错误{}'.format(first_url))
    base_soup = bs(base_response.content if base_response else '', 'lxml')

    # 先抓取每个分局的网站和名称
    department_list = []
    all_department = base_soup.find(attrs={'class': "no_text_content"}).find_all('p')
    for each_department in all_department:
        department_name = each_department.text.strip()
        department_url = each_department.find('a')['href']
        department_list.append({"name": department_name, "url": department_url})
    # 抓取每个分局的行政处罚判决书
    for each_department_info in department_list:
        logger.info('抓取分局{}，url:{}'.format(each_department_info['name'], each_department_info['url']))
        url_format = each_department_info['url'] + 'index{}.html'
        url = url_format.format('')
        if db.crawler.find({'url': url_format.format('')}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': url_format.format('')})['last_updated']
        else:
            last_updated_url = ''
        response = request_site_page(url)
        response.encoding = response.apparent_encoding
        soup = bs(response.text, 'lxml')
        page_count = 1
        stop_flag = False
        while response.status_code != 404 and response.status_code != 400:
            logger.info('第%d页' % page_count)
            try:
                info_list = soup.find(attrs={"class": "list_box"}).find_all('li')
                for index, each_info in enumerate(info_list):
                    href = each_info.find('a')['href']
                    anc_url = urljoin(url, href)

                    if anc_url == last_updated_url:
                        stop_flag = True
                        logger.info('到达上次爬取的链接')
                        break

                    if index == 0 and page_count == 1:
                        if db.crawler.find({'url': url_format.format('')}).count() > 0:
                            if db.crawler.find_one({'url': url_format.format('')})['last_updated'] != anc_url:
                                db.crawler.update_one({'url': url_format.format('')},
                                                      {'$set': {'last_updated': anc_url}})
                        else:
                            db.crawler.insert_one(
                                {'url': url_format.format(''), 'last_updated': anc_url, 'origin': gov_name})
                    publish_date = each_info.find(attrs={"class": "list_time"}).text.strip()
                    title = each_info.find('a')['title']

                    if db[collection_name].count_documents({'url': anc_url}) == 0:
                        info = {
                            'title': title,
                            'publishDate': publish_date,
                            'url': anc_url,
                            'type': '行政处罚决定',
                            'origin': gov_name,
                            'status': 'not parsed'
                        }
                        logger.info('{} 新公告：{} url: {}'.format(gov_name, info['title'], anc_url))
                        if info not in result_list:
                            result_list.append(info)
                    else:
                        if config['crawler_update_type']['update_type'] == '0':
                            break
                if stop_flag:
                    logger.info('到达上次爬取的链接')
                    break
                url = url_format.format('_' + str(page_count))
                response = request_site_page(url)
                response.encoding = response.apparent_encoding
                soup = bs(response.text, 'lxml')
                page_count += 1
            except Exception as e:
                logger.error(e)
                logger.warning('提取公告url出现问题')
                continue
    base_response = request_site_page(second_url + '.jsp')
    if base_response is None:
        logger.error('网页请求错误{}'.format(second_url))

    for index, each_address in enumerate(address_list):
        if db.crawler.find({'url': each_address + ' ' + second_url}).count() > 0:
            last_updated_facts = db.crawler.find_one({'url': each_address + ' ' + second_url})['last_updated']
        else:
            last_updated_facts = ''
        logger.info('第%d个分区抓取' % (index+1))
        data['dq'] = each_address
        data['dqy'] = 1
        response = request_site_page(second_url, data=data, methods='post')
        response.encoding = response.apparent_encoding
        soup = bs(response.content, 'lxml')
        page_count = re.search('共\d+项查询结果 (\d+) 页', soup.text).group(1).strip()
        page_num = 1
        stop_flag = False
        while page_num <= int(page_count):
            logger.info('第%d页' % page_num)
            data_list = soup.body.find('td').find_all('table')[1].find_all('tr')
            for index, each_data in enumerate(data_list):
                # 如果为最后一行，则跳过，进入下一页抓取
                if re.search('页面大小', each_data.text.strip()):
                    break
                article_id_text = each_data.find('input')['onclick']
                article_id = re.search('(\d+)', article_id_text).group(1).strip()
                data_with_id['id'] = article_id
                sub_response = request_site_page(second_url_with_id, data=data_with_id, methods='post')
                sub_response.encoding = sub_response.apparent_encoding
                sub_soup = bs(sub_response.content, 'lxml')
                # 找到处罚信息所有的行
                all_tr = sub_soup.body.find('td').find_all('tr')
                # 预设需要提取的字段
                litigant = ''   # 纳税人名称
                social_credit_code = ''  # 纳税人识别号或社会信用代码
                org_code = ''   # 组织机构代码
                address = ''    # 注册地址
                name = ''       # 违法期间法人代表或者负责人姓名
                nature = ''     # 案件性质
                facts = ''      # 主要违法事实
                for each_tr in all_tr:
                    if each_tr.text.strip() == '':
                        break
                    all_td = each_tr.find_all('td')
                    td_type = all_td[0].text.strip()
                    td_value = all_td[1].text.strip()
                    if re.search('纳税人名称', td_type):
                        litigant = td_value
                    if re.search('纳税人识别号或社会信用代码', td_type):
                        social_credit_code = td_value
                    if re.search('组织机构代码', td_type):
                        org_code = td_value
                    if re.search('注册地址', td_type):
                        address = td_value
                    if re.search('违法期间法人代表或者负责人姓名', td_type):
                        name = td_value
                    if re.search('案件性质', td_type):
                        nature = td_value
                    if re.search('主要违法事实', td_type):
                        facts = td_value
                info = {
                    'title': name,
                    'publishDate': '',
                    'url': '',
                    'litigant': litigant,
                    'social_credit_code': social_credit_code,
                    'org_code': org_code,
                    'address': address,
                    'nature': nature,
                    'facts': facts,
                    'type': '行政处罚决定',
                    'origin': gov_name,
                    'status': 'not parsed'
                }
                if facts == last_updated_facts:
                    stop_flag = True
                    logger.info('到达上次爬取的链接')
                    break

                if index == 0 and page_count == 1:
                    if db.crawler.find({'url': each_address + ' ' + second_url}).count() > 0:
                        if db.crawler.find_one({'url': each_address + ' ' + second_url})['last_updated'] != facts:
                            db.crawler.update_one({'url': each_address + ' ' + second_url},
                                                  {'$set': {'last_updated': facts}})
                    else:
                        db.crawler.insert_one(
                            {'url': each_address + ' ' + second_url, 'last_updated': facts, 'origin': gov_name})
                logger.info('{} 新公告：{}'.format(gov_name, info['title']))
                if info not in result_list:
                    result_list.append(info)
            if stop_flag:
                logger.info('到达上次爬取的链接')
                break
            page_num += 1
            data['dqy'] = page_num
            response = request_site_page(second_url, data=data, methods='post')
            response.encoding = response.apparent_encoding
            soup = bs(response.content, 'lxml')
    if len(result_list) > 0:
        logger.info('{}一共有{}条新公告，导入数据库中......'.format(gov_name, len(result_list)))
        r = db[collection_name].insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('{}公告导入完成！'.format(gov_name))
        else:
            logger.error('{}公告导入出现问题！'.format(gov_name))
    else:
        logger.info('{}没有新公告！'.format(gov_name))


if __name__ == '__main__':
    crawler()
