import re

from pymongo import MongoClient
from utility import request_site_page
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin
from init import logger_init, config_init

logger = logger_init('交易商协会-数据抓取')
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

# 抓取数据存入nafmii_data这个collection
db.nafmii_data.create_index([('url', 1)])


# 中国银行间市场交易商协会
def nafmii_crawler():
    result_list = []
    prefix_url = 'http://www.nafmii.org.cn/zlgl/zwrz/zlcf/'

    # get page count
    response = request_site_page(prefix_url)
    if response is None:
        logger.error('网页请求错误 %s' % prefix_url)
        return
    page_count = int(re.search(r'var countPage = (\d+)', response.text).group(1).strip())
    logger.info('交易商协会' + ' 一共有%d页' % page_count)

    # get crawler data
    for num in range(page_count):
        logger.info('交易商协会 -- 第%d页' % (num + 1))
        if num == 0:
            url = prefix_url + 'index.html'
        else:
            url = prefix_url + 'index_' + str(num) + '.html'

        try:
            content_response = request_site_page(url)
            if content_response is None:
                logger.error('网页请求错误 %s' % url)
                continue

            content_soup = bs(content_response.content, 'lxml') if content_response else ''
            table_content = content_soup.find_all('table')[-4]
            for each_tr in table_content.find_all('tr')[2:-2]:
                try:
                    announcement_url = urljoin(url, each_tr.find('a').attrs['href'].strip())
                    if db.nafmii_data.find({'url': announcement_url}).count() == 0:
                        title = each_tr.find('a').text.strip()
                        publish_date = each_tr.find_all('td')[-1].text.replace('/', '-').strip()
                        logger.info('交易商协会 -- 新公告：' + announcement_url)
                        post = {
                            'title': title,
                            'publishDate': publish_date,
                            'url': announcement_url,
                            'type': '行政处罚决定',
                            'origin': '交易商协会',
                            'status': 'not parsed'
                        }
                        if post not in result_list:
                            result_list.append(post)
                    else:
                        if config['crawler_update_type']['update_type'] == '0':
                            break
                except Exception as e:
                    logger.error(e)
                    logger.warning('提取公告url出现问题')
                    continue
        except Exception as e:
            logger.error(e)
            continue

    if len(result_list) > 0:
        logger.info('交易商协会 -- 一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.nafmii_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('交易商协会 -- 公告导入完成！')
        else:
            logger.error('交易商协会 -- 公告导入出现问题！')
    else:
        logger.info('交易商协会 -- 没有新公告！')


if __name__ == "__main__":
    nafmii_crawler()
