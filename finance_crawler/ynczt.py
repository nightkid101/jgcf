from pymongo import MongoClient
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from utility import request_site_page
from init import logger_init, config_init

logger = logger_init('云南省财政厅-数据抓取')
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

# 抓取数据存入finance_data这个collection
db.finance_data.create_index([('url', 1)])


def ynczt_crawler():
    result_list = []  # 用来保存最后存入数据库的数据
    prefix_url = [
        {
            'url': 'http://www.ynf.gov.cn/zxzx/tzgg_gk/', 'origin': '云南省财政厅'
        }
    ]
    for each_url_info in prefix_url:
        each_url = each_url_info['url']
        stop_flag = False
        logger.info('云南省财政厅 抓取URL：' + each_url)

        if db.crawler.find({'url': each_url}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': each_url})['last_updated']
        else:
            last_updated_url = ''

        # get crawler data
        url = each_url
        page_response = request_site_page(url)
        if page_response is None:
            logger.error('网页请求错误 %s' % url)
            continue
        count = 0
        while page_response.status_code != 404:
            try:
                logger.info('第' + str(count + 1) + '页')
                page_response.encoding = page_response.apparent_encoding
                page_soup = BeautifulSoup(page_response.text, 'lxml')
                all_li = page_soup.find(attrs={"class": 'fl marl30 wid640 mart20 list'}).find_all('li')
                for index, each_result in enumerate(all_li):
                    title = each_result.find('a').attrs['title'].strip()
                    href = each_result.find('a').attrs['href'].strip()
                    true_url = urljoin(url, href)

                    # 判断是否为之前抓取过的
                    if true_url == last_updated_url:
                        stop_flag = True
                        logger.info('到达上次爬取的链接')
                        break

                    # 更新抓取的分割线
                    if count == 0 and index == 0:
                        if db.crawler.find({'url': each_url}).count() > 0:
                            if db.crawler.find_one({'url': each_url})['last_updated'] != true_url:
                                db.crawler.update_one({'url': each_url}, {'$set': {'last_updated': true_url}})
                        else:
                            db.crawler.insert_one(
                                {'url': each_url, 'last_updated': true_url, 'origin': each_url_info['origin']})

                    if re.search('.*(撤回.*?执业许可).*', title):
                        publish_date = each_result.find('p').text.strip()
                        if db.finance_data.find({'url': true_url}).count() == 0:
                            logger.info('云南省财政厅新公告：' + true_url + ' title: ' + title)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': true_url,
                                'type': '撤回行政许可',
                                'origin': '云南省财政厅',
                                'status': 'not parsed'
                            }
                            if post not in result_list:
                                result_list.append(post)
                        else:
                            if config['crawler_update_type']['update_type'] == '0':
                                break
                if stop_flag:
                    logger.info('到达上次爬取的链接')
                    break
            except Exception as e:
                logger.error(e)
                continue
            count += 1
            url = each_url + 'index_' + str(count) + '.html'
            page_response = request_site_page(url)
            if page_response is None:
                logger.error('网页请求错误 %s' % url)
                continue

    if len(result_list) > 0:
        logger.info('云南省财政厅一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.finance_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('云南省财政厅公告导入完成！')
        else:
            logger.error('云南省财政厅公告导入出现问题！')
    else:
        logger.info('云南省财政厅没有新公告！')


if __name__ == "__main__":
    ynczt_crawler()
