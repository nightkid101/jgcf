from pymongo import MongoClient
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from utility import request_site_page
from init import logger_init, config_init

logger = logger_init('河北省财政局-数据抓取')
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


def hebcz_crawler():
    result_list = []  # 用来保存最后存入数据库的数据
    prefix_url = [
        {'url': 'http://www.hebcz.gov.cn/root17/3003/3042/3044/list_292', 'type': '监管关注', 'origin': '河北省财政厅'},
        {'url': 'http://www.hebcz.gov.cn/root17/3003/3042/3045/list_292', 'type': '行政处罚决定', 'origin': '河北省财政厅'}
    ]
    for each_url_info in prefix_url:
        each_url = each_url_info['url']
        stop_flag = False
        logger.info('河北省财政厅 ' + each_url_info['type'] + ' 抓取URL：' + each_url + '.htm')
        # get page count
        base_page = request_site_page(each_url + '.htm')
        if base_page is None:
            logger.error('网页请求错误 %s' % each_url + '.htm')
            continue
        base_soup = BeautifulSoup(base_page.text.encode(base_page.encoding).decode('utf-8'), 'lxml')
        try:
            page_count_text = re.search(r'var m_nRecordCount = "(.*?)";', base_soup.text).group(1).strip()
            page_count = int(int(page_count_text) / 15) + 1
        except Exception as e:
            logger.warning(e)
            page_count = 0
        logger.info('一共有%d页' % page_count)

        if db.crawler.find({'url': each_url}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': each_url})['last_updated']
        else:
            last_updated_url = ''

        # get crawler data
        for page_num in range(page_count):
            logger.info('第' + str(page_num + 1) + '页')
            if page_num == 0:
                url = each_url + '.htm'
            else:
                url = each_url + '_' + str(page_num) + '.htm'

            try:
                page_response = request_site_page(url)
                if page_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                page_soup = BeautifulSoup(page_response.text.encode(page_response.encoding).decode('utf-8'), 'lxml')
                all_li = page_soup.find(attrs={"class": 'content'}).find_all(attrs={"class": 'xitable'})
                for index, each_result in enumerate(all_li):
                    title = each_result.find('td').find('a').text.strip()
                    href_0 = each_result.find('td').find('a')['href'].strip()
                    code = each_result.find_all('td')[1].text.strip()
                    true_url = urljoin(url, href_0)
                    date = each_result.find_all('td')[-1].text.strip()
                    publish_date = re.search(r'(\d{4}-\d{2}-\d{2})', date).group(1).strip()

                    # 判断是否为之前抓取过的
                    if true_url == last_updated_url:
                        stop_flag = True
                        logger.info('到达上次爬取的链接')
                        break

                    # 更新抓取的分割线
                    if page_num == 0 and index == 0:
                        if db.crawler.find({'url': each_url}).count() > 0:
                            if db.crawler.find_one({'url': each_url})['last_updated'] != true_url:
                                db.crawler.update_one({'url': each_url},
                                                      {'$set': {'last_updated': true_url}})
                        else:
                            db.crawler.insert_one(
                                {'url': each_url, 'last_updated': true_url, 'origin': each_url_info['origin']})

                    if db.finance_data.find({'url': true_url}).count() == 0:
                        logger.info('河北省财政厅新公告：' + true_url + ' title: ' + title)
                        post = {
                            'title': title,
                            'publishDate': publish_date,
                            'url': true_url,
                            'type': each_url_info['type'],
                            'code': code,
                            'origin': '河北省财政厅',
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

    if len(result_list) > 0:
        logger.info('河北省财政厅一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.finance_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('河北省财政厅公告导入完成！')
        else:
            logger.error('河北省财政厅公告导入出现问题！')
    else:
        logger.info('河北省财政厅没有新公告！')


if __name__ == "__main__":
    hebcz_crawler()
