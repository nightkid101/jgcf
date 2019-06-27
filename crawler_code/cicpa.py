from pymongo import MongoClient
import re
from utility import request_site_page
from bs4 import BeautifulSoup as bs
from init import logger_init, config_init
from urllib.parse import urljoin

logger = logger_init('注会协会-数据抓取')
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

# 抓取数据存入cicpa_data这个collection
db.cicpa_data.create_index([('url', 1)])


def cicpa_crawler():
    result_list = []  # 用来保存最后存入数据库的数据
    prefix_url = [{'url': 'http://www.cicpa.org.cn/Industry_regulation/Monitoring_info/index', 'origin': '注册会计师协会'}]
    for each_url_info in prefix_url:
        each_url = each_url_info['url']
        stop_flag = False
        logger.info('注册会计师协会 抓取URL：' + each_url + '.html')
        # get page count
        response = request_site_page(each_url + '.html')
        if response is None:
            logger.error('网页请求错误 %s' % (each_url + '.html'))
            continue
        soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
        page_count = int(re.search(r'var countPage = (\d+)', soup.text).group(1).strip())
        logger.info('注册会计师协会' + ' 一共有%d页' % page_count)

        if db.crawler.find({'url': each_url + '.html'}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': each_url + '.html'})['last_updated']
        else:
            last_updated_url = ''

        # get data
        for page_num in range(page_count):
            logger.info('注册会计师协会 -- 第%d页' % (page_num + 1))
            if page_num == 0:
                page_url = each_url + '.html'
            else:
                page_url = each_url + '_' + str(page_num) + '.html'
            try:
                page_response = request_site_page(page_url)
                if response is None:
                    logger.error('网页请求错误 %s' % page_url)
                    continue
                page_soup = bs(page_response.content, 'lxml')
                all_result = page_soup.find(class_='news-next-list').find_all('li')

                for index, each_result in enumerate(all_result):
                    href = re.search('<a href="(.*?)"', each_result.text.strip()).group(1).strip()
                    true_url = urljoin(page_url, href)

                    # 判断是否为之前抓取过的
                    if true_url == last_updated_url:
                        stop_flag = True
                        logger.info('到达上次爬取的链接')
                        break

                    # 更新抓取的分割线
                    if page_num == 0 and index == 0:
                        if db.crawler.find({'url': each_url + '.html'}).count() > 0:
                            if db.crawler.find_one({'url': each_url + '.html'})['last_updated'] != true_url:
                                db.crawler.update_one({'url': each_url + '.html'}, {'$set': {'last_updated': true_url}})
                        else:
                            db.crawler.insert_one(
                                {'url': each_url + '.html', 'last_updated': true_url, 'origin': each_url_info['origin']})

                    title = re.search('target="_blank">(.*?)</a>', each_result.text.strip()).group(1).strip()
                    if re.search(r'对.*[做作]出惩戒', title) or \
                            (re.search(r'约谈', title) and not re.search(r'约谈工作|提示.*风险', title)):
                        publish_date = re.search(r'\((\d{4}-\d{1,2}-\d{1,2})\)', each_result.text.strip()).group(1).strip()
                        if db.cicpa_data.find({'url': true_url}).count() == 0:
                            logger.info('注册会计师协会新公告：' + true_url + ' title:' + title)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': true_url,
                                'type': '',
                                'origin': '注册会计师协会',
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
        logger.info('注册会计师协会一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.cicpa_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('注册会计师协会公告导入完成！')
        else:
            logger.error('注册会计师协会公告导入出现问题！')
    else:
        logger.info('注册会计师协会没有新公告！')


if __name__ == "__main__":
    cicpa_crawler()
