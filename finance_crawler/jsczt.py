from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup as bs
import re
from init import logger_init, config_init

logger = logger_init('江苏省财政厅-数据抓取')
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


def jiangsuczt_crawler():
    result_list = []  # 用来保存最后存入数据库的数据

    url = 'http://czt.jiangsu.gov.cn/module/xxgk/search.jsp'
    s = requests.Session()
    s.get('http://czt.jiangsu.gov.cn')
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Content-Length': '282',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Host': 'czt.jiangsu.gov.cn',
        'Origin': 'http://czt.jiangsu.gov.cn',
        'Referer': 'http://czt.jiangsu.gov.cn/col/col51215/index.html',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }
    data = {
        'divid': 'div51143',
        'infotypeId': '01010H',
        'jdid': '35',
        'area': ''
    }
    stop_flag = False
    r = s.post(url, data=data, headers=headers)
    r.encoding = r.apparent_encoding
    soup = bs(r.text, 'lxml')
    page_text = soup.find(attrs={"class": 'tb_title'}).text
    page_num = int(re.search(r'共(\d+)页', page_text).group(1).strip())
    logger.info('江苏省财政厅 一共有%d页' % page_num)
    page_count = 1

    if db.crawler.find({'url': url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': url})['last_updated']
    else:
        last_updated_url = ''

    while page_count <= int(page_num) and not stop_flag:
        logger.info('第' + str(page_count) + '页')
        article = soup.find_all(attrs={"class": 'tr_main_value_odd'})
        for index, each_article in enumerate(article):
            title = each_article.find('a').text
            true_url = each_article.find('td').find('a')['href']
            # 判断是否为之前抓取过的
            if true_url == last_updated_url:
                stop_flag = True
                logger.info('到达上次爬取的链接')
                break

            # 更新抓取的分割线
            if page_count == 1 and index == 0:
                if db.crawler.find({'url': url}).count() > 0:
                    if db.crawler.find_one({'url': url})['last_updated'] != true_url:
                        db.crawler.update_one({'url': url}, {'$set': {'last_updated': true_url}})
                else:
                    db.crawler.insert_one({'url': url, 'last_updated': true_url, 'origin': '江苏省财政厅'})

            if re.search('处罚决定', title):
                publish_date = each_article.find_all('td')[-1].text
                if db.finance_data.find({'url': true_url}).count() == 0:
                    logger.info('江苏省财政厅新公告：' + true_url + ' title: ' + title)
                    post = {
                        'title': title,
                        'publishDate': publish_date,
                        'url': true_url,
                        'type': '',
                        'origin': '江苏省财政厅',
                        'status': 'not parsed'
                    }
                    if post not in result_list:
                        result_list.append(post)
                else:
                    if config['crawler_update_type']['update_type'] == '0':
                        break
        article = soup.find_all(attrs={"class": 'tr_main_value_even'})
        for index, each_article in enumerate(article):
            title = each_article.find('a').text
            true_url = each_article.find('td').find('a')['href']
            # 判断是否为之前抓取过的
            if true_url == last_updated_url:
                stop_flag = True
                logger.info('到达上次爬取的链接')
                break

            # 更新抓取的分割线
            if page_count == 0 and index == 0:
                if db.crawler.find({'url': url}).count() > 0:
                    if db.crawler.find_one({'url': url})['last_updated'] != true_url:
                        db.crawler.update_one({'url': url}, {'$set': {'last_updated': true_url}})
                else:
                    db.crawler.insert_one({'url': url, 'last_updated': true_url, 'origin': '江苏省财政厅'})
            if re.search('处罚决定', title):
                publish_date = each_article.find_all('td')[-1].text
                if db.finance_data.find({'url': true_url}).count() == 0:
                    logger.info('江苏省财政厅新公告：' + true_url + ' title: ' + title)
                    post = {
                        'title': title,
                        'publishDate': publish_date,
                        'url': true_url,
                        'type': '',
                        'origin': '江苏省财政厅',
                        'status': 'not parsed'
                    }
                    if post not in result_list:
                        result_list.append(post)
                else:
                    if config['crawler_update_type']['update_type'] == '0':
                        break
        page_count += 1
        data['currpage'] = str(page_count)
        r = s.post(url, data=data, headers=headers)
        r.encoding = r.apparent_encoding
        soup = bs(r.text, 'lxml')

    if len(result_list) > 0:
        logger.info('江苏省财政厅一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.finance_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('江苏省财政厅公告导入完成！')
        else:
            logger.error('江苏省财政厅公告导入出现问题！')
    else:
        logger.info('江苏省财政厅没有新公告！')


if __name__ == "__main__":
    jiangsuczt_crawler()
