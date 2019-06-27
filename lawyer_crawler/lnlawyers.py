from pymongo import MongoClient
import requests
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin
import math
import re
from init import logger_init, config_init

logger = logger_init('辽宁省律师协会-数据抓取')
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

# 抓取数据存入lawyers_data这个collection
db.lawyers_data.create_index([('url', 1)])


def lnlawyers_crawler():
    result_list = []  # 用来保存最后存入数据库的数据
    url = 'http://www.lnlawyers.net/ItemList.aspx?ItemCode=3'
    logger.info('辽宁省律师协会 抓取URL：' + url)

    s = requests.Session()
    s.get('http://www.lnlawyers.net')

    data = {
        'RaiseException': '',
        '__VIEWSTATE': '',
        '__EVENTTARGET': 'AspNetPager1',
        '__EVENTARGUMENT': '',
        '__VIEWSTATEENCRYPTED': '',
        'AspNetPager1_input': ''
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,zh-TW;q=0.7',
        'Cache-Control': 'no-cache',
        'Content-Length': '18612',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Host': 'www.lnlawyers.net',
        'Origin': 'http://www.lnlawyers.net',
        'Pragma': 'no-cache',
        'Proxy-Connection': 'keep-alive',
        'Referer': 'http://www.lnlawyers.net/ItemList.aspx?ItemCode=3',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
    }

    if db.crawler.find({'url': url}).count() > 0:
        last_updated_url = db.crawler.find_one({'url': url})['last_updated']
    else:
        last_updated_url = ''

    # 第一页
    r = s.get(url)
    # 得到ViewState、RaiseException以及总页数
    soup = bs(r.text, 'lxml')
    page_count = math.ceil(int(soup.find(id='spanRowCount').text) / 20)
    raise_exception = soup.find(id='RaiseException').get('value')
    view_state = soup.find(id='__VIEWSTATE').get('value')
    logger.info('第1页')
    article = soup.find(attrs={"class": 'w712 h935 sideline_blue_RLTB p12'})
    url_list = article.find_all(attrs={"class": 'w587 pl25 h37 lh37 tl list_bg fl f12'})
    date_list = article.find_all(attrs={"class": ' h37 lh37 tl fl f12 w100'})
    for index, each_li in enumerate(url_list):
        href = each_li.find('a').attrs['href'].strip()
        title = each_li.find('a').attrs['title'].strip()
        true_url = urljoin(url, href)
        # 判断是否为之前抓取过的
        if true_url == last_updated_url:
            page_count = 0
            logger.info('到达上次爬取的链接')
            break

        # 更新抓取的分割线
        if index == 0:
            if db.crawler.find({'url': url}).count() > 0:
                if db.crawler.find_one({'url': url})['last_updated'] != true_url:
                    db.crawler.update_one({'url': url}, {'$set': {'last_updated': true_url}})
            else:
                db.crawler.insert_one({'url': url, 'last_updated': true_url, 'origin': '辽宁省律师协会'})
        if re.search(r'纪律处分情况|撤销行政许可决定书', title):
            publish_date_raw = date_list[index].text.strip()
            publish_date = re.search(r'(.{4}-.{2}-.{2})', publish_date_raw).group(1).strip()

            if db.lawyers_data.find({'url': true_url}).count() == 0:
                logger.info('辽宁省律师协会新公告：' + true_url + ' title:' + title)
                post = {
                    'title': title,
                    'publishDate': publish_date,
                    'url': true_url,
                    'type': '',
                    'origin': '辽宁省律师协会',
                    'status': 'not parsed'
                }
                if post not in result_list:
                    result_list.append(post)
            else:
                if config['crawler_update_type']['update_type'] == '0':
                    break
    # 第二页开始
    for i in range(page_count):
        if i == 0:
            continue
        data['RaiseException'] = raise_exception
        data['__VIEWSTATE'] = view_state
        data['__EVENTARGUMENT'] = str(i + 1)
        data['AspNetPager1_input'] = str(i)
        r = s.post(url, data=data, headers=headers)
        soup = bs(r.text, 'lxml')
        logger.info('第' + str(i + 1) + '页')
        article = soup.find(attrs={"class": 'w712 h935 sideline_blue_RLTB p12'})
        url_list = article.find_all(attrs={"class": 'w587 pl25 h37 lh37 tl list_bg fl f12'})
        date_list = article.find_all(attrs={"class": ' h37 lh37 tl fl f12 w100'})
        for index, each_li in enumerate(url_list):
            href = each_li.find('a').attrs['href'].strip()
            title = each_li.find('a').attrs['title'].strip()
            true_url = urljoin(url, href)
            # 判断是否为之前抓取过的
            if true_url == last_updated_url:
                page_count = 0
                logger.info('到达上次爬取的链接')
                break

            # 更新抓取的分割线
            if index == 0:
                if db.crawler.find({'url': url}).count() > 0:
                    if db.crawler.find_one({'url': url})['last_updated'] != true_url:
                        db.crawler.update_one({'url': url}, {'$set': {'last_updated': true_url}})
                else:
                    db.crawler.insert_one({'url': url, 'last_updated': true_url, 'origin': '辽宁省律师协会'})
            if re.search(r'纪律处分情况|撤销行政许可决定书', title):
                publish_date_raw = date_list[index].text.strip()
                publish_date = re.search(r'(.{4}-.{2}-.{2})', publish_date_raw).group(1).strip()

                if db.lawyers_data.find({'url': true_url}).count() == 0:
                    logger.info('辽宁省律师协会新公告：' + true_url + ' title:' + title)
                    post = {
                        'title': title,
                        'publishDate': publish_date,
                        'url': true_url,
                        'type': '',
                        'origin': '辽宁省律师协会',
                        'status': 'not parsed'
                    }
                    if post not in result_list:
                        result_list.append(post)
                else:
                    if config['crawler_update_type']['update_type'] == '0':
                        break
        raise_exception = soup.find(id='RaiseException').get('value')
        view_state = soup.find(id='__VIEWSTATE').get('value')
    if len(result_list) > 0:
        logger.info('辽宁省律师协会一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.lawyers_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('辽宁省律师协会公告导入完成！')
        else:
            logger.error('辽宁省律师协会公告导入出现问题！')
    else:
        logger.info('辽宁省律师协会没有新公告！')


if __name__ == "__main__":
    lnlawyers_crawler()
