from pymongo import MongoClient
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from utility import request_site_page
from init import logger_init, config_init

logger = logger_init('厦门市财政局-数据抓取')
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


def xmczj_crawler():
    result_list = []  # 用来保存最后存入数据库的数据
    prefix_url = [
        {'url': 'http://cz.xm.gov.cn/zwxx/zczd/bjcz/', 'origin': '厦门市财政局'}
    ]
    prefix_url2 = [
        {'url': 'http://cz.xm.gov.cn/ztzl/xzxkhxzcfgs/', 'origin': '厦门市财政局'}
    ]
    for each_url_info in prefix_url:
        each_url = each_url_info['url']
        stop_flag = False
        logger.info('厦门市财政局' + ' 抓取URL：' + each_url)
        # get page count
        base_page = request_site_page(each_url)
        if base_page is None:
            logger.error('网页请求错误 %s' % each_url)
            continue
        base_soup = BeautifulSoup(base_page.text.encode(base_page.encoding).decode('utf-8'), 'lxml')
        try:
            page_count_text = base_soup.find(id='table12').text.strip()
            page_count = int(re.findall(r'\d+', page_count_text)[3])
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
                url = each_url + 'index.htm'
            else:
                url = each_url + 'index_' + str(page_num) + '.htm'

            try:
                page_response = request_site_page(url)
                if page_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                page_soup = BeautifulSoup(page_response.text.encode(page_response.encoding).decode('utf-8'), 'lxml')
                all_li = page_soup.find(attrs={"class": 'table1'}).find_all('tr')[1:]
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
                    if page_num == 0 and index == 0:
                        if db.crawler.find({'url': each_url}).count() > 0:
                            if db.crawler.find_one({'url': each_url})['last_updated'] != true_url:
                                db.crawler.update_one({'url': each_url}, {'$set': {'last_updated': true_url}})
                        else:
                            db.crawler.insert_one(
                                {'url': each_url, 'last_updated': true_url, 'origin': each_url_info['origin']})

                    if re.search('.*?(行政处罚决定书|政府采购投诉处理决定书|政府采购监督意见书).*?', title):
                        announcement_type = re.search(
                            '.*?(行政处罚决定书|政府采购投诉处理决定书|政府采购监督意见书).*?', title).group(1).strip()
                        if announcement_type == '行政处罚决定书':
                            announcement_type = '行政处罚决定'
                        elif announcement_type == '政府采购投诉处理决定书':
                            announcement_type = '行政处罚决定'
                        else:
                            announcement_type = '监管措施'
                        publish_date = each_result.find_all('td')[-1].text.strip()
                        if db.finance_data.find({'url': true_url}).count() == 0:
                            logger.info('厦门市财政局新公告：' + true_url + ' title: ' + title)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': true_url,
                                'type': announcement_type,
                                'origin': '厦门市财政局',
                                'status': 'not parsed',
                                'code': each_result.find_all('td')[-2].text.strip()
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

    for each_url_info in prefix_url2:
        each_url = each_url_info['url']
        stop_flag = False
        logger.info('厦门市财政局' + ' 抓取URL：' + each_url)
        # get page count
        base_page = request_site_page(each_url)
        if base_page is None:
            logger.error('网页请求错误 %s' % each_url)
            continue
        base_soup = BeautifulSoup(base_page.text.encode(base_page.encoding).decode('utf-8'), 'lxml')
        try:
            page_count_text = base_soup.find(id='table12').text.strip()
            page_count = int(re.findall(r'\d+', page_count_text)[3])
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
                url = each_url + 'index.htm'
            else:
                url = each_url + 'index_' + str(page_num) + '.htm'

            try:
                page_response = request_site_page(url)
                if page_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                page_soup = BeautifulSoup(page_response.text.encode(page_response.encoding).decode('utf-8'), 'lxml')
                all_li = page_soup.find(attrs={"class": 'p_news01'}).find_all('li')
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
                    if page_num == 0 and index == 0:
                        if db.crawler.find({'url': each_url}).count() > 0:
                            if db.crawler.find_one({'url': each_url})['last_updated'] != true_url:
                                db.crawler.update_one({'url': each_url}, {'$set': {'last_updated': true_url}})
                        else:
                            db.crawler.insert_one(
                                {'url': each_url, 'last_updated': true_url, 'origin': each_url_info['origin']})

                    if re.search('.*?行政处罚决定书.*?', title):
                        publish_date = each_result.find_all('span')[-1].text.strip()
                        if db.finance_data.find({'url': true_url}).count() == 0:
                            logger.info('厦门市财政局新公告：' + true_url + ' title: ' + title)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': true_url,
                                'type': '行政处罚决定',
                                'origin': '厦门市财政局',
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
        logger.info('厦门市财政局一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.finance_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('厦门市财政局公告导入完成！')
        else:
            logger.error('厦门市财政局公告导入出现问题！')
    else:
        logger.info('厦门市财政局没有新公告！')


if __name__ == "__main__":
    xmczj_crawler()
