import re

from pymongo import MongoClient
from utility import request_site_page
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin
from init import logger_init, config_init

logger = logger_init('中国基金业协会-数据抓取')
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

# 抓取数据存入amac_data这个collection
db.amac_data.create_index([('url', 1)])


def amac_crawler():
    result_list = []
    # 行业自律
    prefix_url = ['http://www.amac.org.cn/xxgs/jlcf/index']
    # 不予登记机构
    prefix_url0 = ['http://www.amac.org.cn/xxgs/bydjjg/index']
    # 黑名单
    prefix_url1 = ['http://www.amac.org.cn/xxgs/hmd/index']

    # 行业自律 数据抓取
    for index, each_url in enumerate(prefix_url):
        # get page count
        response = request_site_page(each_url + '.shtml')
        if response is None:
            logger.error('网页请求错误 %s' % (each_url + '.shtml'))
            continue
        soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
        page_count_text = soup.find(class_='noBorder').text if soup.find(class_='noBorder') else ''
        page_count = int(re.findall(r'\d+', page_count_text)[-1])
        logger.info('中国基金业协会 行业自律' + ' 一共有%d页' % page_count)

        # get crawler data
        for num in range(page_count):
            logger.info('第%d页' % (num + 1))
            if num == 0:
                url = each_url + '.shtml'
            else:
                url = each_url + '_' + str(num) + '.shtml'
            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')

                # class = newsList1
                for each_list in content_soup.find_all(class_='newsList1'):
                    try:
                        announcement_url = urljoin(url, each_list.find('a').attrs['href'].strip())
                        if db.amac_data.find({'url': announcement_url}).count() == 0:
                            title = each_list.find('a').text.strip()
                            publish_date = each_list.find(class_='newsDate').text.strip()
                            logger.info('中国基金业协会 行业自律 新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': announcement_url,
                                'type': '纪律处分',
                                'origin': '基金业协会',
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

                # class = newsList2
                for each_list in content_soup.find_all(class_='newsList2'):
                    try:
                        announcement_url = urljoin(url, each_list.find('a').attrs['href'].strip())
                        if db.amac_data.find({'url': announcement_url}).count() == 0:
                            title = each_list.find('a').text.strip()
                            publish_date = each_list.find(class_='newsDate').text.strip()
                            logger.info('中国基金业协会 行业自律 新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': announcement_url,
                                'type': '纪律处分',
                                'origin': '基金业协会',
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

    # 不予登记机构 数据抓取
    for index, each_url in enumerate(prefix_url0):
        # get page count
        response = request_site_page(each_url + '.shtml')
        if response is None:
            logger.error('网页请求错误 %s' % (each_url + '.shtml'))
            continue
        soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
        page_count_text = soup.find(class_='noBorder').text if soup.find(class_='noBorder') else ''
        page_count = int(re.findall(r'\d+', page_count_text)[-1])
        logger.info('中国基金业协会 不予登记机构' + ' 一共有%d页' % page_count)

        # get crawler data
        for num in range(page_count):
            logger.info('第%d页' % (num + 1))
            if num == 0:
                url = each_url + '.shtml'
            else:
                url = each_url + '_' + str(num) + '.shtml'

            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')

                # class = newsList1
                for each_list in content_soup.find_all(class_='newsList1'):
                    try:
                        announcement_url = urljoin(url, each_list.find('a').attrs['href'].strip())
                        if db.amac_data.find({'url': announcement_url}).count() == 0:
                            title = each_list.find('a').text.strip()
                            publish_date = each_list.find(class_='newsDate').text.strip()
                            logger.info('中国基金业协会 不予登记机构 新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': announcement_url,
                                'type': '不予登记',
                                'origin': '基金业协会',
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

                # class = newsList2
                for each_list in content_soup.find_all(class_='newsList2'):
                    try:
                        announcement_url = urljoin(url, each_list.find('a').attrs['href'].strip())
                        if db.amac_data.find({'url': announcement_url}).count() == 0:
                            title = each_list.find('a').text.strip()
                            publish_date = each_list.find(class_='newsDate').text.strip()
                            logger.info('中国基金业协会 不予登记机构 新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': announcement_url,
                                'type': '不予登记',
                                'origin': '基金业协会',
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

    # 黑名单 数据抓取
    for index, each_url in enumerate(prefix_url1):
        # get page count
        response = request_site_page(each_url + '.shtml')
        if response is None:
            logger.error('网页请求错误 %s' % (each_url + '.shtml'))
            continue
        soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
        page_count_text = soup.find(class_='noBorder').text if soup.find(class_='noBorder') else ''
        page_count = int(re.findall(r'\d+', page_count_text)[-1])
        logger.info('中国基金业协会 黑名单' + ' 一共有%d页' % page_count)

        # get crawler data
        for num in range(page_count):
            logger.info('第%d页' % (num + 1))
            if num == 0:
                url = each_url + '.shtml'
            else:
                url = each_url + '_' + str(num) + '.shtml'

            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')

                # class = newsList1
                for each_list in content_soup.find_all(class_='newsList1'):
                    try:
                        announcement_url = urljoin(url, each_list.find('a').attrs['href'].strip())
                        if db.amac_data.find({'url': announcement_url}).count() == 0:
                            title = each_list.find('a').text.strip()
                            publish_date = each_list.find(class_='newsDate').text.strip()
                            logger.info('中国基金业协会 不予登记机构 新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': announcement_url,
                                'type': '黑名单',
                                'origin': '基金业协会',
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

                # class = newsList2
                for each_list in content_soup.find_all(class_='newsList2'):
                    try:
                        announcement_url = urljoin(url, each_list.find('a').attrs['href'].strip())
                        if db.amac_data.find({'url': announcement_url}).count() == 0:
                            title = each_list.find('a').text.strip()
                            publish_date = each_list.find(class_='newsDate').text.strip()
                            logger.info('中国基金业协会 不予登记机构 新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': announcement_url,
                                'type': '黑名单',
                                'origin': '基金业协会',
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
        logger.info('中国基金业协会一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.amac_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('中国基金业协会公告导入完成！')
        else:
            logger.error('中国基金业协会公告导入出现问题！')
    else:
        logger.info('中国基金业协会没有新公告！')


if __name__ == "__main__":
    amac_crawler()
