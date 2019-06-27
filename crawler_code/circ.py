import re

from pymongo import MongoClient
from bs4 import BeautifulSoup as bs
from init import logger_init, config_init
from utility import request_site_page
from urllib.parse import urljoin

logger = logger_init('中国保监会-数据抓取')
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

# 抓取数据存入circ_data这个collection
db.circ_data.create_index([('url', 1)])


# 保监会行政处罚决定
def circ_bjhcf_crawler():
    result_list = []
    prefix_url = 'http://bxjg.circ.gov.cn/web/site0/tab5240/module14430/page'

    # get page count
    response = request_site_page(prefix_url + '1.htm')
    if response is None:
        logger.error('网页请求错误 %s' % (prefix_url + '1.htm'))
        return
    soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
    page_count_text = soup.find(class_='Normal').text if soup.find(class_='Normal') else ''
    page_count = int(re.findall(r'\d+', page_count_text)[-1]) if page_count_text != '' else 0
    logger.info('保监会 -- 行政处罚决定' + ' 一共有%d页' % page_count)

    # get crawler data
    for i in range(page_count):
        logger.info('保监会 -- 行政处罚决定' + ' -- 第%d页' % (i + 1))
        url = prefix_url + str(i + 1) + '.htm'

        try:
            content_response = request_site_page(url)
            if content_response is None:
                logger.error('网页请求错误 %s' % url)
                continue
            content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
            table_content = content_soup.find(id='ess_ctr14430_ListC_Info_LstC_Info')

            if not table_content:
                logger.error('网页请求错误')
                continue

            for each_table in table_content.find_all('table'):
                try:
                    if each_table.attrs.get('id', '') != 'ess_ctr14430_ListC_Info_LstC_Info':
                        announcement_url = urljoin(url, each_table.find('a').attrs['href'].strip())
                        if db.circ_data.find({'url': announcement_url}).count() == 0:
                            title = each_table.find('a').attrs['title'].strip()
                            publish_date = each_table.find_all('td')[-1].text.strip().replace('(', '20').replace(')', '')
                            logger.info('保监会 -- 行政处罚决定 -- 新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': announcement_url,
                                'type': '行政处罚决定',
                                'origin': '保监会',
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
        logger.info('保监会 -- 行政处罚决定 -- 一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.circ_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('保监会 -- 行政处罚决定 -- 公告导入完成！')
        else:
            logger.error('保监会 -- 行政处罚决定 -- 公告导入出现问题！')
    else:
        logger.info('保监会 -- 行政处罚决定 -- 没有新公告！')


# 地方保监局行政处罚决定
def circ_bjjcf_crawler():
    result_list = []
    prefix_url = 'http://bxjg.circ.gov.cn/web/site0/tab5241/module14458/page'
    # get page count
    response = request_site_page(prefix_url + '1.htm')
    if response is None:
        logger.error('网页请求错误 %s' % (prefix_url + '1.htm'))
        return
    soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
    page_count_text = soup.find(class_='Normal').text if soup.find(class_='Normal') else ''
    page_count = int(re.findall(r'\d+', page_count_text)[-1]) if page_count_text != '' else 0
    logger.info('地方保监局 -- 行政处罚决定' + ' 一共有%d页' % page_count)

    # get crawler data
    for i in range(page_count):
        logger.info('地方保监局 -- 行政处罚决定' + ' -- 第%d页' % (i + 1))
        url = prefix_url + str(i + 1) + '.htm'

        try:
            content_response = request_site_page(url)
            if content_response is None:
                logger.error('网页请求错误 %s' % url)
                continue
            content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
            table_content = content_soup.find(id='ess_ctr14458_ListC_Info_LstC_Info')

            if not table_content:
                logger.error('网页请求错误')
                continue

            for each_table in table_content.find_all('table'):
                try:
                    if each_table.attrs.get('id', '') != 'ess_ctr14458_ListC_Info_LstC_Info':
                        publish_date = each_table.find_all('td')[-1].text.strip().replace('(', '20').replace(')', '')
                        announcement_url = urljoin(url, each_table.find('a').attrs['href'].strip())
                        if db.circ_data.find(
                                {'url': {'$regex': '.*' + announcement_url.split('/')[-1] + '$'}}).count() == 0:
                            title = each_table.find('a').attrs['title'].strip()
                            logger.info('地方保监局 -- 行政处罚决定 -- 新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': announcement_url,
                                'type': '行政处罚决定',
                                'origin': '保监局',
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
        logger.info('地方保监局 -- 行政处罚决定 -- 一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.circ_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('地方保监局 -- 行政处罚决定 -- 公告导入完成！')
        else:
            logger.error('保监局 -- 行政处罚决定 -- 公告导入出现问题！')
    else:
        logger.info('地方保监局 -- 行政处罚决定 -- 没有新公告！')


# 保监会监管函
def circ_jgh_crawler():
    result_list = []
    prefix_url = 'http://bxjg.circ.gov.cn/web/site0/tab7324/module25157/page'

    # get page count
    response = request_site_page(prefix_url + '1.htm')
    if response is None:
        logger.error('网页请求错误 %s' % (prefix_url + '1.htm'))
        return
    soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
    page_count_text = soup.find_all(class_='Normal')[-1].text if soup.find(class_='Normal') else ''
    page_count = int(re.findall(r'\d+', page_count_text)[-1]) if page_count_text != '' else 0
    logger.info('保监会 -- 监管函' + ' 一共有%d页' % page_count)

    # get crawler data
    for i in range(page_count):
        logger.info('保监会 -- 监管函' + ' -- 第%d页' % (i + 1))
        url = prefix_url + str(i + 1) + '.htm'

        try:
            content_response = request_site_page(url)
            if content_response is None:
                logger.error('网页请求错误 %s' % url)
                continue
            content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
            table_content = content_soup.find(id='ess_ctr25157_ListC_Info_LstC_Info')

            if not table_content:
                logger.error('网页请求错误')
                continue

            for each_table in table_content.find_all('table'):
                try:
                    if each_table.attrs.get('id', '') != 'ess_ctr25157_ListC_Info_LstC_Info':
                        announcement_url = urljoin(url, each_table.find('a').attrs['href'].strip())
                        if db.circ_data.find({'url': announcement_url}).count() == 0:
                            title = each_table.find('a').attrs['title'].strip()
                            publish_date = each_table.find_all('td')[-1].text.strip().replace('(', '20').replace(')', '')
                            logger.info('保监会 -- 监管函 -- 新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': announcement_url,
                                'type': '监管措施',
                                'origin': '保监会',
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
        logger.info('保监会 -- 监管函 -- 一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.circ_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('保监会 -- 监管函 -- 公告导入完成！')
        else:
            logger.error('保监会 -- 监管函 -- 公告导入出现问题！')
    else:
        logger.info('保监局 -- 监管函 -- 没有新公告！')


if __name__ == "__main__":
    circ_bjhcf_crawler()
    circ_bjjcf_crawler()
    circ_jgh_crawler()
