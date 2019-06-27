import re

from pymongo import MongoClient
from init import logger_init, config_init
from bs4 import BeautifulSoup as bs
from utility import request_site_page
from urllib.parse import urljoin

logger = logger_init('中国证监会-数据抓取')
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

# 抓取数据存入csrc_data这个collection
db.csrc_data.create_index([('url', 1)])


# 证监会
def csrc_crawler():
    # 行政处罚决定 + 市场禁入决定
    url_list = [
        {

            'page_url': 'http://www.csrc.gov.cn/pub/zjhpublic/index.htm?channel=3300/3313',
            'request_url': 'http://www.csrc.gov.cn/pub/zjhpublic/3300/3313/index_7401'
        },
        {
            'page_url': 'http://www.csrc.gov.cn/pub/zjhpublic/index.htm?channel=3300/3619',
            'request_url': 'http://www.csrc.gov.cn/pub/zjhpublic/3300/3619/index_7401'
        },
    ]
    # 责令整改通知
    url2_list = [
        'http://www.csrc.gov.cn/pub/newsite/xzcfw/zlzgtz/index',
    ]
    # 要闻
    url3_list = [
        'http://www.csrc.gov.cn/pub/newsite/zjhxwfb/xwdd/index'
    ]

    new_csrc_announcement_list = []

    for index, each_url_info in enumerate(url_list):
        logger.info('行政处罚决定' if index == 0 else '市场禁入决定')

        # get page_count
        page_count_url = each_url_info['request_url'] + '.htm'
        response = request_site_page(page_count_url)
        if response is None:
            logger.error('网页请求错误 %s' % page_count_url)
            continue
        page_count = int(int(re.search(r'var m_nRecordCount = "(\d+)"?;', response.text).group(1).strip()) / 20 + 1)
        logger.info(('行政处罚决定' if index == 0 else '市场禁入决定') + ' --  一共有%d页' % page_count)

        # get crawler data
        for i in range(page_count):
            logger.info(('行政处罚决定' if index == 0 else '市场禁入决定') + ' -- 第%d页' % (i + 1))
            url = each_url_info['request_url'] + '_' + str(i) + '.htm' if i > 0 \
                else each_url_info['request_url'] + '.htm'

            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
                dl_content = content_soup.find(id='documentContainer')
                if not dl_content:
                    logger.error('网页请求错误 %s' % url)
                    continue
                for each_dd in dl_content.find_all(class_='row'):
                    try:
                        if len(each_dd.find_all('a')) > 0:
                            announcement_url = urljoin(url, each_dd.find('a').attrs['href'])
                            if db.csrc_data.find({'url': announcement_url}).count() == 0:
                                title = each_dd.find('a').text.strip()
                                announcement_date = each_dd.find(class_='fbrq').text.strip()
                                logger.info('证监会' + ('行政处罚决定' if index == 0 else '市场禁入决定') + '新公告：' + announcement_url)
                                post = {
                                    'title': title,
                                    'publishDate': announcement_date,
                                    'url': announcement_url,
                                    'type': '行政处罚决定' if index == 0 else '市场禁入决定',
                                    'origin': '证监会',
                                    'status': 'not parsed'
                                }
                                if post not in new_csrc_announcement_list:
                                    new_csrc_announcement_list.append(post)
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

    for each_url in url2_list:
        logger.info('责令整改通知')

        # get page_count
        page_count_url = each_url + '.htm'
        response = request_site_page(page_count_url)
        if response is None:
            logger.error('网页请求错误 %s' % page_count_url)
            continue
        soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
        page_count_text = soup.find(class_='page').text if soup.find(class_='page') else ''
        page_count = int(re.findall(r'\d+', page_count_text)[0]) if page_count_text != '' else 0
        logger.info('责令整改通知 -- 一共有%d页' % page_count)

        # get crawler data
        for i in range(page_count):
            logger.info('责令整改通知 -- 第%d页' % (i + 1))
            url = each_url + '_' + str(i) + '.htm' if i > 0 else each_url + '.htm'

            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
                dl_content = content_soup.find(id='myul')
                if not dl_content:
                    logger.error('网页请求错误 %s' % url)
                    continue
                for each_dd in dl_content.find_all('li'):
                    try:
                        if len(each_dd.find_all('a')) > 0:
                            announcement_url = urljoin(url, each_dd.find('a').attrs['href'])
                            if db.csrc_data.find({'url': announcement_url}).count() == 0:
                                title = each_dd.find('a').attrs['title'].strip()
                                announcement_date = each_dd.find('span').text.strip()
                                logger.info('证监会责令整改通知新公告：' + announcement_url)
                                post = {
                                    'title': title,
                                    'publishDate': announcement_date,
                                    'url': announcement_url,
                                    'type': '责令整改通知',
                                    'origin': '证监会',
                                    'status': 'not parsed'
                                }
                                if post not in new_csrc_announcement_list:
                                    new_csrc_announcement_list.append(post)
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

    for each_url in url3_list:
        logger.info('要闻')

        # get page_count
        page_count_url = each_url + '.html'
        response = request_site_page(page_count_url)
        if response is None:
            logger.error('网页请求错误 %s' % page_count_url)
            continue
        soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
        page_count = int(re.search(r'var countPage = (\d+)', soup.text).group(1).strip())
        logger.info('要闻 -- 一共有%d页' % page_count)

        # get crawler data
        for i in range(page_count):
            logger.info('要闻 -- 第%d页' % (i + 1))
            url = each_url + '_' + str(i) + '.html' if i > 0 else each_url + '.html'

            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
                dl_content = content_soup.find(id='myul')
                if not dl_content:
                    logger.error('网页请求错误 %s' % url)
                    continue
                for each_dd in dl_content.find_all('li'):
                    try:
                        if len(each_dd.find_all('a')) > 0:
                            title = each_dd.find('a').attrs['title'].strip()
                            if re.search(r'^证监会.*?作出行政处罚(决定)?$', title) or '现场检查情况' in title:
                                announcement_url = urljoin(url, each_dd.find('a').attrs['href'])
                                if db.csrc_data.find({'url': announcement_url}).count() == 0:
                                    announcement_date = each_dd.find('span').text.strip()
                                    logger.info('证监会要闻新公告：' + announcement_url)
                                    post = {
                                        'title': title,
                                        'publishDate': announcement_date,
                                        'url': announcement_url,
                                        'type': '要闻',
                                        'origin': '证监会',
                                        'status': 'not parsed'
                                    }
                                    if post not in new_csrc_announcement_list:
                                        new_csrc_announcement_list.append(post)
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

    if len(new_csrc_announcement_list) > 0:
        logger.info('证监会一共有%d条新公告，导入数据库中......' % len(new_csrc_announcement_list))
        r = db.csrc_data.insert_many(new_csrc_announcement_list)
        if len(r.inserted_ids) == len(new_csrc_announcement_list):
            logger.info('证监会公告导入完成！')
        else:
            logger.error('证监会公告导入出现问题！')
    else:
        logger.info('证监会没有新公告！')


if __name__ == "__main__":
    csrc_crawler()
