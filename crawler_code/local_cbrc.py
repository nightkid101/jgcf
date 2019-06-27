import re

from pypinyin import lazy_pinyin
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin
from pymongo import MongoClient
from init import logger_init, config_init
from utility import request_site_page

logger = logger_init('地方银监局-数据抓取')
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

# 抓取数据存入cbrc_data这个collection
db.cbrc_data.create_index([('url', 1)])


# 银监局
def local_cbrc(city_info):
    city = ''.join(lazy_pinyin(city_info)) if city_info != '陕西' else 'shaanxi'
    result_list = []

    # get page_count
    prefix_url = 'http://www.cbrc.gov.cn/zhuanti/xzcf/getPcjgXZCFDocListDividePage/' + city + '.html?current='
    response = request_site_page(prefix_url)
    if response is None:
        logger.error('网页请求错误 %s' % prefix_url)
        return
    soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
    page_count_text = soup.find(class_='work_page').text if soup.find(class_='work_page') else ''
    page_count = int(re.findall(r'\d+', page_count_text)[-1]) if page_count_text != '' else 0
    logger.info(city_info + '银监局 -- 一共有%d页' % page_count)

    # get all data
    for num in range(page_count):
        logger.info(city_info + '银监局 -- 第%d页' % (num + 1))
        url = prefix_url + str(num + 1)

        try:
            content_response = request_site_page(url)
            if content_response is None:
                logger.error('网页请求错误 %s' % url)
                continue
            content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')

            # class==work_list
            for each_list in content_soup.find_all(class_='work_list'):
                try:
                    each_link = each_list.find('a')
                    publish_date = each_list.find(class_='work_list_date').text.strip()
                    title = each_link.attrs['title'].strip()
                    announcement_url = urljoin(url, each_link.attrs['href'])

                    if db.cbrc_data.find({'url': announcement_url}).count() == 0:
                        post = {
                            'title': title,
                            'publishDate': publish_date,
                            'url': announcement_url,
                            'type': '行政处罚决定',
                            'origin': city_info + '银监局',
                            'status': 'not parsed'
                        }
                        if db.cbrc_data.find(
                                {'url': {'$regex': '.*' + announcement_url.split('/')[-2] + '.*'}}).count() == 1:
                            if db.cbrc_data.find(
                                    {'url': announcement_url, 'title': title, 'publishDate': publish_date}) \
                                    .count() == 0:
                                logger.info(city_info + '银监局公告更新数据库：' + announcement_url)
                                db.cbrc_data.update_one(
                                    {'url': {'$regex': '.*' + announcement_url.split('/')[-2] + '.*'}}, {'$set': post})
                        else:
                            logger.info(city_info + '银监局新公告：' + announcement_url)
                            if post not in result_list:
                                result_list.append(post)
                    else:
                        if config['crawler_update_type']['update_type'] == '0':
                            break
                except Exception as e:
                    logger.error(e)
                    logger.warning('提取公告url出现问题')
                    continue

            # class==work_list02
            for each_list in content_soup.find_all(class_='work_list02'):
                try:
                    each_link = each_list.find('a')
                    publish_date = each_list.find(class_='work_list_date').text.strip()
                    title = each_link.attrs['title'].strip()
                    announcement_url = urljoin(url, each_link.attrs['href'])

                    if db.cbrc_data.find({'url': announcement_url}).count() == 0:
                        post = {
                            'title': title,
                            'publishDate': publish_date,
                            'url': announcement_url,
                            'type': '行政处罚决定',
                            'origin': city_info + '银监局',
                            'status': 'not parsed'
                        }
                        if db.cbrc_data.find({'url': {'$regex': '.*' + announcement_url.split('/')[-2] + '.*'}
                                              }).count() == 1:
                            if db.cbrc_data.find(
                                    {'url': announcement_url, 'title': title, 'publishDate': publish_date}) \
                                    .count() == 0:
                                logger.info(city_info + '银监局公告更新数据库：' + announcement_url)
                                db.cbrc_data.update_one(
                                    {'url': {'$regex': '.*' + announcement_url.split('/')[-2] + '.*'}}, {'$set': post})
                        else:
                            logger.info(city_info + '银监局新公告：' + announcement_url)
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
        logger.info(city_info + '银监局一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.cbrc_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info(city_info + '银监局公告导入完成！')
        else:
            logger.error(city_info + '银监局公告导入出现问题！')
    else:
        logger.info(city_info + '银监局没有新公告！')
    logger.info('\n')


if __name__ == '__main__':
    city_list = [
        '北京', '天津', '河北', '山西',
        '内蒙古', '辽宁', '吉林', '黑龙江',
        '上海', '江苏', '浙江', '安徽',
        '福建', '江西', '山东', '河南',
        '湖北', '湖南', '广东', '广西',
        '海南', '重庆', '四川', '贵州',
        '云南', '西藏', '甘肃', '陕西',
        '青海', '宁夏', '新疆', '深圳',
        '大连', '宁波', '厦门', '青岛'
    ]
    for each_city in city_list:
        local_cbrc(each_city)
