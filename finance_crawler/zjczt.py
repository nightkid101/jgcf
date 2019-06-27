from pymongo import MongoClient
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from utility import request_site_page
from init import logger_init, config_init

logger = logger_init('浙江省财政厅-数据抓取')
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


def zjczt_crawler():
    result_list = []  # 用来保存最后存入数据库的数据
    prefix_url = [
        {'url': 'http://www.zjczt.gov.cn/col/col1164164/index.html?uid=3715896', 'origin': '浙江省财政厅'}
    ]
    for each_url_info in prefix_url:
        each_url = each_url_info['url']
        stop_flag = False
        logger.info('浙江省财政厅 抓取URL：' + each_url)
        # get page count
        data = {
            'col': '1',
            'appid': '1',
            'webid': '1791',
            'path': '/',
            'columnid': '1164164',
            'sourceContentType': '1',
            'unitid': '3715896',
            'webname': '浙江省财政厅',
            'permissiontype': '0'
        }
        base_response = request_site_page(
            'http://www.zjczt.gov.cn/module/jpage/dataproxy.jsp?startrecord=1&endrecord=15&perpage=15',
            data=data, methods='post')
        if base_response is None:
            logger.error('网页请求错误 %s' % each_url)
            continue
        tree = BeautifulSoup(base_response.text, 'xml')
        page_count = int(tree.datastore.totalpage.text)
        logger.info('一共有%d页' % page_count)

        if db.crawler.find({'url': each_url}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': each_url})['last_updated']
        else:
            last_updated_url = ''

        for page_num in range(page_count):
            logger.info('第' + str(page_num + 1) + '页')
            url = 'http://www.zjczt.gov.cn/module/jpage/dataproxy.jsp?perpage=15' + \
                  '&startrecord=' + str(page_num * 15 + 1) + '&endrecord=' + str(page_num * 15 + 15)

            try:
                page_response = request_site_page(url, data=data, methods='post')
                if page_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                page_tree = BeautifulSoup(page_response.text, 'xml')
                all_record = page_tree.datastore.recordset.findAll("record")
                for index, each_record in enumerate(all_record):
                    href = re.search(r'<a href="(.*?)" target="_blank">(.*?)</a></th>', each_record.text) \
                        .group(1).strip()
                    true_url = urljoin(each_url, href)
                    title = re.search(r'<a href="(.*?)" target="_blank">(.*?)</a></th>', each_record.text) \
                        .group(2).strip()

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
                    if re.search('.*?行政处罚决定书.*?', title):
                        if db.finance_data.find({'url': true_url}).count() == 0:
                            publish_date = re.search(
                                r'<td  width="10%"  style=";border-bottom: #CCC 1px dotted;line-height:24px;">(.*?)'
                                r'</th>  </tr></table>$',
                                each_record.text).group(1).strip()
                            logger.info('浙江省财政厅新公告：' + true_url + ' title: ' + title)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': true_url,
                                'type': '行政处罚决定',
                                'origin': '浙江省财政厅',
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
        logger.info('浙江省财政厅一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.finance_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('浙江省财政厅公告导入完成！')
        else:
            logger.error('浙江省财政厅公告导入出现问题！')
    else:
        logger.info('浙江省财政厅没有新公告！')


if __name__ == "__main__":
    zjczt_crawler()
