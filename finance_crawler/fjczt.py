from pymongo import MongoClient
import re
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from utility import request_site_page
from init import logger_init, config_init

logger = logger_init('福建省财政厅-数据抓取')
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


def fjczt_crawler():
    result_list = []  # 用来保存最后存入数据库的数据
    prefix_url = [
        {
            'url': 'http://czt.fujian.gov.cn/ztzl/xzxkxzcfgs/gsqsxzcfjd/', 'origin': '福建省财政厅',
            'request_url': 'http://czt.fujian.gov.cn/was5/web/search?channelid=229105&templet=docs.jsp'
                           '&sortfield=-docorderpri%2C-docorder&classsql=chnlid%3D21090&prepage=15',
            'type': '各设区市行政处罚决定'
        },
        {
            'url': 'http://czt.fujian.gov.cn/ztzl/xzxkxzcfgs/sjxzcfjd/', 'origin': '福建省财政厅',
            'request_url': 'http://czt.fujian.gov.cn/was5/web/search?channelid=229105&templet=docs.jsp'
                           '&sortfield=-docorderpri%2C-docorder&classsql=chnlid%3D21088&prepage=15',
            'type': '省级行政处罚决定'
        },
        {
            'url': 'http://czt.fujian.gov.cn/zfxxgkzl/zfxxgkml/xzzf/xzcf/', 'origin': '福建省财政厅',
            'request_url': 'http://czt.fujian.gov.cn/was5/web/search?channelid=229105'
                           '&templet=docs.jsp&sortfield=-docreltime'
                           '&classsql=((chnlid%3E20991*chnlid%3C21060)%5E(chnlid%3E33266*chnlid%3C33272)%5E(chnlid%3E34639*chnlid%3C34642))*(chnlid%2Cparentid%2B%3D21054)'
                           '&prepage=15',
            'type': '政府信息公开目录 行政执法 行政处罚'
        }
    ]
    for each_url_info in prefix_url:
        each_url = each_url_info['url']
        stop_flag = False
        logger.info('福建省财政厅 ' + each_url_info['type'] + ' 抓取URL：' + each_url)
        # get page count
        base_page = request_site_page(each_url_info['request_url'])
        if base_page is None:
            logger.error('网页请求错误 %s' % each_url)
            continue
        base_page.encoding = base_page.apparent_encoding
        try:
            base_json_text = re.sub(r'\n+', r'\n', base_page.text.replace('\r\n', '\n'))
            base_json_text = re.sub(r',\n"content":[\s\S]*?\n"chnl":', r',\n"chnl":', base_json_text)
            base_json = json.loads(base_json_text)
            page_count = int(base_json['pagenum'])
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
            url = each_url_info['request_url'] + '&page=' + str(page_num + 1)

            try:
                page_response = request_site_page(url)
                if page_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                page_response.encoding = page_response.apparent_encoding
                page_json_text = re.sub(r'\n+', r'\n', page_response.text.replace('\r\n', '\n'))
                page_json_text = re.sub(r',\n"content":[\s\S]*?\n"chnl":', r',\n"chnl":', page_json_text)
                page_json = json.loads(page_json_text)
                for index, each_result in enumerate(page_json['docs']):
                    title = each_result['title'].strip()
                    if title == '文章标题':
                        continue
                    true_url = each_result['url'].strip()

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

                    publish_date = each_result['time']
                    if db.finance_data.find({'url': true_url}).count() == 0:
                        logger.info('福建省财政厅新公告：' + true_url + ' title: ' + title)
                        post = {
                            'title': title,
                            'publishDate': publish_date,
                            'url': true_url,
                            'type': '行政处罚决定',
                            'origin': '福建省财政厅',
                            'status': 'not parsed',
                            'code': each_result['fileno'].strip()
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
        logger.info('福建省财政厅一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.finance_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('福建省财政厅公告导入完成！')
        else:
            logger.error('福建省财政厅公告导入出现问题！')
    else:
        logger.info('福建省财政厅没有新公告！')


if __name__ == "__main__":
    fjczt_crawler()
