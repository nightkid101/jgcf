from pymongo import MongoClient
from utility import request_site_page
import json
from init import logger_init, config_init

logger = logger_init('全国律师协会-数据抓取')
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


def acla_crawler():
    result_list = []  # 用来保存最后存入数据库的数据
    prefix_url = [{'url': 'http://www.acla.org.cn/article/page/articleList/165/10/', 'origin': '全国律师协会'}]
    for each_url_info in prefix_url:
        each_url = each_url_info['url']
        stop_flag = False
        logger.info('全国律师协会 抓取URL：' + each_url + '1')
        # get page count
        response = request_site_page(each_url + '1', methods='post')
        if response is None:
            logger.error('网页请求错误 %s' % (each_url + '1'))
            continue
        result = json.loads(response.text)
        page_count = result['pageCount']
        logger.info('全国律师协会' + ' 一共有%d页' % page_count)

        if db.crawler.find({'url': each_url + '1'}).count() > 0:
            last_updated_url = db.crawler.find_one({'url': each_url + '1'})['last_updated']
        else:
            last_updated_url = ''

        # get crawler data
        for page_num in range(page_count):
            logger.info('全国律师协会 -- 第%d页' % (page_num + 1))
            page_url = each_url + str(page_num + 1)

            try:
                page_response = request_site_page(page_url, methods='post')
                if page_response is None:
                    logger.error('网页请求错误 %s' % page_url)
                    continue
                page_result = json.loads(page_response.text)
                for index, each_result in enumerate(page_result['data']):
                    true_url = 'http://www.acla.org.cn/article/page/detailById/' + str(each_result['articleId'])
                    # 判断是否为之前抓取过的
                    if true_url == last_updated_url:
                        stop_flag = True
                        logger.info('到达上次爬取的链接')
                        break

                    # 更新抓取的分割线
                    if page_num == 0 and index == 0:
                        if db.crawler.find({'url': each_url + '1'}).count() > 0:
                            if db.crawler.find_one({'url': each_url + '1'})['last_updated'] != true_url:
                                db.crawler.update_one({'url': each_url + '1'}, {'$set': {'last_updated': true_url}})
                        else:
                            db.crawler.insert_one(
                                {'url': each_url + '1', 'last_updated': true_url, 'origin': each_url_info['origin']})

                    title = each_result['headline'].strip()
                    if '决定书' in title:
                        publish_date = each_result['publishTimeStr'].strip()
                        if db.lawyers_data.find({'url': true_url}).count() == 0:
                            logger.info('全国律师协会新公告：' + true_url + ' title:' + title)
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': true_url,
                                'type': '',
                                'origin': '全国律师协会',
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
        logger.info('全国律师协会一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.lawyers_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('全国律师协会公告导入完成！')
        else:
            logger.error('全国律师协会公告导入出现问题！')
    else:
        logger.info('全国律师协会没有新公告！')


if __name__ == "__main__":
    acla_crawler()
