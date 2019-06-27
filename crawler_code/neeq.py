from datetime import datetime
import re
import json

from pymongo import MongoClient
from utility import request_site_page
from init import logger_init, config_init

logger = logger_init('全国中小企业股份转让系统-数据抓取')
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

# 抓取数据存入neeq_data这个collection
db.neeq_data.create_index([('url', 1)])


# 全国中小企股份转让系统公司 （按时间获取）
# 自律监管措施
def neeq_crawler_one(start_time, end_time):
    result_list = []
    # get page count
    r = request_site_page('http://www.neeq.com.cn/disclosureInfoController/infoResult.do',
                          params={'disclosureType': 8, 'page': 0, 'startTime': start_time, 'endTime': end_time},
                          methods='post')
    if r is None:
        logger.error('网页请求错误')
        return
    result_text = re.sub(r'null\(', '', r.text)
    result_json = json.loads(''.join(list(result_text)[:-1]))
    page_count = result_json[0]['listInfo']['totalPages']
    logger.info('股转系统 自律监管措施一共有%d页' % page_count)
    # get crawler data
    for num in range(page_count):
        try:
            logger.info('第%d页' % (num + 1))
            r = request_site_page('http://www.neeq.com.cn/disclosureInfoController/infoResult.do',
                                  params={
                                      'disclosureType': 8, 'page': num, 'startTime': start_time, 'endTime': end_time
                                  },
                                  methods='post')
            if r is None:
                logger.error('网页请求错误')
                continue
            result_text = re.sub(r'null\(', '', r.text)
            result_json = json.loads(''.join(list(result_text)[:-1]))
            for each_announcement in result_json[0]['listInfo']['content']:
                announcement_url = 'http://www.neeq.com.cn' + each_announcement['destFilePath']
                if db.neeq_data.find({'url': announcement_url}).count() == 0:
                    logger.info('股转系统 自律监管措施新公告：' + announcement_url)
                    result_list.append({
                        'title': each_announcement['disclosureTitle'],
                        'publishDate': each_announcement['publishDate'],
                        'url': announcement_url,
                        'type': '监管措施',
                        'origin': '股转系统',
                        'status': 'not parsed'
                    })
                else:
                    if config['crawler_update_type']['update_type'] == '0':
                        break
        except Exception as e:
            logger.error(e)
            continue
    return result_list


# 全国中小企股份转让系统公司 （按时间获取）
# 纪律处分
def neeq_crawler_two(start_time, end_time):
    result_list = []
    # get page count
    r = request_site_page('http://www.neeq.com.cn/PunishmentController/infoResult.do',
                          params={'disclosureType': 8, 'page': 0, 'startTime': start_time, 'endTime': end_time},
                          methods='post')
    if r is None:
        logger.error('网页请求错误')
        return
    result_text = re.sub(r'null\(', '', r.text)
    result_json = json.loads(''.join(list(result_text)[:-1]))
    page_count = result_json[0]['pageList']['totalPages']
    logger.info('股转系统 纪律处分一共有%d页' % page_count)
    # get crawler data
    for num in range(page_count):
        try:
            logger.info('第%d页' % (num + 1))
            r = request_site_page('http://www.neeq.com.cn/PunishmentController/infoResult.do',
                                  params={'disclosureType': 8, 'page': num, 'startTime': start_time,
                                          'endTime': end_time},
                                  methods='post')
            if r is None:
                logger.error('网页请求错误')
                continue
            result_text = re.sub(r'null\(', '', r.text)
            result_json = json.loads(''.join(list(result_text)[:-1]))
            for each_announcement in result_json[0]['pageList']['content']:
                announcement_url = each_announcement['destFilePath'] if 'http://www.neeq.com.cn' in each_announcement[
                    'destFilePath'] else 'http://www.neeq.com.cn' + each_announcement['destFilePath']
                if db.neeq_data.find({'url': announcement_url}).count() == 0:
                    logger.info('股转系统 纪律处分新公告：' + announcement_url)
                    result_list.append({
                        'title': each_announcement['announcementTitle'],
                        'publishDate': str(each_announcement['announcementDate']['year'] + 1900) + '-' + str(
                            each_announcement['announcementDate']['month'] + 1) + '-' + str(
                            each_announcement['announcementDate']['day']),
                        'url': announcement_url,
                        'type': '纪律处分',
                        'origin': '股转系统',
                        'status': 'not parsed'
                    })
                else:
                    if config['crawler_update_type']['update_type'] == '0':
                        break
        except Exception as e:
            logger.error(e)
            continue
    return result_list


def neeq_crawler():
    # 最多抓取一年
    # 2016-05-06 ~ 2017-05-06
    from_date = '2016-05-06'
    today_date = datetime.now().strftime("%Y-%m-%d")
    while from_date < today_date:
        end_date = '-'.join([str(int(from_date.split('-')[0]) + 1), from_date.split('-')[1], from_date.split('-')[2]])
        logger.info('开始时间：%s 结束时间：%s' % (from_date, end_date))
        each_result_list_one = neeq_crawler_one(from_date, end_date)
        each_result_list_two = neeq_crawler_two(from_date, end_date)

        if len(each_result_list_one) > 0:
            logger.info('自律监管措施一共有%d条新公告，导入数据库中......' % len(each_result_list_one))
            r = db.neeq_data.insert_many(each_result_list_one)
            if len(r.inserted_ids) == len(each_result_list_one):
                logger.info('自律监管措施公告导入完成！')
            else:
                logger.error('自律监管措施公告导入出现问题！')
        else:
            logger.info('自律监管措施没有新公告！')

        if len(each_result_list_two) > 0:
            logger.info('纪律处分一共有%d条新公告，导入数据库中......' % len(each_result_list_two))
            r = db.neeq_data.insert_many(each_result_list_two)
            if len(r.inserted_ids) == len(each_result_list_two):
                logger.info('纪律处分公告导入完成！')
            else:
                logger.error('纪律处分公告导入出现问题！')
        else:
            logger.info('纪律处分没有新公告！')

        from_date = end_date


if __name__ == "__main__":
    neeq_crawler()
