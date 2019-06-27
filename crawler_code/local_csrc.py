import re

from pymongo import MongoClient
from init import logger_init, config_init
from bs4 import BeautifulSoup as bs
from utility import request_site_page
from urllib.parse import urljoin

logger = logger_init('地方证监局-数据抓取')
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


def local_csrc_crawler():
    # 已有单独页面的行政处罚决定链接
    xzcf_url_list = [
        {'url': 'http://www.csrc.gov.cn/pub/beijing/bjxyzl/bjxzcf/', 'area': '北京证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/beijing/bjxzcf/', 'area': '北京证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/tianjin/xzcf/', 'area': '天津证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/hebei/hbxzcf/', 'area': '河北证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/shanxi/xzcf/', 'area': '山西证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/neimenggu/nmgxzcf/', 'area': '内蒙古证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/liaoning/lnjxzcf/', 'area': '辽宁证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/jilin/jlxzcf/', 'area': '吉林证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/heilongjiang/hljjxzcf/', 'area': '黑龙江证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/shanghai/xzcf/', 'area': '上海证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/jiangsu/jsxzcf/', 'area': '江苏证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zhejiang/zjxzcf/', 'area': '浙江证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/anhui/ahxzcf/', 'area': '安徽证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/fujian/fjjxzcf/', 'area': '福建证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/jiangxi/jxxzcf/', 'area': '江西证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/shandong/sdxzcf/', 'area': '山东证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/henan/hnxzcf/', 'area': '河南证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/hubei/hbxzcf/', 'area': '湖北证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/hunan/hnxzcf/', 'area': '湖南证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/guangdong/xzcf/', 'area': '广东证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/hainan/hnjxzcf/', 'area': '海南证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/chongqing/cqjxzcf/', 'area': '重庆证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/sichuan/scxzcf/', 'area': '四川证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/guizhou/gzxzcf/', 'area': '贵州证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/xizang/xzxzcf/', 'area': '西藏证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/qinghai/qhxzcf/', 'area': '青海证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/xinjiang/xjxzcf/', 'area': '新疆证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/shenzhen/xzcf/', 'area': '深圳证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/dalian/dlxzcf/', 'area': '大连证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/ningbo/nbxzcf/', 'area': '宁波证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/xiamen/xmxzcf/', 'area': '厦门证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/qingdao/xzcf/', 'area': '青岛证监局', 'type': '行政处罚决定'}
    ]

    # 已有单独页面的监管措施链接
    jgcs_url_list = [
        {'url': 'http://www.csrc.gov.cn/pub/beijing/bjxyzl/bjxzjgcs/', 'area': '北京证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/beijing/bjjgcs/', 'area': '北京证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/jilin/jljgcs/', 'area': '吉林证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/heilongjiang/jgcs/', 'area': '黑龙江证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zhejiang/zjcxxx/', 'area': '浙江证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/hunan/hnjxzjgcs/', 'area': '湖南证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/guangdong/gdjjgcs/', 'area': '广东证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/shenzhen/ztzl/ssgsjgxx/jgcs/', 'area': '深圳证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/dalian/dljgcs/', 'area': '大连证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/xiamen/xmjgcs/', 'area': '厦门证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/qingdao/jgcs/', 'area': '青岛证监局', 'type': '监管措施'}
    ]

    # 以下地址检索标题中含有“行政处罚决定书”的公告
    xzcf_search_url_list = [
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofheb/', 'area': '河北证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofsx/', 'area': '山西证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofhlj/', 'area': '黑龙江证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofjs/', 'area': '江苏证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofsd/', 'area': '山东证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofhen/', 'area': '河南证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofhn/', 'area': '湖南证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofgx/', 'area': '广西证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofhan/', 'area': '海南证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofyn/', 'area': '云南证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofxz/', 'area': '西藏证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofsax/', 'area': '陕西证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofgs/', 'area': '甘肃证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofnx/', 'area': '宁夏证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofsz/', 'area': '深圳证监局', 'type': '行政处罚决定'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofqd/', 'area': '青岛证监局', 'type': '行政处罚决定'},
    ]

    # 以下地址检索标题中含有“行政处罚”的公告
    xzcf_search_url_other_list = [
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofsh/', 'area': '上海证监局', 'type': '行政处罚决定'}
    ]

    # 搜索名称中有“措施的决定”或者“行政监管措施决定书”的公告
    jgcs_search_url_list = [
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofbj/', 'area': '北京证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicoftj/', 'area': '天津证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofheb/', 'area': '河北证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofsx/', 'area': '山西证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofnmg/', 'area': '内蒙古证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofln/', 'area': '辽宁证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofjl/', 'area': '吉林证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofhlj/', 'area': '黑龙江证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofsh/', 'area': '上海证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofjs/', 'area': '江苏证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofzj/', 'area': '浙江证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofah/', 'area': '安徽证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicoffj/', 'area': '福建证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofjx/', 'area': '江西证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofsd/', 'area': '山东证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofhen/', 'area': '河南证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofhb/', 'area': '湖北证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofhn/', 'area': '湖南证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofgd/', 'area': '广东证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofgx/', 'area': '广西证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofhan/', 'area': '海南证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofcq/', 'area': '重庆证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofsc/', 'area': '四川证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofgz/', 'area': '贵州证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofyn/', 'area': '云南证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofxz/', 'area': '西藏证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofsax/', 'area': '陕西证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofgs/', 'area': '甘肃证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofqh/', 'area': '青海证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofnx/', 'area': '宁夏证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofxj/', 'area': '新疆证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofsz/', 'area': '深圳证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofdl/', 'area': '大连证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofnb/', 'area': '宁波证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofxm/', 'area': '厦门证监局', 'type': '监管措施'},
        {'url': 'http://www.csrc.gov.cn/pub/zjhpublicofqd/', 'area': '青岛证监局', 'type': '监管措施'}
    ]

    logger.info('地方证监局 数据抓取')

    new_local_csrc_announcement_list = []

    # 已有单独页面的行政处罚决定链接
    for each_xzcf_url_info in xzcf_url_list:
        logger.info(each_xzcf_url_info['area'] + each_xzcf_url_info['type'] + ' ' + each_xzcf_url_info['url'])

        # get page_count
        page_count_url = each_xzcf_url_info['url']
        response = request_site_page(page_count_url)
        if response is None:
            logger.error('网页请求错误 %s' % page_count_url)
            continue

        try:
            page_count = int(re.search(r'var countPage = (\d+)?//共多少页',
                                       response.text.encode(response.encoding).decode('utf-8')).group(1).strip()) \
                if re.search(r'var countPage = (\d+)?//共多少页',
                             response.text.encode(response.encoding).decode('utf-8')) else 0
            logger.info(each_xzcf_url_info['area'] + each_xzcf_url_info['type'] + ' --  一共有%d页' % page_count)
        except Exception as e:
            logger.error(e)
            page_count = 0

        # get crawler data
        for i in range(page_count):
            logger.info(each_xzcf_url_info['area'] + each_xzcf_url_info['type'] + '-- 第%d页' % (i + 1))
            url = each_xzcf_url_info['url'] + 'index_' + str(i) + '.html' if i > 0 \
                else each_xzcf_url_info['url'] + 'index.html'

            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
                dl_content = content_soup.find(class_='fl_list')
                if not dl_content:
                    logger.error('网页请求错误 %s' % url)
                    continue
                for each_dd in dl_content.find_all('li'):
                    if len(each_dd.find_all('a')) > 0:
                        try:
                            announcement_url = urljoin(url, each_dd.find('a').attrs['href'])
                            if db.csrc_data.find({'url': announcement_url}).count() == 0:
                                title = each_dd.find('a').text.strip()
                                announcement_date = each_dd.find('span').text.strip()
                                logger.info(
                                    each_xzcf_url_info['area'] + each_xzcf_url_info['type'] + '新公告：' + announcement_url)
                                post = {
                                    'title': title,
                                    'publishDate': announcement_date,
                                    'url': announcement_url,
                                    'type': each_xzcf_url_info['type'],
                                    'origin': each_xzcf_url_info['area'],
                                    'status': 'not parsed'
                                }
                                if post not in new_local_csrc_announcement_list:
                                    new_local_csrc_announcement_list.append(post)
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
        logger.info('\n')

    # 已有单独页面的监管措施链接
    for each_jgcs_url_info in jgcs_url_list:
        logger.info(each_jgcs_url_info['area'] + each_jgcs_url_info['type'] + ' ' + each_jgcs_url_info['url'])

        # get page_count
        page_count_url = each_jgcs_url_info['url']
        response = request_site_page(page_count_url)
        if response is None:
            logger.error('网页请求错误 %s' % page_count_url)
            continue

        try:
            page_count = int(re.search(r'var countPage = (\d+)?//共多少页',
                                       response.text.encode(response.encoding).decode('utf-8')).group(1).strip()) if \
                re.search(r'var countPage = (\d+)?//共多少页',
                          response.text.encode(response.encoding).decode('utf-8')) else 0
            logger.info(each_jgcs_url_info['area'] + each_jgcs_url_info['type'] + ' --  一共有%d页' % page_count)
        except Exception as e:
            logger.error(e)
            page_count = 0

        # get crawler data
        for i in range(page_count):
            logger.info(each_jgcs_url_info['area'] + each_jgcs_url_info['type'] + ' --  第%d页' % (i + 1))
            url = each_jgcs_url_info['url'] + 'index_' + str(i) + '.html' if i > 0 \
                else each_jgcs_url_info['url'] + 'index.html'

            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')

                dl_content = content_soup.find(class_='fl_list')
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
                                logger.info(
                                    each_jgcs_url_info['area'] + each_jgcs_url_info['type'] + '新公告：' + announcement_url)
                                post = {
                                    'title': title,
                                    'publishDate': announcement_date,
                                    'url': announcement_url,
                                    'type': each_jgcs_url_info['type'],
                                    'origin': each_jgcs_url_info['area'],
                                    'status': 'not parsed'
                                }
                                if post not in new_local_csrc_announcement_list:
                                    new_local_csrc_announcement_list.append(post)
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
        logger.info('\n')

    # 以下地址检索标题中含有“行政处罚决定书”的公告
    for each_xzcf_search_url_info in xzcf_search_url_list:
        logger.info(each_xzcf_search_url_info['area'] + each_xzcf_search_url_info['type'] + '检索' + ' ' +
                    each_xzcf_search_url_info['url'])

        # get page_count
        page_count_url = each_xzcf_search_url_info['url']
        params = {
            'SType': '1',
            'searchColumn': 'biaoti',
            'searchYear': 'all',
            'preSWord': 'docTitle=("行政处罚决定书")',
            'sword': '行政处罚决定书',
            'searchAgain': '',
            'page': 1,
            'res_wenzhong': '',
            'res_wenzhonglist': '',
            'wenzhong': '',
            'pubwebsite': '/' + page_count_url.split('/')[-2] + '/'
        }
        response = request_site_page('http://www.csrc.gov.cn/wcm/govsearch/simp_gov_list.jsp', params=params,
                                     methods='post')
        if response is None:
            logger.error('网页请求错误')
            continue

        try:
            page_count = int(int(re.search(r'var m_nRecordCount = (\d+)?;', response.text).group(1).strip()) / 20 + 1)
            logger.info(
                each_xzcf_search_url_info['area'] + each_xzcf_search_url_info['type'] + '检索 一共有%d页' % page_count)
        except Exception as e:
            logger.error(e)
            page_count = 0

        # get crawler data
        for i in range(page_count):
            logger.info(
                each_xzcf_search_url_info['area'] + each_xzcf_search_url_info['type'] + '检索 第%d页' % (i + 1))
            params['page'] = i + 1
            try:
                content_response = request_site_page('http://www.csrc.gov.cn/wcm/govsearch/simp_gov_list.jsp',
                                                     params=params, methods='post')
                if content_response is None:
                    logger.error('网页请求错误')
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')

                dl_content = content_soup.find(id='documentContainer')
                if not dl_content:
                    logger.error('网页请求错误')
                    continue
                for each_row in dl_content.find_all(class_='row'):
                    try:
                        announcement_url = urljoin(each_xzcf_search_url_info['url'], each_row.find('a').attrs['href'])
                        announcement_url = announcement_url.split('?')[0].strip()
                        if db.csrc_data.find({'url': announcement_url}).count() == 0:
                            title = each_row.find('a').text.strip()
                            announcement_date = each_row.find(class_='fbrq').text.strip()
                            logger.info(each_xzcf_search_url_info['area'] + each_xzcf_search_url_info['type'] +
                                        '检索新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': announcement_date,
                                'url': announcement_url,
                                'type': each_xzcf_search_url_info['type'],
                                'origin': each_xzcf_search_url_info['area'],
                                'status': 'not parsed'
                            }
                            if post not in new_local_csrc_announcement_list:
                                new_local_csrc_announcement_list.append(post)
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
        logger.info('\n')

    # 以下地址检索标题中含有“行政处罚”的公告
    for each_xzcf_search_url_other_info in xzcf_search_url_other_list:
        logger.info(each_xzcf_search_url_other_info['area'] + each_xzcf_search_url_other_info['type'] + '检索' + ' ' +
                    each_xzcf_search_url_other_info['url'])

        # get page_count
        page_count_url = each_xzcf_search_url_other_info['url']
        params = {
            'SType': '1',
            'searchColumn': 'biaoti',
            'searchYear': 'all',
            'preSWord': 'docTitle=("行政处罚")',
            'sword': '行政处罚',
            'searchAgain': '',
            'page': 1,
            'res_wenzhong': '',
            'res_wenzhonglist': '',
            'wenzhong': '',
            'pubwebsite': '/' + page_count_url.split('/')[-2] + '/'
        }
        response = request_site_page('http://www.csrc.gov.cn/wcm/govsearch/simp_gov_list.jsp', params=params,
                                     methods='post')
        if response is None:
            logger.error('网页请求错误')
            continue
        try:
            page_count = int(int(re.search(r'var m_nRecordCount = (\d+)?;', response.text).group(1).strip()) / 20 + 1)
            logger.info(each_xzcf_search_url_other_info['area'] + each_xzcf_search_url_other_info['type'] +
                        '检索 一共有%d页' % page_count)
        except Exception as e:
            logger.error(e)
            page_count = 0

            # get crawler data
        for i in range(page_count):
            logger.info(
                each_xzcf_search_url_other_info['area'] + each_xzcf_search_url_other_info['type'] + '检索 第%d页' % (i + 1))
            params['page'] = i + 1

            try:
                content_response = request_site_page('http://www.csrc.gov.cn/wcm/govsearch/simp_gov_list.jsp',
                                                     params=params, methods='post')
                if content_response is None:
                    logger.error('网页请求错误')
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')

                dl_content = content_soup.find(id='documentContainer')
                if not dl_content:
                    logger.error('网页请求错误')
                    continue
                for each_row in dl_content.find_all(class_='row'):
                    try:
                        announcement_url = urljoin(each_xzcf_search_url_other_info['url'], each_row.find('a').attrs['href'])
                        announcement_url = announcement_url.split('?')[0].strip()
                        if db.csrc_data.find({'url': announcement_url}).count() == 0:
                            title = each_row.find('a').text.strip()
                            announcement_date = each_row.find(class_='fbrq').text.strip()
                            logger.info(each_xzcf_search_url_other_info['area'] + each_xzcf_search_url_other_info['type'] +
                                        '检索新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': announcement_date,
                                'url': announcement_url,
                                'type': each_xzcf_search_url_other_info['type'],
                                'origin': each_xzcf_search_url_other_info['area'],
                                'status': 'not parsed'
                            }
                            if post not in new_local_csrc_announcement_list:
                                new_local_csrc_announcement_list.append(post)
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
        logger.info('\n')

    # 搜索名称中有“措施的决定”或者“行政监管措施决定书”的公告
    for each_jgcs_search_url_info in jgcs_search_url_list:
        logger.info(each_jgcs_search_url_info['area'] + each_jgcs_search_url_info['type'] + '检索' + ' ' +
                    each_jgcs_search_url_info['url'])

        # 措施的决定
        # get page_count
        page_count_url = each_jgcs_search_url_info['url']
        params = {
            'SType': '1',
            'searchColumn': 'biaoti',
            'searchYear': 'all',
            'preSWord': 'docTitle=("措施的决定")',
            'sword': '措施的决定',
            'searchAgain': '',
            'page': 1,
            'res_wenzhong': '',
            'res_wenzhonglist': '',
            'wenzhong': '',
            'pubwebsite': '/' + page_count_url.split('/')[-2] + '/'
        }
        response = request_site_page('http://www.csrc.gov.cn/wcm/govsearch/simp_gov_list.jsp', params=params,
                                     methods='post')
        if response is None:
            logger.error('网页请求错误')
            continue

        try:
            page_count = int(int(re.search(r'var m_nRecordCount = (\d+)?;', response.text).group(1).strip()) / 20 + 1)
            logger.info(
                each_jgcs_search_url_info['area'] + each_jgcs_search_url_info['type'] + '检索 一共有%d页' % page_count)
        except Exception as e:
            logger.error(e)
            page_count = 0

        # get crawler data
        for i in range(page_count):
            logger.info(
                each_jgcs_search_url_info['area'] + each_jgcs_search_url_info['type'] + '检索 第%d页' % (i + 1))
            params['page'] = i + 1

            try:
                content_response = request_site_page('http://www.csrc.gov.cn/wcm/govsearch/simp_gov_list.jsp',
                                                     params=params, methods='post')
                if content_response is None:
                    logger.error('网页请求错误')
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')

                dl_content = content_soup.find(id='documentContainer')
                if not dl_content:
                    logger.error('网页请求错误')
                    continue
                for each_row in dl_content.find_all(class_='row'):
                    try:
                        announcement_url = urljoin(each_jgcs_search_url_info['url'], each_row.find('a').attrs['href'])
                        announcement_url = announcement_url.split('?')[0].strip()
                        if db.csrc_data.find({'url': announcement_url}).count() == 0:
                            title = each_row.find('a').text.strip()
                            announcement_date = each_row.find(class_='fbrq').text.strip()
                            logger.info(each_jgcs_search_url_info['area'] + each_jgcs_search_url_info['type'] +
                                        '检索新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': announcement_date,
                                'url': announcement_url,
                                'type': each_jgcs_search_url_info['type'],
                                'origin': each_jgcs_search_url_info['area'],
                                'status': 'not parsed'
                            }
                            if post not in new_local_csrc_announcement_list:
                                new_local_csrc_announcement_list.append(post)
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

        # 行政监管措施决定书
        # get page_count
        page_count_url = each_jgcs_search_url_info['url']
        params = {
            'SType': '1',
            'searchColumn': 'biaoti',
            'searchYear': 'all',
            'preSWord': 'docTitle=("行政监管措施决定书")',
            'sword': '行政监管措施决定书',
            'searchAgain': '',
            'page': 1,
            'res_wenzhong': '',
            'res_wenzhonglist': '',
            'wenzhong': '',
            'pubwebsite': '/' + page_count_url.split('/')[-2] + '/'
        }
        response = request_site_page('http://www.csrc.gov.cn/wcm/govsearch/simp_gov_list.jsp', params=params,
                                     methods='post')

        try:
            page_count = int(int(re.search(r'var m_nRecordCount = (\d+)?;', response.text).group(1).strip()) / 20 + 1)
            logger.info(
                each_jgcs_search_url_info['area'] + each_jgcs_search_url_info['type'] + '检索 一共有%d页' % page_count)
        except Exception as e:
            logger.error(e)
            page_count = 0

        # get crawler data
        for i in range(page_count):
            logger.info(
                each_jgcs_search_url_info['area'] + each_jgcs_search_url_info['type'] + '检索 第%d页' % (i + 1))
            params['page'] = i + 1

            try:
                content_response = request_site_page('http://www.csrc.gov.cn/wcm/govsearch/simp_gov_list.jsp',
                                                     params=params, methods='post')
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')

                dl_content = content_soup.find(id='documentContainer')
                for each_row in dl_content.find_all(class_='row'):
                    try:
                        announcement_url = urljoin(each_jgcs_search_url_info['url'], each_row.find('a').attrs['href'])
                        announcement_url = announcement_url.split('?')[0].strip()
                        if db.csrc_data.find({'url': announcement_url}).count() == 0:
                            title = each_row.find('a').text.strip()
                            announcement_date = each_row.find(class_='fbrq').text.strip()
                            logger.info(each_jgcs_search_url_info['area'] + each_jgcs_search_url_info['type'] +
                                        '检索新公告：' + announcement_url)
                            post = {
                                'title': title,
                                'publishDate': announcement_date,
                                'url': announcement_url,
                                'type': each_jgcs_search_url_info['type'],
                                'origin': each_jgcs_search_url_info['area'],
                                'status': 'not parsed'
                            }
                            if post not in new_local_csrc_announcement_list:
                                new_local_csrc_announcement_list.append(post)
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
        logger.info('\n')

    if len(new_local_csrc_announcement_list) > 0:
        logger.info('地方证监局一共有%d条新公告，导入数据库中......' % len(new_local_csrc_announcement_list))
        r = db.csrc_data.insert_many(new_local_csrc_announcement_list)
        if len(r.inserted_ids) == len(new_local_csrc_announcement_list):
            logger.info('地方证监局公告导入完成！')
        else:
            logger.error('地方证监局公告导入出现问题！')
    else:
        logger.info('地方证监局没有新公告！')


if __name__ == "__main__":
    local_csrc_crawler()
