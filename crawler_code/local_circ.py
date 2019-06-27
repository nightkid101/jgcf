import re

from pymongo import MongoClient
from bs4 import BeautifulSoup as bs
from init import logger_init, config_init
from utility import request_site_page
from urllib.parse import urljoin

logger = logger_init('地方保监局-数据抓取')
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


# 北京保监局
def beijing():
    result_list = []
    prefix_url = 'http://beijing.circ.gov.cn/web/site3/tab3419/module'
    module_id_list = ['27974', '27568', '27246', '26485', '25373', '14116', '12627', '12438', '11871', '8950', '11169',
                      '11170', '11171']

    for each_module_id in module_id_list:

        # get each_module page count
        response = request_site_page(prefix_url + each_module_id + '/page1.htm')
        if response is None:
            logger.error('网页请求错误 %s' % (prefix_url + each_module_id + '/page1.htm'))
            continue
        soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
        page_count_text = soup.find(
            id='ess_ctr' + each_module_id + '_ModuleContent').find_all(class_='Normal')[-1].text \
            if soup.find(id='ess_ctr' + each_module_id + '_ModuleContent') else ''
        page_count = int(re.findall(r'\d+', page_count_text)[-1]) if page_count_text != '' else 0
        logger.info('北京保监局' + each_module_id + ' -- 行政处罚决定' + ' 一共有%d页' % page_count)

        # get each_module crawler data
        for i in range(page_count):
            logger.info('北京保监局' + each_module_id + ' -- 行政处罚决定' + ' 第%d页' % (i + 1))
            url = prefix_url + each_module_id + '/page' + str(i + 1) + '.htm'

            try:
                content_response = request_site_page(url)
                if content_response is None:
                    logger.error('网页请求错误 %s' % url)
                    continue
                content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
                table_content = content_soup.find(id='ess_ctr' + each_module_id + '_ListC_Info_LstC_Info')

                if not table_content:
                    logger.error('网页请求错误 %s' % url)
                    continue

                for each_tr in table_content.find_all('table'):
                    try:
                        announcement_url = urljoin(url, each_tr.find('a').attrs.get('href', '').strip())
                        if db.circ_data.find({'url': announcement_url}).count() == 0:
                            title = each_tr.find('a').attrs.get('title', '').strip()
                            publish_date = each_tr.find_all('td')[-1].text.strip().replace('(', '20').replace(')', '')
                            post = {
                                'title': title,
                                'publishDate': publish_date,
                                'url': announcement_url,
                                'type': '行政处罚决定',
                                'origin': '北京保监局',
                                'status': 'not parsed'
                            }
                            if db.circ_data.find({'url': {'$regex': '.*' + announcement_url.split('/')[-1] + '$'},
                                                  'publishDate': publish_date}).count() == 1:
                                logger.info('北京保监局 -- 行政处罚决定 -- 更新数据库：' + announcement_url)
                                db.circ_data.update_one(
                                    {'url': {'$regex': '.*' + announcement_url.split('/')[-1] + '$'},
                                     'publishDate': publish_date}, {'$set': post})
                            else:
                                logger.info('北京保监局 -- 行政处罚决定 -- 新公告：' + announcement_url)
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
        logger.info('北京保监局 -- 行政处罚决定 -- 一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.circ_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('北京保监局 -- 行政处罚决定 -- 公告导入完成！')
        else:
            logger.error('北京保监局 -- 行政处罚决定 -- 公告导入出现问题！')
    else:
        logger.info('北京保监局 -- 行政处罚决定 -- 没有新公告！')


# 西藏保监局
# 只有一页 所以单独开来
def xizang():
    result_list = []
    page_count = 1
    logger.info('西藏保监局 -- 行政处罚决定' + ' 一共有%d页' % page_count)

    for i in range(page_count):
        logger.info('西藏保监局 -- 行政处罚决定' + ' 第%d页' % (i + 1))
        url = 'http://xizang.circ.gov.cn/web/site49/tab4613/'
        content_response = request_site_page(url)
        if content_response is None:
            logger.error('网页请求错误 %s' % url)
            continue
        content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
        table_content = content_soup.find(id='ess_ctr12923_ListC_Info_LstC_Info')

        if not table_content:
            logger.error('网页请求错误 %s' % url)
            continue

        for each_tr in table_content.find_all('table'):
            try:
                announcement_url = urljoin(url, each_tr.find('a').attrs.get('href', '').strip())
                if db.circ_data.find({'url': announcement_url}).count() == 0:
                    title = each_tr.find('a').attrs.get('title', '').strip()
                    publish_date = each_tr.find_all('td')[-1].text.strip().replace('(', '20').replace(')', '')
                    post = {
                        'title': title,
                        'publishDate': publish_date,
                        'url': announcement_url,
                        'type': '行政处罚决定',
                        'origin': '西藏保监局',
                        'status': 'not parsed'
                    }
                    if db.circ_data.find({'url': {'$regex': '.*' + announcement_url.split('/')[-1] + '$'},
                                          'publishDate': publish_date}).count() == 1:
                        logger.info('西藏保监局 -- 行政处罚决定 -- 更新数据库：' + announcement_url)
                        db.circ_data.update_one(
                            {'url': {'$regex': '.*' + announcement_url.split('/')[-1] + '$'},
                             'publishDate': publish_date}, {'$set': post})
                    else:
                        logger.info('西藏保监局 -- 行政处罚决定 -- 新公告：' + announcement_url)
                        if post not in result_list:
                            result_list.append(post)

                else:
                    if config['crawler_update_type']['update_type'] == '0':
                        break
            except Exception as e:
                logger.error(e)
                logger.warning('提取公告url出现问题')
                continue

    if len(result_list) > 0:
        logger.info('西藏保监局 -- 行政处罚 -- 一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.circ_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('西藏保监局 -- 行政处罚 -- 公告导入完成！')
        else:
            logger.error('西藏保监局 -- 行政处罚 -- 公告导入出现问题！')
    else:
        logger.info('西藏保监局 -- 行政处罚 -- 没有新公告！')


# 其它 保监局
def local_circ(prefix_url, table_id, city_info):
    result_list = []

    # get page count
    response = request_site_page(prefix_url + '1.htm')
    if response is None:
        logger.error('网页请求错误 %s' % prefix_url + '1.htm')
        return
    soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
    page_count_text = soup.find_all(class_='Normal')[-1].text if soup.find(class_='Normal') else ''
    page_count = int(re.findall(r'\d+', page_count_text)[-1]) if page_count_text != '' else 0
    logger.info(city_info + '保监局 -- 行政处罚决定' + ' 一共有%d页' % page_count)

    # get crawler data
    for i in range(page_count + 1):
        logger.info(city_info + '保监局 -- 行政处罚决定' + ' 第%d页' % i)
        if i == 0:
            url = re.search(r'(^.*tab\d+\/).*?$', prefix_url).group(1).strip()
        else:
            url = prefix_url + str(i) + '.htm'

        try:
            content_response = request_site_page(url)
            if content_response is None:
                logger.error('网页请求错误 %s' % url)
                continue
            content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
            table_content = content_soup.find(id=table_id)

            if not table_content:
                logger.error('网页请求错误 %s' % url)
                continue

            for each_tr in table_content.find_all('table'):
                try:
                    announcement_url = urljoin(url, each_tr.find('a').attrs['href'])
                    if db.circ_data.find({'url': announcement_url}).count() == 0:
                        title = each_tr.find('a').attrs['title'].strip()
                        publish_date = each_tr.find_all('td')[-1].text.strip().replace('(', '20').replace(')', '')
                        post = {
                            'title': title,
                            'publishDate': publish_date,
                            'url': announcement_url,
                            'type': '行政处罚决定',
                            'origin': city_info + '保监局',
                            'status': 'not parsed'
                        }
                        if db.circ_data.find(
                                {'url': {'$regex': '.*' + announcement_url.split('/')[-1] + '$'}}).count() == 1:
                            logger.info(city_info + '保监局 -- 行政处罚决定 -- 更新数据库：' + announcement_url)
                            db.circ_data.update_one(
                                {'url': {'$regex': '.*' + announcement_url.split('/')[-1] + '$'},
                                 'publishDate': publish_date}, {'$set': post})
                        else:
                            logger.info(city_info + '保监局 -- 行政处罚决定 -- 新公告：' + announcement_url)
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
        logger.info(city_info + '保监局 -- 行政处罚决定 -- 一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.circ_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info(city_info + '保监局 -- 行政处罚决定 -- 公告导入完成！')
        else:
            logger.error(city_info + '保监局 -- 行政处罚决定 -- 公告导入出现问题！')
    else:
        logger.info(city_info + '保监局 -- 行政处罚决定 -- 没有新公告！')
    logger.info('\n')


def local_circ_crawler():
    city_list = [
        {
            'city': '天津',
            'prefix_url': 'http://tianjin.circ.gov.cn/web/site35/tab3385/module8892/page',
            'table_id': 'ess_ctr8892_ListC_Info_LstC_Info'
        },
        {
            'city': '河北',
            'prefix_url': 'http://hebei.circ.gov.cn/web/site19/tab3439/module9910/page',
            'table_id': 'ess_ctr9910_ListC_Info_LstC_Info'
        },
        {
            'city': '山西',
            'prefix_url': 'http://shanxi.circ.gov.cn/web/site31/tab3452/module9077/page',
            'table_id': 'ess_ctr9077_ListC_Info_LstC_Info'
        },
        {
            'city': '黑龙江',
            'prefix_url': 'http://heilongjiang.circ.gov.cn/web/site20/tab3422/module9912/page',
            'table_id': 'ess_ctr9912_ListC_Info_LstC_Info'
        },
        {
            'city': '上海',
            'prefix_url': 'http://shanghai.circ.gov.cn/web/site7/tab3427/module9914/page',
            'table_id': 'ess_ctr9914_ListC_Info_LstC_Info'
        },
        {
            'city': '江苏',
            'prefix_url': 'http://jiangsu.circ.gov.cn/web/site24/tab3392/module8911/page',
            'table_id': 'ess_ctr8911_ListC_Info_LstC_Info'
        },
        {
            'city': '浙江',
            'prefix_url': 'http://zhejiang.circ.gov.cn/web/site39/tab3594/module9905/page',
            'table_id': 'ess_ctr9905_ListC_Info_LstC_Info'
        },
        {
            'city': '山东',
            'prefix_url': 'http://shandong.circ.gov.cn/web/site30/tab3444/module9029/page',
            'table_id': 'ess_ctr9029_ListC_Info_LstC_Info'
        },
        {
            'city': '河南',
            'prefix_url': 'http://henan.circ.gov.cn/web/site10/tab3426/module9897/page',
            'table_id': 'ess_ctr9897_ListC_Info_LstC_Info'
        },
        {
            'city': '湖北',
            'prefix_url': 'http://hubei.circ.gov.cn/web/site21/tab3434/module8998/page',
            'table_id': 'ess_ctr8998_ListC_Info_LstC_Info'
        },
        {
            'city': '湖南',
            'prefix_url': 'http://hunan.circ.gov.cn/web/site22/tab3410/module9893/page',
            'table_id': 'ess_ctr9893_ListC_Info_LstC_Info'
        },
        {
            'city': '重庆',
            'prefix_url': 'http://chongqing.circ.gov.cn/web/site40/tab3460/module9889/page',
            'table_id': 'ess_ctr9889_ListC_Info_LstC_Info'
        },
        {
            'city': '四川',
            'prefix_url': 'http://sichuan.circ.gov.cn/web/site34/tab3596/module9888/page',
            'table_id': 'ess_ctr9888_ListC_Info_LstC_Info'
        },
        {
            'city': '贵州',
            'prefix_url': 'http://guizhou.circ.gov.cn/web/site17/tab3467/module9099/page',
            'table_id': 'ess_ctr9099_ListC_Info_LstC_Info'
        },
        {
            'city': '云南',
            'prefix_url': 'http://yunnan.circ.gov.cn/web/site38/tab3458/module9879/page',
            'table_id': 'ess_ctr9879_ListC_Info_LstC_Info'
        },
        {
            'city': '青海',
            'prefix_url': 'http://qinghai.circ.gov.cn/web/site41/tab3428/module9877/page',
            'table_id': 'ess_ctr9877_ListC_Info_LstC_Info'
        },
        {
            'city': '宁夏',
            'prefix_url': 'http://ningxia.circ.gov.cn/web/site28/tab3598/module9872/page',
            'table_id': 'ess_ctr9872_ListC_Info_LstC_Info'
        },
        {
            'city': '新疆',
            'prefix_url': 'http://xinjiang.circ.gov.cn/web/site37/tab3407/module8927/page',
            'table_id': 'ess_ctr8927_ListC_Info_LstC_Info'
        },
        {
            'city': '深圳',
            'prefix_url': 'http://shenzhen.circ.gov.cn/web/site33/tab3425/module8974/page',
            'table_id': 'ess_ctr8974_ListC_Info_LstC_Info'
        },
        {
            'city': '厦门',
            'prefix_url': 'http://xiamen.circ.gov.cn/web/site36/tab3415/module8941/page',
            'table_id': 'ess_ctr8941_ListC_Info_LstC_Info'
        },
        {
            'city': '苏州',
            'prefix_url': 'http://jiangsu.circ.gov.cn/web/site61/tab4852/module13407/page',
            'table_id': 'ess_ctr13407_ListC_Info_LstC_Info'
        },
        {
            'city': '烟台',
            'prefix_url': 'http://shandong.circ.gov.cn/web/site59/tab4813/module13326/page',
            'table_id': 'ess_ctr13326_ListC_Info_LstC_Info'
        },
        {
            'city': '汕头',
            'prefix_url': 'http://guangdong.circ.gov.cn/web/site53/tab4679/module13910/page',
            'table_id': 'ess_ctr13910_ListC_Info_LstC_Info'
        },
        {
            'city': '内蒙古',
            'prefix_url': 'http://neimenggu.circ.gov.cn/web/site4/tab3394/module8916/page',
            'table_id': 'ess_ctr8916_ListC_Info_LstC_Info'
        },
        {
            'city': '辽宁',
            'prefix_url': 'http://liaoning.circ.gov.cn/web/site43/tab3418/module8948/page',
            'table_id': 'ess_ctr8948_ListC_Info_LstC_Info'
        },
        {
            'city': '吉林',
            'prefix_url': 'http://jilin.circ.gov.cn/web/site23/tab3593/module9911/page',
            'table_id': 'ess_ctr9911_ListC_Info_LstC_Info'
        },
        {
            'city': '安徽',
            'prefix_url': 'http://anhui.circ.gov.cn/web/site11/tab3388/module8940/page',
            'table_id': 'ess_ctr8940_ListC_Info_LstC_Info'
        },
        {
            'city': '福建',
            'prefix_url': 'http://fujian.circ.gov.cn/web/site13/tab3386/module9903/page',
            'table_id': 'ess_ctr9903_ListC_Info_LstC_Info'
        },
        {
            'city': '江西',
            'prefix_url': 'http://jiangxi.circ.gov.cn/web/site25/tab3595/module9898/page',
            'table_id': 'ess_ctr9898_ListC_Info_LstC_Info'
        },
        {
            'city': '广东',
            'prefix_url': 'http://guangdong.circ.gov.cn/web/site15/tab3454/module9049/page',
            'table_id': 'ess_ctr9049_ListC_Info_LstC_Info'
        },
        {
            'city': '广西',
            'prefix_url': 'http://guangxi.circ.gov.cn/web/site16/tab3448/module9055/page',
            'table_id': 'ess_ctr9055_ListC_Info_LstC_Info'
        },
        {
            'city': '海南',
            'prefix_url': 'http://hainan.circ.gov.cn/web/site18/tab3416/module8943/page',
            'table_id': 'ess_ctr8943_ListC_Info_LstC_Info'
        },
        {
            'city': '陕西',
            'prefix_url': 'http://shaanxi.circ.gov.cn/web/site44/tab3597/module9878/page',
            'table_id': 'ess_ctr9878_ListC_Info_LstC_Info'
        },
        {
            'city': '甘肃',
            'prefix_url': 'http://gansu.circ.gov.cn/web/site14/tab3389/module8914/page',
            'table_id': 'ess_ctr8914_ListC_Info_LstC_Info'
        },
        {
            'city': '大连',
            'prefix_url': 'http://dalian.circ.gov.cn/web/site12/tab3429/module8991/page',
            'table_id': 'ess_ctr8991_ListC_Info_LstC_Info'
        },
        {
            'city': '宁波',
            'prefix_url': 'http://ningbo.circ.gov.cn/web/site27/tab3466/module9866/page',
            'table_id': 'ess_ctr9866_ListC_Info_LstC_Info'
        },
        {
            'city': '青岛',
            'prefix_url': 'http://qingdao.circ.gov.cn/web/site29/tab3435/module9005/page',
            'table_id': 'ess_ctr9005_ListC_Info_LstC_Info'
        },
        {
            'city': '温州',
            'prefix_url': 'http://zhejiang.circ.gov.cn/web/site54/tab4735/module13164/page',
            'table_id': 'ess_ctr13164_ListC_Info_LstC_Info'
        },
        {
            'city': '唐山',
            'prefix_url': 'http://hebei.circ.gov.cn/web/site58/tab4774/module13245/page',
            'table_id': 'ess_ctr13245_ListC_Info_LstC_Info'
        }
    ]
    for each_city in city_list:
        local_circ(each_city['prefix_url'], each_city['table_id'], each_city['city'])
    beijing()
    xizang()


if __name__ == "__main__":
    local_circ_crawler()
