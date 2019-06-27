import re
import jsbeautifier
import js2py
import time

from pymongo import MongoClient
from init import logger_init, config_init
import requests
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin

logger = logger_init('地方人民银行-数据抓取')
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

# 抓取数据存入pbc_data这个collection
db.pbc_data.create_index([('url', 1)])


def prepare(url):
    # get real html content
    # 利用session保存cookie信息，第一次请求会设置cookie类似
    # {'wzwsconfirm': 'ab3039756ba3ee041f7e68f634d28882', 'wzwsvtime': '1488938461'}，
    # 与js解析得到的cookie合起来才能通过验证
    r = requests.session()
    content = r.get(url, timeout=(10, 30)).content
    time.sleep(3)
    # 获取页面脚本内容
    re_script = re.search(r'<script type="text/javascript">(?P<script>.*)</script>', content.decode('utf-8'),
                          flags=re.DOTALL)
    # 用点匹配所有字符，用(?P<name>...)获取：https://docs.python.org/3/howto/regex.html#regex-howto
    script = re_script.group('script')
    script = script.replace('\r\n', '')
    # 在美化之前，去掉\r\n之类的字符才有更好的效果
    res = jsbeautifier.beautify(script)
    # 美化并一定程度解析js代码：https://github.com/beautify-web/js-beautify
    js_code_list = res.split('function')
    var_ = js_code_list[0]
    var_list = var_.split('\n')
    if len(var_list) == 0:
        logger.error('网页请求错误 %s' % url)
        return
    template_js = var_list[3]  # 依顺序获取，亦可用正则
    template_py = js2py.eval_js(template_js)
    # 将所有全局变量插入第一个函数变为局部变量并计算
    function1_js = 'function' + js_code_list[1]
    position = function1_js.index('{') + 1
    function1_js = function1_js[:position] + var_ + function1_js[position:]
    function1_py = js2py.eval_js(function1_js)
    cookie1 = function1_py(str(template_py))  # 结果类似'NA=='
    # 保存得到的第一个cookie
    cookies = {'wzwstemplate': cookie1}
    # 对第三个函数做类似操作
    function3_js = 'function' + js_code_list[3]
    position = function3_js.index('{') + 1
    function3_js = function3_js[:position] + var_ + function3_js[position:]
    function3_py = js2py.eval_js(function3_js)
    middle_var = function3_py()
    cookie2 = function1_py(middle_var)
    cookies['wzwschallenge'] = cookie2
    # 关于js代码中的document.cookie参见 https://developer.mozilla.org/zh-CN/docs/Web/API/Document/cookie
    dynamic_url = js2py.eval_js(var_list[0])

    # 利用新的cookie对提供的动态网址进行访问即是我们要达到的内容页面了
    r.cookies.update(cookies)
    rep3 = r.get(urljoin(url, dynamic_url), timeout=(10, 30))
    time.sleep(3)
    if not rep3:
        logger.error('网页请求错误 %s' % url)
        return
    return rep3


def local_pbc(prefix_url, origin, table_id, start, end):
    result_list = []

    # get real html content
    # get page count
    response = prepare(prefix_url + '1.html')
    logger.info(origin + '地址：%s' % (prefix_url + '1.html'))
    if response is None:
        logger.error('网页请求错误 %s' % (prefix_url + '1.html'))
        return
    soup = bs(response.content, 'lxml') if response else bs('', 'lxml')
    page_count_content = soup.find(id=table_id).find_all('table')[-1] if soup.find(id=table_id) else ''
    text = page_count_content.text if page_count_content != '' else ''
    page_count = int(re.findall(r'\d+', text)[1]) if text != '' else 0
    logger.info(origin + ' 一共有%d页' % page_count)

    # get crawler data
    for num in range(page_count):
        logger.info(origin + ' -- 第%d页' % (num + 1))
        url = prefix_url + str(num + 1) + '.html'

        try:
            content_response = prepare(url)
            if content_response is None:
                logger.error('网页请求错误 %s' % url)
                continue
            content_soup = bs(content_response.content, 'lxml') if content_response else bs('', 'lxml')
            table_content = content_soup.find(id=table_id).find_all('table')[int(start):int(end)] \
                if content_soup.find(id=table_id) else []
            logger.info(len(table_content))
            for each_table in table_content:
                try:
                    announcement_url = urljoin(url, each_table.find('a').attrs['href'].strip())
                    if db.pbc_data.find({'url': announcement_url}).count() == 0:
                        title = each_table.find('a').attrs['title'].strip()
                        publish_date = each_table.find_all('td')[-2].text.strip()
                        logger.info(origin + ' -- 新公告：' + announcement_url)
                        post = {
                            'title': title,
                            'publishDate': publish_date,
                            'url': announcement_url,
                            'type': '行政处罚决定',
                            'origin': origin,
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
        logger.info(origin + ' -- 一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.pbc_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info(origin + ' -- 公告导入完成！')
        else:
            logger.error(origin + ' -- 公告导入出现问题！')
    else:
        logger.info(origin + ' -- 没有新公告！')
    logger.info('\n')


pbc_info_list = [
    {
        'prefix_url': 'http://shanghai.pbc.gov.cn/fzhshanghai/113577/114832/114918/14681/index',
        'table_id': '14681',
        'origin': '人民银行上海分行',
        'start': 4,
        'end': -1
    },
    {
        'prefix_url': 'http://tianjin.pbc.gov.cn/fzhtianjin/113682/113700/113707/10983/index',
        'table_id': '10983',
        'origin': '人民银行天津分行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://shenyang.pbc.gov.cn/shenyfh/108074/108127/108208/8267/index',
        'table_id': '8267',
        'origin': '人民银行沈阳分行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://nanjing.pbc.gov.cn/nanjing/117542/117560/117567/12561/index',
        'table_id': '12561',
        'origin': '人民银行南京分行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://jinan.pbc.gov.cn/jinan/120967/120985/120994/13768/index',
        'table_id': '13768',
        'origin': '人民银行济南分行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://wuhan.pbc.gov.cn/wuhan/123472/123493/123502/16682/index',
        'table_id': '16682',
        'origin': '人民银行武汉分行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://guangzhou.pbc.gov.cn/guangzhou/129142/129159/129166/20713/index',
        'table_id': '20713',
        'origin': '人民银行广州分行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://chengdu.pbc.gov.cn/chengdu/129320/129341/129350/18154/index',
        'table_id': '18154',
        'origin': '人民银行成都分行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://xian.pbc.gov.cn/xian/129428/129449/129458/23967/index',
        'table_id': '23967',
        'origin': '人民银行西安分行',
        'start': 6,
        'end': -1
    },
    {
        'prefix_url': 'http://beijing.pbc.gov.cn/beijing/132030/132052/132059/19192/index',
        'table_id': '19192',
        'origin': '人民银行营业管理部（北京）',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://chongqing.pbc.gov.cn/chongqing/107680/107897/107909/8000/index',
        'table_id': '8000',
        'origin': '人民银行重庆营业管理部',
        'start': 0,
        'end': -1
    },
    {
        'prefix_url': 'http://shijiazhuang.pbc.gov.cn/shijiazhuang/131442/131463/131472/20016/index',
        'table_id': '20016',
        'origin': '人民银行石家庄中心支行',
        'start': 6,
        'end': -1
    },
    {
        'prefix_url': 'http://taiyuan.pbc.gov.cn/taiyuan/133960/133981/133988/20320/index',
        'table_id': '20320',
        'origin': '人民银行太原中心支行',
        'start': 6,
        'end': -1
    },
    {
        'prefix_url': 'http://huhehaote.pbc.gov.cn/huhehaote/129797/129815/129822/23932/index',
        'table_id': '23932',
        'origin': '人民银行呼和浩特中心支行',
        'start': 6,
        'end': -1
    },
    {
        'prefix_url': 'http://changchun.pbc.gov.cn/changchun/124680/124698/124705/16071/index',
        'table_id': '16071',
        'origin': '人民银行长春中心支行',
        'start': 4,
        'end': -1
    },
    {
        'prefix_url': 'http://haerbin.pbc.gov.cn/haerbin/112693/112776/112783/11181/index',
        'table_id': '11181',
        'origin': '人民银行哈尔滨中心支行',
        'start': 4,
        'end': -1
    },
    {
        'prefix_url': 'http://hangzhou.pbc.gov.cn/hangzhou/125268/125286/125293/16349/index',
        'table_id': '16349',
        'origin': '人民银行杭州中心支行',
        'start': 4,
        'end': -1
    },
    {
        'prefix_url': 'http://fuzhou.pbc.gov.cn/fuzhou/126805/126823/126830/17179/index',
        'table_id': '17179',
        'origin': '人民银行福州中心支行',
        'start': 4,
        'end': -1
    },
    {
        'prefix_url': 'http://hefei.pbc.gov.cn/hefei/122364/122382/122389/14535/index',
        'table_id': '14535',
        'origin': '人民银行合肥中心支行',
        'start': 4,
        'end': -1
    },
    {
        'prefix_url': 'http://zhengzhou.pbc.gov.cn/zhengzhou/124182/124200/124207/18390/index',
        'table_id': '18390',
        'origin': '人民银行郑州中心支行',
        'start': 4,
        'end': -1
    },
    {
        'prefix_url': 'http://changsha.pbc.gov.cn/changsha/130011/130029/130036/18625/index',
        'table_id': '18625',
        'origin': '人民银行长沙中心支行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://nanchang.pbc.gov.cn/nanchang/132372/132390/132397/19361/index',
        'table_id': '19361',
        'origin': '人民银行南昌中心支行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://nanning.pbc.gov.cn/nanning/133346/133364/133371/19833/index',
        'table_id': '19833',
        'origin': '人民银行南宁中心支行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://haikou.pbc.gov.cn/haikou/132982/133000/133007/19966/index',
        'table_id': '19966',
        'origin': '人民银行海口中心支行',
        'start': 6,
        'end': -1
    },
    {
        'prefix_url': 'http://kunming.pbc.gov.cn/kunming/133736/133760/133767/20429/index',
        'table_id': '20429',
        'origin': '人民银行昆明中心支行',
        'start': 6,
        'end': -1
    },
    {
        'prefix_url': 'http://guiyang.pbc.gov.cn/guiyang/113288/113306/113313/10855/index',
        'table_id': '10855',
        'origin': '人民银行贵阳中心支行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://lasa.pbc.gov.cn/lasa/120480/120504/120511/18819/index',
        'table_id': '18819',
        'origin': '人民银行拉萨中心支行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://lanzhou.pbc.gov.cn/lanzhou/117067/117091/117098/12820/index',
        'table_id': '12820',
        'origin': '人民银行兰州中心支行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://xining.pbc.gov.cn/xining/118239/118263/118270/13228/index',
        'table_id': '13228',
        'origin': '人民银行西宁中心支行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://yinchuan.pbc.gov.cn/yinchuan/119983/120001/120008/14095/index',
        'table_id': '14095',
        'origin': '人民银行银川中心支行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://wulumuqi.pbc.gov.cn/wulumuqi/121755/121777/121784/14752/index',
        'table_id': '14752',
        'origin': '人民银行乌鲁木齐中心支行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://shenzhen.pbc.gov.cn/shenzhen/122811/122833/122840/15142/index',
        'table_id': '15142',
        'origin': '人民银行深圳市中心支行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://dalian.pbc.gov.cn/dalian/123812/123830/123837/16262/index',
        'table_id': '16262',
        'origin': '人民银行大连市中心支行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://ningbo.pbc.gov.cn/ningbo/127076/127098/127105/17279/index',
        'table_id': '17279',
        'origin': '人民银行宁波市中心支行',
        'start': 6,
        'end': -1
    },
    {
        'prefix_url': 'http://qingdao.pbc.gov.cn/qingdao/126166/126184/126191/16720/index',
        'table_id': '16720',
        'origin': '人民银行青岛市中心支行',
        'start': 5,
        'end': -1
    },
    {
        'prefix_url': 'http://xiamen.pbc.gov.cn/xiamen/127703/127721/127728/18534/index',
        'table_id': '18534',
        'origin': '人民银行厦门市中心支行',
        'start': 6,
        'end': -1
    }
]


def local_pbc_crawler():
    for each_pbc_info in pbc_info_list:
        local_pbc(each_pbc_info['prefix_url'], each_pbc_info['origin'], each_pbc_info['table_id'],
                  each_pbc_info['start'], each_pbc_info['end'])


if __name__ == '__main__':
    local_pbc_crawler()
