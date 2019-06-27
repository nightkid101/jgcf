import re
import jsbeautifier
import js2py

from pymongo import MongoClient
from init import logger_init, config_init
import requests
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin

logger = logger_init('中国人民银行-数据抓取')
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


# 中国人民银行
def pbc():
    host_url = 'http://www.pbc.gov.cn'
    url = 'http://www.pbc.gov.cn/zhengwugongkai/127924/128041/2161421/index.html'
    # 利用session保存cookie信息，第一次请求会设置cookie类似
    # {'wzwsconfirm': 'ab3039756ba3ee041f7e68f634d28882', 'wzwsvtime': '1488938461'}，
    # 与js解析得到的cookie合起来才能通过验证
    r = requests.session()
    content = r.get(url).content
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
    rep3 = r.get(host_url + dynamic_url)
    if not rep3:
        logger.error('网页请求错误 %s' % url)
        return
    soup_req3 = bs(rep3.content, 'lxml') if rep3 else bs('', 'lxml')
    # get page count
    page_count_content = soup_req3.find(class_='Normal')
    text = page_count_content.text if page_count_content != '' else ''
    page_count = int(re.findall(r'\d+', text)[1]) if len(re.findall(r'\d+', text)) > 1 else 0
    logger.info('中国人民银行' + ' 一共有%d页' % page_count)

    # get crawler data
    result_list = []
    div_content = soup_req3.find(id='zwgk_rlist')

    if not div_content:
        logger.error('网页请求错误 %s' % url)
        return

    for each_table in div_content.find_all('table')[1:-1]:
        try:
            announcement_url = urljoin(url, each_table.find('a').attrs['href'].strip())
            if db.pbc_data.find({'url': announcement_url}).count() == 0:
                title = each_table.find('a').attrs['title'].strip()
                publish_date = each_table.find_all('td')[-2].text.strip()
                logger.info('中国人民银行 -- 新公告：' + announcement_url)
                post = {
                    'title': title,
                    'publishDate': publish_date,
                    'url': announcement_url,
                    'type': '行政处罚决定',
                    'origin': '人民银行',
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

    if len(result_list) > 0:
        logger.info('人民银行一共有%d条新公告，导入数据库中......' % len(result_list))
        r = db.pbc_data.insert_many(result_list)
        if len(r.inserted_ids) == len(result_list):
            logger.info('人民银行公告导入完成！')
        else:
            logger.error('人民银行公告导入出现问题！')
    else:
        logger.info('人民银行没有新公告！')


if __name__ == "__main__":
    pbc()
