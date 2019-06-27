import re
import requests
import random
import urllib3
import argparse
import time
import sys
from lxml import etree
import concurrent.futures as cf
import multiprocessing as mp
from bs4 import BeautifulSoup as bs
import pymongo
import schedule
from init import config_init

config = config_init()

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

###########################################################################
############################ global Variables #############################
pageStart5 = pageStart4 = pageStart3 = pageStart2 = pageStart1 = 1  # current page number
pageNum = 50  # number of pages to be processed once a time
timeout = 3  # valid proxy test timeout param in seconds
remoteTrigger = 0
sleepTime = 5
maxProxySize = 50000
testUrl = 'http://www.baidu.com/duty/index.html'
validString = 'baidu'
sTestUrl = 'https://www.baidu.com/duty/index.html'
sValidString = 'baidu'
scanningPorts = [80, 1080, 8080, 3128, 8081]
proxySize = 0


mongo_client = pymongo.MongoClient(config['mongodb']['ali_mongodb_url'], username=config['mongodb']['ali_mongodb_username'], password=config['mongodb']['ali_mongodb_password'], port=int(config['mongodb']['ali_mongodb_port']))
collProxy = mongo_client['touzhiwang']['proxypool']


def getOneProxy(verify_info=None, proxy_type=None):
    pt = proxy_type if proxy_type in ['http', 'https'] else 'http'
    query = {pt: {"$exists": 1}}
    while True:
        try:
            proxy_count = collProxy.count(query)
            if not proxy_count: return {}
            proxy = collProxy.find(query, {'_id': 0}).skip(random.randrange(proxy_count)).limit(1).next()
            proxyStatus, _ = validUsefulProxy(proxy, verify_info=verify_info)
            if proxyStatus:
                if proxy_type is None:
                    p = getOneProxy(verify_info=verify_info, proxy_type='https')
                    proxy.update(p)
                return proxy
        except:
            return {}


def verifyProxyFormat(proxy):
    """
    :param proxy:
    :return:
    """
    verify_regex = r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d{1,5}"
    return True if re.findall(verify_regex, proxy) else False


def errorPageOrNot(text):
    """
    检测广告代理以及运营商劫持
    return True if not ad page..
    """
    if 'null({"baseinfo' in text:  # special for neeq company info
        return True
    if not re.findall(r"那家网", text) and text != 'NO' and \
            not re.findall(r"{\"rtn\":", text) and not re.findall(r"大数据操作系统", text) and \
            not re.findall(r"针对点一点扫一扫", text) and not re.findall(r"惠惠助手", text) and \
            not re.findall(r"The requested URL could not be retrieved", text) and text != '^@' and \
            not re.findall(r"无效用户", text) and not re.findall(r"禁止外部用户", text) and \
            not re.findall(r"Unauthorized", text) and not re.findall(r"推猫多品营销系统", text) and \
            not re.findall(r"Authorization", text) and not re.findall(r"迅格云视频", text) and \
            not re.findall(r"系统异常", text) and not re.findall(r"Page Not found", text) and \
            not re.findall(r"无法访问", text) and not re.findall(r"网易有道", text) and \
            not re.findall(r"错误页面", text):
        return True
    return False


def user_agent():
    """
    return an User-Agent at random
    :return:
    """
    ua_list = [
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95',
        'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71',
        'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)',
        'Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50',
        'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0',
    ]
    return random.choice(ua_list)


def getHeader():
    """
    basic header
    :return:
    """
    return {'User-Agent': user_agent(),
            'Accept': '*/*',
            'Connection': 'keep-alive',
            'Accept-Language': 'zh-CN,zh;q=0.8'}


def genHeader():
    header = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_3) AppleWebKit/537.36 (KHTML, like Gecko)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch',
        'Accept-Language': 'zh-CN,zh;q=0.8',
    }
    headers = getHeader()
    headers.update(header)
    return headers


def validUsefulProxy(p, verify_info=None):
    """
    :param p:
    :param verify_info:
    :return:
    """
    pt = False
    if isinstance(p, dict):
        if 'http' in p:
            pt = 'http'
        elif 'https' in p:
            pt = 'https'

    if verify_info:
        verify_url, verify_string = verify_info
        proxy_type = verify_url.split('://')[0]
        if pt and proxy_type != pt:
            return False, None
    else:
        if pt:
            verify_url = testUrl if pt == 'http' else sTestUrl
            verify_string = validString
            proxy_type = pt
        else:
            verify_url, verify_string = testUrl, validString
            proxy_type = verify_url.split('://')[0]

    headers = genHeader()

    try:
        pProxies = {proxy_type: "{}://{}".format(proxy_type, p)} if not isinstance(p, dict) and p is not None else p
        with requests.Session() as s:
            s.trust_env = False
            r = s.get(verify_url, proxies=pProxies, headers=headers, timeout=timeout, verify=False)
        if r.status_code == 200:
            if errorPageOrNot(r.content.decode()):
                if not re.findall(verify_string, r.content.decode()):
                    print(r.content.decode())
                else:
                    return True, pProxies
    except:
        pass

    return False, None


def getHtmlTree(url, **kwargs):
    """
    :param url:
    :param kwargs:
    :return:
    """
    headers = genHeader()
    try:
        with requests.Session() as s:
            s.trust_env = False
            html = s.get(url, headers=headers, timeout=timeout)
        return etree.HTML(html.content)
    except:
        return etree.HTML('')


def getHtmlSoup(url, **kwargs):
    """
    :param url:
    :param kwargs:
    :return:
    """
    headers = genHeader()
    try:
        with requests.Session() as s:
            s.trust_env = False
            html = s.get(url, headers=headers, timeout=timeout)
        soup = bs(html.content, 'lxml')
        return soup
    except:
        return bs('', 'lxml')


def checkAndAddProxy(proxy, verify_info=None):
    """
    Check if the proxy is valid if so add to db
    :proxy string format should be like '17.23.198.1'
    """
    if proxySize <= maxProxySize:
        if verifyProxyFormat(proxy):
            rePattern = re.compile(re.escape(proxy.split(':')[0]))
            query = {"$or": [{"https": rePattern}, {"http": rePattern}]}
            if not collProxy.find_one(query):
                statusCode, proxies = validUsefulProxy(proxy)
                if statusCode:
                    print("{proxies} is ok".format(proxies=proxies))
                    _ = collProxy.insert_one(proxies)
                if not verify_info:
                    checkAndAddProxy(proxy, verify_info=(sTestUrl, sValidString))
                    return True
    return False


def checkProxy(proxy):
    """
    Check of proxy in db is valid
    :proxy dict should be like '{"_id": id, "http": "http://17.23.198.1:80"}'
    """
    try:
        dbID = proxy.pop('_id')
        statusCode, proxies = validUsefulProxy(proxy)
        if statusCode:
            return True
        collProxy.delete_one({'_id': dbID})
    except:
        pass
    return False


def freeProxy1():
    """
    :return proxy list generator:
    """
    global pageStart1
    url_list = [
        'http://www.xicidaili.com/nn',
        'http://www.xicidaili.com/nt',
        'http://www.xicidaili.com/wn',
        'http://www.xicidaili.com/wt',
    ]
    for each_url in url_list:
        try:
            tree = getHtmlTree(each_url)
            proxy_list = tree.xpath('.//table[@id="ip_list"]//tr') if tree != '' else []
            for proxy in proxy_list:
                yield ':'.join(proxy.xpath('./td/text()')[0:2])
        except:
            pass
    pageStart1 = -1


def freeProxy2():
    """
    :param page:
    :return:
    """
    global pageStart2
    url_list = [
        'http://www.data5u.com/',
        'http://www.data5u.com/free/index.shtml',
        'http://www.data5u.com/free/gngn/index.shtml',
        'http://www.data5u.com/free/gnpt/index.shtml',
    ]
    for url in url_list:
        try:
            html_tree = getHtmlTree(url)
            ul_list = html_tree.xpath('//ul[@class="l2"]') if html_tree != '' else []
            for ul in ul_list:
                yield ':'.join(ul.xpath('.//li/text()')[0:2])
        except:
            pass
    pageStart2 = -1


def freeProxy3():
    """
    :param page:
    :return:
    """
    global pageStart3
    url = "http://www.goubanjia.com/free/index{page}.shtml"
    for page in range(pageStart3, pageStart3 + pageNum):
        page_url = url.format(page=page)
        soup = getHtmlSoup(page_url)
        for i in soup.findAll('td', attrs={'class': 'ip'}):
            ip = ''
            for span in i.children:
                try:
                    if 'style' in span.attrs and 'none' in span['style']:
                        continue
                    ip = ip + span.text
                except:
                    try:
                        ip = ip + span.text
                    except:
                        ip = ip + span
            yield ip
    pageStart3 += pageNum
    if pageStart3 > 90:
        pageStart3 = -1


def freeProxy4():
    """
    :param page:
    :return:
    """
    url = "http://www.ip181.com/daili/{page}.html"
    global pageStart4
    pageNum = 10  # 50 per page

    soup = getHtmlSoup('http://www.ip181.com/')
    for i in soup.findAll('tr', attrs={"class": "warning"}):
        ip = i.findAll('td')[0].text + ':' + i.findAll('td')[1].text
        yield ip

    for page in range(pageStart4, pageStart4 + pageNum):
        page_url = url.format(page=page)
        soup = getHtmlSoup(page_url)
        for i in soup.findAll('tr', attrs={"class": "warning"}):
            ip = i.findAll('td')[0].text + ':' + i.findAll('td')[1].text
            yield ip
    pageStart4 += pageNum
    if pageStart4 > 100:
        pageStart4 = -1


def freeProxy5():
    """
    :param page:
    :return:
    """
    baseUrl = "http://www.ip3366.net/?stype={type}&page={page}"
    global pageStart5
    url_list = []
    for stype in range(1, 5):
        for page in range(1, 11):
            url_list.append(baseUrl.format(type=stype, page=page))
    for url in url_list:
        try:
            tree = getHtmlTree(url)
            plist = tree.xpath('.//table/tbody//tr') if tree != '' else []
            for el in plist:
                yield ":".join(el.xpath('.//td/text()')[0:2])
            time.sleep(2 + random.random(5))
        except:
            pass
    pageStart5 = -1


# http://www.proxy360.cn/default.aspx


def fetchProxy():
    print('Adding new proxies...')
    global pageStart1, pageStart2, pageStart3, pageStart4, pageStart5
    while True:
        if (proxySize) >= 40000:
            print('ProxyPool oversize and should be cleaned.')
            break
        if pageStart1 != -1:
            print('proxy-1:')
            with cf.ThreadPoolExecutor() as executor:
                result = executor.map(checkAndAddProxy, freeProxy1())
            time.sleep(5)
        if pageStart2 != -1:
            print('proxy-2:')
            with cf.ThreadPoolExecutor() as executor:
                result = executor.map(checkAndAddProxy, freeProxy2())
            time.sleep(5)
        if pageStart4 != -1:
            print('proxy-4:')
            with cf.ThreadPoolExecutor() as executor:
                result = executor.map(checkAndAddProxy, freeProxy4())
            time.sleep(5)
        if pageStart3 != -1:
            print('proxy-3:')
            with cf.ThreadPoolExecutor() as executor:
                result = executor.map(checkAndAddProxy, freeProxy3())
            time.sleep(5)
        if pageStart5 != -1:
            print('proxy-5:')
            with cf.ThreadPoolExecutor() as executor:
                result = executor.map(checkAndAddProxy, freeProxy5())
            time.sleep(5)
        if {pageStart1, pageStart2, pageStart3, pageStart4, pageStart5} == set({-1}):
            break
    pageStart5 = pageStart4 = pageStart3 = pageStart2 = pageStart1 = 1
    print('Proxy new list done!')


def cleanProxy():
    global proxySize
    print('Cleaning up previous run...')
    proxylist = list(collProxy.find({'_id': {"$ne": '1'}}))
    print('Existing proxylist with size: {size}'.format(size=len(proxylist)))
    with cf.ThreadPoolExecutor() as executor:
        executor.map(checkProxy, proxylist)
    proxySize = len(list(collProxy.find({})))
    print('Valid proxylist with size: {size}'.format(size=proxySize))


def checkProxySize():
    global proxySize
    proxySize = collProxy.count({})
    if proxySize >= maxProxySize:
        print('ProxyPool oversize and should be cleaned.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--clean', action='store_true', help="clean up previous run.")
    parser.add_argument('-n', '--new', action='store_true', help='get new proxies.')
    parser.add_argument('-s', '--scan', action='store_true', help='scan db reconstruct for new proxy.')
    parser.add_argument('web', nargs='?', help='valid string to be tested, like baidu, cninfo, csrc, cm, sse, szse...')
    args = parser.parse_args()

    if len(sys.argv) == 1 or args.web is None:
        opt = 'baidu'
    elif args.web not in ['baidu', 'cninfo', 'csrc', 'cm']:
        opt = 'baidu'
    else:
        opt = args.web
    if opt == 'baidu':
        testUrl = 'http://www.baidu.com/duty/index.html'
        validString = 'baidu'
    elif opt == 'cninfo':
        testUrl = 'http://three.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice-emergent'
        validString = 'cninfo'
    elif opt == 'csrc':
        testUrl = 'http://www.csrc.gov.cn/pub/newsite/fzlm/gywm/'
        validString = 'csrc'
    elif opt == 'cm':
        testUrl = 'http://www.chinamoney.com.cn/chinese/legaldeclaration/'
        validString = 'chinamoney'
    elif opt == 'neeq':
        testUrl = 'http://www.neeq.com.cn/company/introduce.html'
        validString = 'neeq'

    print('Initializing Proxy MongoDB..')

    if args.clean:
        cleanProxy()
        schedule.every(12).hours.do(cleanProxy)

    if args.new:
        fetchProxy()
        schedule.every(30).minutes.do(fetchProxy)

    schedule.every(10).minutes.do(checkProxySize)

    print('Now scheduling..')
    while True:
        schedule.run_pending()
        time.sleep(1)
