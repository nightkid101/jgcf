import re
import math
import time
import chardet
import random
from pyquery import PyQuery

from urllib.parse import urlencode
import requests
from init import config_init, logger_init
from getProxy import genHeader, getOneProxy, errorPageOrNot
from collections import defaultdict

config = config_init()
logger = logger_init('utility')
proxiesLastUsed = [[], []]  # record the usage of proxies
lastRequestTime = 0  # record the timestamp of web request
re_type = type(re.compile(''))

UTIL_CN_NUM = {
    '零': 0,
    '一': 1,
    '二': 2,
    '两': 2,
    '三': 3,
    '四': 4,
    '五': 5,
    '六': 6,
    '七': 7,
    '八': 8,
    '九': 9,
    '〇': 0,
    '○': 0,
    'Ｏ': 0,
    'O': 0,
    'Ο': 0,
    'О': 0
}
UTIL_CN_UNIT = {
    '十': 10,
    '百': 100,
    '千': 1000,
    '万': 10000,
}


def enc_char_detection(content, res=None):
    msg = res.url if res else content
    enc = {'encoding': 'utf-8'}
    try:
        enc = chardet.detect(content)
        if enc['confidence'] <= 0.8:
            logger.warning('low confidence of char detection: {}'.format(msg))
        enc['encoding'] = 'utf-8' if not enc['encoding'] else enc['encoding']
    except:
        logger.error('fail to detect char encoding: {}'.format(msg))
    return enc['encoding']


def get_proxy_unused(sleep_time=int(config['crawler_config']['sleep_time']), verify_info=None, proxy_type='http'):
    """Get proxy with last used time span larger than `sleepTime`
    """
    global proxiesLastUsed
    proxy = getOneProxy(verify_info=verify_info, proxy_type=proxy_type)
    while True:
        if proxy not in proxiesLastUsed[0]:
            proxiesLastUsed[0].append(proxy)
            proxiesLastUsed[1].append(time.time())
            return proxy
        else:
            idx = proxiesLastUsed[0].index(proxy)
            if math.floor(time.time() - proxiesLastUsed[1][idx]) > sleep_time:
                proxiesLastUsed[1][idx] = time.time()
                return proxy
            else:
                proxy = getOneProxy(verify_info=verify_info, proxy_type=proxy_type)

def request_site_page(url, params=None, methods='get', trust_environment=False,
                      timeout=(int(config['crawler_config']['crawler_timeout_first']),
                               int(config['crawler_config']['crawler_timeout_second'])),
                      headers=None, retry_max=20, source=None, use_proxy=False, **kwargs):
    """ Request webSite Page and test with wait time
    params: request params
    method: 'get' or 'post'
    """
    req_interval = int(config['crawler_config']['crawler_req_interval_min']) + (
            int(config['crawler_config']['crawler_req_interval_max']) -
            int(config['crawler_config']['crawler_req_interval_min'])) * random.random()

    header = genHeader()
    if headers:
        header.update(headers)

    last_request_time = retry_total = retry_trigger = 0
    url_print = url + '?' + urlencode(params) if params is not None else url
    while True:
        during_time = time.time() - last_request_time
        if last_request_time != 0 and during_time < req_interval:
            logger.info('sleep %d s' % (req_interval - during_time))
            time.sleep(req_interval - during_time)
        if methods == 'get':
            try:
                with requests.Session() as s:
                    s.trust_env = False
                    response_return = s.get(url, params=params, headers=header, timeout=timeout,
                                            **kwargs)
            except Exception as e:
                logger.warning(e)
                response_return = None
        elif methods == 'post':
            try:
                with requests.Session() as s:
                    s.trust_env = False
                    response_return = s.post(url, params=params, headers=header, timeout=timeout,
                                             **kwargs)
            except Exception as e:
                logger.warning(e)
                response_return = None
        else:
            logger.error('requestSitePage wrong methods')
            return None
        last_request_time = time.time()
        if response_return is not None:
            if response_return.status_code == 200:
                return response_return
            else:
                if response_return.status_code == 404:
                    return response_return
                logger.warning('网页返回状态不为 200: ' + url_print)
                logger.warning('直接访问')
                retry_trigger += 1
                retry_total += 1
        else:
            if response_return is None:
                logger.warning('网页返回 None: ' + url_print)
                logger.warning('直接访问')
            retry_trigger += 1
            retry_total += 1
        if retry_total <= retry_max:
            if retry_trigger >= 3:
                retry_trigger = 0
        else:
            logger.warning('too many direct retry operations!')
            return None
# def request_site_page(url, params=None, methods='get',
#                       timeout=(int(config['crawler_config']['crawler_timeout_first']),
#                                int(config['crawler_config']['crawler_timeout_second'])),
#                       headers=None, retry_max=2, verify_info=None, proxy_type='http',
#                       use_proxy=False, **kwargs):
#     """ Request webSite Page and test with wait time
#     params: request params
#     method: 'get' or 'post'
#     """
#     req_interval = int(config['crawler_config']['crawler_req_interval_min']) + (
#             int(config['crawler_config']['crawler_req_interval_max']) -
#             int(config['crawler_config']['crawler_req_interval_min'])) * random.random()
#
#     if use_proxy:
#         proxy = get_proxy_unused(verify_info=verify_info, proxy_type=proxy_type)
#         header = genHeader()
#         if headers:
#             header.update(headers)
#
#         last_request_time = retry_total = retry_trigger = 0
#         url_print = url + '?' + urlencode(params) if params is not None else url
#         while True:
#             logger.info('Using proxy ' + str(proxy))
#             logger.info('此Proxy第%d次' % (retry_trigger + 1))
#             logger.info('一共 第%d次' % (retry_total + 1))
#             during_time = time.time() - last_request_time
#             if last_request_time != 0 and during_time < req_interval:
#                 time.sleep(req_interval - during_time)
#             if methods == 'get':
#                 try:
#                     with requests.Session() as s:
#                         s.trust_env = False
#                         response_return = s.get(url, params=params, proxies=proxy, headers=header, timeout=timeout,
#                                                 **kwargs)
#                 except Exception as e:
#                     logger.warning(e)
#                     response_return = None
#             elif methods == 'post':
#                 try:
#                     with requests.Session() as s:
#                         s.trust_env = False
#                         response_return = s.post(url, params=params, proxies=proxy, headers=header, timeout=timeout,
#                                                  **kwargs)
#                 except Exception as e:
#                     logger.warning(e)
#                     response_return = None
#             else:
#                 logger.error('requestSitePage wrong methods')
#                 return None
#             last_request_time = time.time()
#             if response_return is not None:
#                 if response_return.status_code == 200:
#                     try:
#                         encoding = enc_char_detection(response_return.content, res=response_return)
#                         encoding = 'big5hkscs' if encoding == 'big5' else encoding
#                         if errorPageOrNot(response_return.content.decode(encoding, 'ignore')):
#                             return response_return
#                         else:
#                             logger.warning('AD PAGE FOUND: ' + str(response_return.content.decode()))
#                             logger.warning('发现广告页面: ' + url_print)
#                             retry_total += 1
#                             proxy = get_proxy_unused(verify_info=verify_info, proxy_type=proxy_type) if proxy else None
#                     except Exception as e:
#                         logger.warning(e)
#                         return response_return
#                 else:
#                     if response_return.status_code == 404:
#                         return response_return
#                     logger.warning('网页返回状态不为 200: ' + url_print)
#                     retry_trigger += 1
#                     retry_total += 1
#             else:
#                 if response_return is None:
#                     logger.warning('网页返回 None: ' + url_print)
#                 retry_trigger += 1
#                 retry_total += 1
#             if proxy is None:
#                 logger.error('Cannot reach url: ' + url_print)
#                 return None
#             if retry_total <= retry_max:
#                 if retry_trigger >= 3:
#                     logger.warning('Proxy: ' + str(proxy))
#                     proxy = get_proxy_unused(verify_info=verify_info, proxy_type=proxy_type)
#                     retry_trigger = 0
#             else:
#                 logger.warning('too many proxy retry operations!')
#                 return None
#                 # logger.warning('trying direct')
#                 # proxy = None
#     else:
#         header = genHeader()
#         if headers:
#             header.update(headers)
#
#         last_request_time = retry_total = retry_trigger = 0
#         url_print = url + '?' + urlencode(params) if params is not None else url
#         while True:
#             during_time = time.time() - last_request_time
#             if last_request_time != 0 and during_time < req_interval:
#                 time.sleep(req_interval - during_time)
#             if methods == 'get':
#                 try:
#                     with requests.Session() as s:
#                         s.trust_env = False
#                         response_return = s.get(url, params=params, headers=header, timeout=timeout,
#                                                 **kwargs)
#                 except Exception as e:
#                     logger.warning(e)
#                     response_return = None
#             elif methods == 'post':
#                 try:
#                     with requests.Session() as s:
#                         s.trust_env = False
#                         response_return = s.post(url, params=params, headers=header, timeout=timeout,
#                                                  **kwargs)
#                 except Exception as e:
#                     logger.warning(e)
#                     response_return = None
#             else:
#                 logger.error('requestSitePage wrong methods')
#                 return None
#             last_request_time = time.time()
#             if response_return is not None:
#                 if response_return.status_code == 200:
#                     try:
#                         encoding = enc_char_detection(response_return.content, res=response_return)
#                         encoding = 'big5hkscs' if encoding == 'big5' else encoding
#                         if errorPageOrNot(response_return.content.decode(encoding, 'ignore')):
#                             return response_return
#                         else:
#                             logger.warning('AD PAGE FOUND: ' + str(response_return.content.decode()))
#                             logger.warning('发现广告页面: ' + url_print)
#                             retry_total += 1
#                     except Exception:
#                         return response_return
#                 else:
#                     if response_return.status_code == 404:
#                         return response_return
#                     logger.warning('网页返回状态不为 200: ' + url_print)
#                     logger.warning('直接访问')
#                     retry_trigger += 1
#                     retry_total += 1
#             else:
#                 if response_return is None:
#                     logger.warning('网页返回 None: ' + url_print)
#                     logger.warning('直接访问')
#                 retry_trigger += 1
#                 retry_total += 1
#             if retry_total <= retry_max:
#                 if retry_trigger >= 3:
#                     retry_trigger = 0
#             else:
#                 logger.warning('too many direct retry operations!')
#                 return None


# 中文一二三转为数字
def cn2dig(src):
    if src == "":
        return None
    m = re.match(r"\d+", src)
    if m:
        return m.group(0)
    rsl = 0
    unit = 1
    for item in src[::-1]:
        if item in UTIL_CN_UNIT.keys():
            unit = UTIL_CN_UNIT[item]
        elif item in UTIL_CN_NUM.keys():
            num = UTIL_CN_NUM[item]
            rsl += num * unit
        else:
            return None
    if rsl < unit:
        rsl += unit
    return str(rsl)


# 获取年份
def get_year(src):
    result = ''
    for each in src:
        if each in UTIL_CN_NUM.keys():
            result += str(UTIL_CN_NUM[each])
        else:
            result += each
    return result


# remove \n \r \r\n \t 空格 \xa0
def remove_strip(input_string):
    return input_string.replace('\n', '').replace('\r', '').replace('\r\n', '').replace(' ', '') \
        .replace('\t', '').replace('\xa0', '').replace('\u3000', '').replace(' ', '').strip()


# 格式化日期 全部转化为2018年12月11日这种形式
def format_date(date_string):
    try:
        date_string = remove_strip(date_string).replace('号', '日').strip()
        date_int_list = re.split(r'年|月|日|-|\.|/', date_string)
        year_string = get_year(date_int_list[0])
        if 1973 < int(year_string) < 2100:
            year_int = int(year_string)
        else:
            return 'Bad date'
        month_string = cn2dig(date_int_list[1])
        if 0 < int(month_string) < 13:
            month_int = int(month_string)
        else:
            return 'Bad date'
        day_string = cn2dig(date_int_list[2])
        if 0 < int(day_string) < 32:
            day_int = int(day_string)
        else:
            return 'Bad date'
        return str(year_int) + '年' + str(month_int) + '月' + str(day_int) + '日'
    except:
        return 'Bad date'


# 以下三个方法针对表格处理，主要是针对合并单元格做了一些操作，得到一个list
def table_to_list(table):
    dct = table_to_2d_dict(table)
    table_list = []
    for each_row_list in list(iter_2d_dict(dct)):
        if len([i for i in each_row_list if i != '']) > 0:
            table_list.append(each_row_list)
    return table_list


def table_to_2d_dict(table):
    result = defaultdict(lambda: defaultdict())
    for row_i, row in enumerate([each_tr for each_tr in table.find_all('tr')]):
        for col_i, col in enumerate(row.find_all('td')):
            colspan = int(col.attrs.get('colspan', 1))
            rowspan = int(col.attrs.get('rowspan', 1))
            col_data = col.text
            while row_i in result and col_i in result[row_i]:
                col_i += 1
            for i in range(row_i, row_i + rowspan):
                for j in range(col_i, col_i + colspan):
                    result[i][j] = format_text(col_data)
    return result


def iter_2d_dict(dct):
    for i, row in sorted(dct.items()):
        cols = []
        for j, col in sorted(row.items()):
            cols.append(col)
        yield cols


# 去除特殊字符
def remove_special_char(string):
    special_char_list = ['\xa0', '\u3000', ' ']
    if string in special_char_list:
        return ''
    else:
        for each_special_char in special_char_list:
            string = string.replace(each_special_char, '')
        if string.strip() == '':
            return ''
        else:
            return string.strip()


# 按照p标签获取网页text
def get_content_text(content):
    # content = [s.extract() for s in content('style')]
    content_text = PyQuery(str(content)).text()
    content_text = content_text.replace('\r\n', '\n').replace('\r', '\n')
    final_content_text = ''
    for each_text in content_text.split('\n'):
        each_final_text = remove_special_char(each_text).strip()
        if each_final_text != '':
            final_content_text += each_final_text + '\n'
    return final_content_text.strip()


def format_text(string):
    return '\n'.join(
        [remove_special_char(kk).strip() for kk in string.replace('\r\n', '\n').replace('\r', '\n').split('\n') if
         remove_special_char(kk).strip() != ''])


def get_chinese_proportion(string):
    if string == '':
        return 0, 0
    chinese_str_count = 0
    for each_string in string:
        if u'\u4e00' <= each_string <= u'\u9fff':
            chinese_str_count += 1
    return chinese_str_count / len(string), len(string)
