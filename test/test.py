from pymongo import MongoClient
import re
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from docx import Document
from xlrd import open_workbook, xldate_as_tuple
from init import logger_init, config_init
from utility import format_date, remove_special_char, request_site_page, get_content_text
from oss_utils import init_ali_oss, oss_add_file
import subprocess
from pdf2html import pdf_ocr_to_text
import xlrd

def test(announcement_url='http://sthjt.shanxi.gov.cn/html/xzcfjd/20170227/54071.html'):
    r = request_site_page(announcement_url)
    r.encoding = r.apparent_encoding
    content_soup = BeautifulSoup(r.text, 'lxml')
    if announcement_url == 'http://sthjt.shanxi.gov.cn/html/xzcfjd/20170614/54089.html' \
            or announcement_url == 'http://sthjt.shanxi.gov.cn/html/xzcfjd/20170227/54071.html':
        content_text_list = content_soup.find('div', class_='td-con').find_all('tr')[2:]
        i = 0
        while i<len(content_text_list):
            if ('季度' in content_text_list[i].text) or ('企业名称' in content_text_list[i].text):
                del (content_text_list[i])
            else:
                i = i + 1
        result_map_list = []
        for content_text in content_text_list:
            context = content_text.find_all('td')
            # 处罚机构
            announcement_org = context[3].text
            # 处罚日期
            # real_publish_date = format_date(each_document['publishDate'].split(' ')[0])
            # 文号
            announcement_code = context[4].text
            # 当事人
            litigant = context[1].text
            # 违规事实
            facts = '超标率： ' + context[2].text
            # 认定意见
            punishment_basis = ''
            # 申辩意见
            defenseOpinion = ''

            # 申辩意见反馈
            defenseResponse = ''

            # 处罚决定
            punishment_decision = context[5].text




if __name__ == '__main__':
    test('http://sthjt.shanxi.gov.cn/html/xzcfjd/20170227/54071.html')