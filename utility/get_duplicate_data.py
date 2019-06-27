import execjs
import re
import os
import subprocess
from urllib.parse import urljoin

import requests
import pymongo
from bs4 import BeautifulSoup as bs
from pymongo import MongoClient
from docx import Document
from xlrd import open_workbook, xldate_as_tuple
import pdfplumber
from init import logger_init, config_init
from utility import table_to_list, request_site_page, genHeader, format_date, get_content_text, format_text, \
    get_chinese_proportion
from pdf2html import pdf_ocr_to_table
from oss_utils import init_ali_oss, oss_add_file

logger = logger_init('获取重复数据')
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

ali_bucket = init_ali_oss()

for each_csrc_announcement in db.announcement.find({'announcementOrg': {'$regex': '.*人民银行.*'}}, no_cursor_timeout=True):
    id_list = []
    url_id_list = []
    url_list = []
    if db.announcement.find({'announcementDate': each_csrc_announcement['announcementDate'],
                             'announcementTitle': each_csrc_announcement['announcementTitle'],
                             'litigant': each_csrc_announcement['litigant'],
                             'announcementCode': each_csrc_announcement['announcementCode']}).count() > 1:
        # id_list.append(each_csrc_announcement['_id'])
        for kk in db.announcement.find({'announcementDate': each_csrc_announcement['announcementDate'],
                                        'announcementTitle': each_csrc_announcement['announcementTitle'],
                                        'litigant': each_csrc_announcement['litigant'],
                                        'announcementCode': each_csrc_announcement['announcementCode']}):
            id_list.append(kk['_id'])
            if kk['oss_file_id'] != '':
                url_id_list.append(str(db.parsed_data.find_one({'_id': kk['oss_file_id']})['_id']))
                url_list.append(db.parsed_data.find_one({'_id': kk['oss_file_id']})['origin_url'])
            else:
                url_id_list.append('')
                url_list.append('')
        if len(set(url_list)) > 1 and \
                'http://guangzhou.pbc.gov.cn/guangzhou/129142/129159/129166/3761531/index.html' in url_list:
            print(id_list)
            print(url_id_list)
            print(url_list)
            print('\n')
