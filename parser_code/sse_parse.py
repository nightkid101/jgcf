from pymongo import MongoClient
import re
import os
import sys
import subprocess
import docx

from init import logger_init, config_init
from utility import cn2dig, get_year, request_site_page, get_content_text, format_date
from oss_utils import init_ali_oss, oss_add_file
from pdf2html import pdf_to_text
from bs4 import BeautifulSoup as bs

logger = logger_init('上交所 数据解析')
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


def parse_sse():
    for document in db.sse_data.find({'status': {'$nin': ['ignored']}}, no_cursor_timeout=True):
        try:
            announcement_url = document['url']
            announcement_title = document['title']
            origin_url_id = document['_id']
            announcement_type = document['type']

            if db.sse_data.find(
                    {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1 and \
                    db.announcement.find({'oss_file_id': db.parsed_data.find(
                        {'origin_url_id': origin_url_id})[0][
                        '_id']}).count() > 0:
                continue

            logger.info('Url to parse: ' + announcement_url)

            if document['url'].endswith('shtml'):
                response = request_site_page(announcement_url, use_proxy=True)
                if response is None:
                    logger.error('网页请求错误')
                    continue
                content_soup = bs(response.text.encode(response.encoding).decode('utf-8'), 'lxml')
                content_text = get_content_text(content_soup.find(class_='allZoom'))
                if db.parsed_data.find(
                        {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                    oss_file_map = {
                        'origin_url': announcement_url,
                        'oss_file_origin_url': announcement_url,
                        'origin_url_id': origin_url_id,
                        'oss_file_type': 'shtml',
                        'oss_file_name': announcement_title,
                        'oss_file_content': response.text.encode(response.encoding).decode('utf-8'),
                        'parsed': False
                    }
                    insert_response = db.parsed_data.insert_one(oss_file_map)
                    file_id = insert_response.inserted_id
                    oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.shtml',
                                 response.text.encode(response.encoding).decode('utf-8'))
                    db.sse_data.update_one({'_id': origin_url_id}, {'$set': {'status': 'parsed'}})
                else:
                    db.sse_data.update_one({'_id': origin_url_id}, {'$set': {'status': 'parsed'}})
                    file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                       'oss_file_origin_url': announcement_url})['_id']

                if re.search(r'(上证债监.\d{4}.\d+号)', content_text):
                    announcement_code = re.search(r'(上证债监.\d{4}.\d+号)', content_text).group(1).strip()
                else:
                    announcement_code = ''
                announcement_code = announcement_code.replace('─', '').replace('\n', '').strip()
                if re.search(r'(当事人[\s\S]*?)(经查|经核实|\n.*?股份有限公司.*?披露|\n甘肃莫高实业发展股份有限公司)', content_text):
                    litigant = re.search(r'(当事人[\s\S]*?)(经查|经核实|\n.*?股份有限公司.*?披露|\n甘肃莫高实业发展股份有限公司)',
                                         content_text).group(
                        1).strip()
                else:
                    litigant = re.search(announcement_code + r'\n([\s\S]*?)' + r'(经查|你单位作为中体产业集团股份有限公司|'
                                                                               r'2013年5月29日至2013年5月31日|'
                                                                               r'2013年公司实际发生日常关联交易175亿元|'
                                                                               r'漳州片仔癀药业股份有限公司（以下简称)',
                                         content_text).group(1).strip()

                punishment_decision = re.search(
                    r'(((根据|属于|基于|考虑|按照|依照|鉴于|综上|据此).*?(依据|根据|依照|按照).*?第.*?[条之].*规定.*?'
                    r'((本所|我部)(另)?(做出|作出)(如下|以下).*?决定|对.*?予以公开谴责|我部决定|我部对|现对你公司予以)|'
                    r'根据.*?规定，(本所|我部)做出如下.*?决定|'
                    r'但鉴于情节较轻，未造成严重后果，我部决定|公司应当引以为戒|'
                    r'鉴于.*?我部决定|我部对此予以关注。|(据此，)?我部决定对.*?予以监管关注。|'
                    r'对于公司和董事会秘书的上述违规事实和情节，我部予以监管关注。|因此我部决定对|我部对此表示关注。|我部对中科渝祥予以监管关注|'
                    r'考虑到你公司刚完成重大资产置换，新一届董事会对相关规则的掌握存在缺陷，根据《上海证券交易所纪律处分与监管措施实施办法》第9条和第64条的规定，我部对)'
                    r'[\s\S]*?)\n'
                    r'(特此函告|上海证券交易所)', content_text).group(1).strip()

                truth_text_str = r'((经查|经核实|二、|' \
                                 r'甘肃莫高实业发展股份有限公司|' \
                                 r'\n.*?股份有限公司.*?披露|' \
                                 r'2013年公司实际发生日常关联交易175亿元|' \
                                 r'漳州片仔癀药业股份有限公司（以下简称)' \
                                 r'[\s\S]*?)' \
                                 r'(\n[^。\n]*?行为.*?违反|综上|' + \
                                 punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                     .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*').replace(r'+',
                                                                                                             r'\+') \
                                 + ')'

                truth_compiler = re.compile(truth_text_str, re.MULTILINE)
                truth_list = truth_compiler.findall(content_text)
                truth = '\n'.join([kk[0] for kk in truth_list])

                punishment_basis_text_str = truth_list[-1][0].replace(r'(', r'\(').replace(r')', r'\)') \
                                                .replace(r'.', r'\.') \
                                                .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                .replace(r'+', r'\+') \
                                            + r'([\s\S]*?)' \
                                            + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                                .replace(r'.', r'\.') \
                                                .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                .replace(r'+', r'\+')
                punishment_basis_compiler = re.compile(punishment_basis_text_str, re.MULTILINE)
                punishment_basis = punishment_basis_compiler.search(content_text).group(1).strip()
                if punishment_basis == '':
                    if re.search(r'([^，。\n]*?违反.*?规定.*?)\n', content_text):
                        punishment_basis = re.search(r'([^，。\n]*?违反.*?规定.*?)\n', content_text).group(1).strip()

                publish_date_text = re.search(
                    punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                    .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*').replace(r'+', r'\+')
                    + r'([\s\S]*?)$', content_text).group(1).strip()
                if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                    publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
                    m = re.match("([0-9零一二两三四五六七八九十〇○ＯOΟО]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?",
                                 publish_date)
                    real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(
                        cn2dig(m.group(3))) + '日'
                else:
                    publish_date = response.find(class_='article_opt').text
                    real_publish_date = format_date(publish_date)

                result_map = {
                    'announcementTitle': announcement_title,
                    'announcementOrg': '上交所',
                    'announcementDate': real_publish_date,
                    'announcementCode': announcement_code,
                    'facts': truth,
                    'defenseOpinion': '',
                    'defenseResponse': '',
                    'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                    'punishmentBasement': punishment_basis,
                    'punishmentDecision': punishment_decision,
                    'type': announcement_type,
                    'oss_file_id': file_id,
                    'status': 'not checked'
                }
                if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
                    logger.info(result_map)
                    db.announcement.insert_one(result_map)
                    logger.info('上交所 数据解析 ' + ' -- 数据导入完成')
                else:
                    logger.info('上交所 数据解析 ' + ' -- 数据已经存在')
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('上交所 数据解析 ' + ' -- 修改parsed完成')

            elif document['url'].endswith('pdf'):
                response = request_site_page(announcement_url, use_proxy=True)
                if response is None:
                    logger.error('网页请求错误')
                    continue
                with open('./test/tmp.pdf', 'wb') as f:
                    f.write(response.content)
                content_text = pdf_to_text('./test/tmp.pdf')
                logger.info(content_text)

                with open('./test/tmp.pdf', 'rb') as pdf_file:
                    pdf_content = pdf_file.read()

                if db.parsed_data.find(
                        {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                    oss_file_map = {
                        'origin_url': announcement_url,
                        'oss_file_origin_url': announcement_url,
                        'origin_url_id': origin_url_id,
                        'oss_file_type': 'pdf',
                        'oss_file_name': announcement_title,
                        'oss_file_content': pdf_content,
                        'parsed': False
                    }
                    insert_response = db.parsed_data.insert_one(oss_file_map)
                    file_id = insert_response.inserted_id
                    oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.pdf', pdf_content)
                    db.sse_data.update_one({'_id': origin_url_id}, {'$set': {'status': 'parsed'}})
                else:
                    db.sse_data.update_one({'_id': origin_url_id}, {'$set': {'status': 'parsed'}})
                    file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                       'oss_file_origin_url': announcement_url})['_id']

                if '监管类型：监管工作函' in content_text:
                    announcement_title = re.search(r'标题：(.*?)\n', content_text).group(1).strip()
                    litigant = re.search(r'(证券代码[\s\S]*?)监管类型', content_text).group(1).strip() + '\n' + re.search(
                        r'(涉及对象.*?)\n', content_text).group(1).strip()
                    publish_date = document['webStorageTime']
                    real_publish_date = str(publish_date.year) + '年' + str(publish_date.month) + '月' + str(
                        publish_date.day) + '日'
                    punishment_decision = re.search(r'处理事由：([\s\S]*?)$', content_text).group(1).strip()

                    result_map = {
                        'announcementTitle': announcement_title,
                        'announcementOrg': '上交所',
                        'announcementDate': real_publish_date,
                        'announcementCode': '',
                        'facts': '',
                        'defenseOpinion': '',
                        'defenseResponse': '',
                        'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                        'punishmentBasement': '',
                        'punishmentDecision': punishment_decision,
                        'type': announcement_type,
                        'oss_file_id': file_id,
                        'status': 'not checked'
                    }
                else:
                    if re.search(r'^(上海证券交易所|上 海 证 券 交 易 所)([\s\S]*?)\n(关于对|关于暂停)', content_text):
                        announcement_code = re.search(r'^(上海证券交易所|上 海 证 券 交 易 所)([\s\S]*?)\n(关于对|关于暂停)',
                                                      content_text).group(2).strip()
                    else:
                        if re.search(r'^([\s\S]*?)\n(关于对|关于暂停)', content_text):
                            announcement_code = re.search(r'^([\s\S]*?)\n(关于对|关于暂停)', content_text).group(1).strip()
                        else:
                            announcement_code = ''
                    announcement_code = announcement_code.replace('─', '').replace('\n', '').strip()
                    if re.search(r'\n(当事[\s\S]*?)(经查|经査|经核实|\n.*?股份有限公司.*?披露|\n甘肃莫高实业发展股份有限公司|'
                                 '根据证监会行政处罚査明的事实|根据证监会行政处罚查明的事实|2017年5月5日和5月12日|'
                                 '有关违规事实|2017年10月16日|根据.*?查明的事实)', content_text):
                        litigant = re.search(r'\n(当事[\s\S]*?)(经查|经査|经核实|\n.*?股份有限公司.*?披露|'
                                             '\n甘肃莫高实业发展股份有限公司|根据证监会行政处罚査明的事实|'
                                             '根据证监会行政处罚查明的事实|'
                                             '2017年5月5日和5月12日|'
                                             '有关违规事实|'
                                             '2017年10月16日|'
                                             '根据.*?查明的事实)', content_text).group(
                            1).strip()
                    else:
                        if announcement_code != '':
                            litigant = re.search(announcement_code + r'\n([\s\S]*?)' + r'(经查|经査|你单位作为中体产业集团股份有限公司|'
                                                                                       r'2013年5月29日至2013年5月31日|'
                                                                                       r'2013年公司实际发生日常关联交易175亿元|'
                                                                                       r'漳州片仔癀药业股份有限公司（以下简称|'
                                                                                       r'2017年5月5日和5月12日)',
                                                 content_text).group(1).strip()
                        else:
                            litigant = re.search(r'(决定|決定|決\n定)\n([\s\S]*?)(:|经查|经査|'
                                                 r'上海证券交易所\(以下简称“本所”\)于2013年8月14日|'
                                                 r'武汉钢铁股份有限公司\(以下简称)', content_text).group(2).strip()

                    punishment_decision = re.search(
                        r'(((根据|属于|基于|考虑|按照|依照|鉴于|综上|据此).*?(依据|根据|依照|按照)'
                        r'.*?第.*?[条之][\s\S]*?((本所|我\n?部)\n?(另)?(做\n?出|作\n?出)\n?(如下|以下).*?决定|对.*?予以公开谴责|我部决定|我部对)|'
                        r'鉴于上述(违规)?事实和情节，经上海证券交易所（以下简?称“?本\n?所”?）\n?纪律处分委员会审核(通过)?，根据|'
                        r'鉴于上述违规事实和情节，经本所纪律处分委员会审核通\n过，根据|'
                        r'综上，经上海证券交易所（以下简称本所）纪律处分委员\n会审核，根据|'
                        r'基于上述(违规)?事实和情节，经(本所|上海证券交易所（以下简称本所）)\n?纪律处分委员会审核通\n?过，根\n?据|'
                        r'鉴鉴于于上上述述违违规规事事实实和和情情节节，根据《股票上市规则》》'
                        r'第.*?条和\n《上上海海证证券券交交易易所所纪纪律律处处分分和和监监管管措措施施实实施施办办法法》(的的)?有有关关规规定定，'
                        r'我\n?部?部\n?做做出出如如下下监监管管措措施施决决定定)'
                        r'[\s\S]*?)'
                        r'(\n(特此函告|上海证券)|上海证券交易所上市公司监管一部)', content_text).group(
                        1).strip()

                    truth_text_str = r'((经查|经査|经核实|二、|一、|' \
                                     r'根据.*?查明的事实|' \
                                     r'甘肃莫高实业发展股份有限公司|' \
                                     r'\n.*?股份有限公司.*?披露|' \
                                     r'2013年公司实际发生日常关联交易175亿元|' \
                                     r'漳州片仔癀药业股份有限公司（以下简称|' \
                                     r'公司重大关联交易未履行相应决策程序及信息披露义务|' \
                                     r'2017年5月5日和5月12日|' \
                                     r'收购人庄敏及其一致行动人提供|' \
                                     r'上海证券交易所\(以下简称“本所”\)于2013年8月14日|' \
                                     r'2017年10月16日,因2014-2015年度江苏三房巷实业股)' \
                                     r'[\s\S]*?)' \
                                     r'((\n|。)[^。\n]*?行为[^。\n]*?违反|综上|' + \
                                     punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                         .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                         .replace(r'+', r'\+') + ')'

                    truth_compiler = re.compile(truth_text_str, re.MULTILINE)
                    truth_list = truth_compiler.findall(content_text)
                    truth = '\n'.join([kk[0] for kk in truth_list]).replace('査', '查')

                    punishment_basis_text_str = truth_list[-1][0].replace(r'(', r'\(').replace(r')', r'\)') \
                                                    .replace(r'.', r'\.') \
                                                    .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                    .replace(r'+', r'\+') \
                                                + r'([\s\S]*?)' \
                                                + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                                    .replace(r'.', r'\.') \
                                                    .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                    .replace(r'+', r'\+')
                    punishment_basis_compiler = re.compile(punishment_basis_text_str, re.MULTILINE)
                    punishment_basis = punishment_basis_compiler.search(content_text).group(1).strip()
                    if punishment_basis != '' and punishment_basis[0] == '。':
                        punishment_basis = punishment_basis[1:]

                    publish_date_text = re.search(
                        punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                        .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*').replace(r'+', r'\+')
                        + r'([\s\S]*?)$', content_text).group(1).strip()
                    if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                        publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
                        m = re.match("([0-9零一ー二ニ两三四五六七八九十O○-〇]+年)([0-9一ー二ニ两三四五六七八九十]+)月([0-9一ー二ニ两三四五六七八九十+]+)[号日]",
                                     publish_date)
                        if m:
                            real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(
                                cn2dig(m.group(3))) + '日'
                        else:
                            real_publish_date = ''
                    else:
                        publish_date = document['webStorageTime']
                        real_publish_date = str(publish_date.year) + '年' + str(publish_date.month) + '月' + str(
                            publish_date.day) + '日'

                    result_map = {
                        'announcementTitle': announcement_title,
                        'announcementOrg': '上交所',
                        'announcementDate': real_publish_date,
                        'announcementCode': announcement_code,
                        'facts': truth,
                        'defenseOpinion': '',
                        'defenseResponse': '',
                        'litigant': litigant[:-1] if litigant[-1] == '：' or litigant[-1] == ':' else litigant,
                        'punishmentBasement': punishment_basis,
                        'punishmentDecision': punishment_decision,
                        'type': announcement_type,
                        'oss_file_id': file_id,
                        'status': 'not checked'
                    }
                if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
                    logger.info(result_map)
                    db.announcement.insert_one(result_map)
                    logger.info('上交所 数据解析 ' + ' -- 数据导入完成')
                else:
                    logger.info('上交所 数据解析 ' + ' -- 数据已经存在')
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('上交所 数据解析 ' + ' -- 修改parsed完成')
                logger.info('删除TMP文件')
                if os.path.exists('./test/tmp.pdf'):
                    os.remove('./test/tmp.pdf')

            elif document['url'].endswith('doc') or document['url'].endswith('docx'):
                response = request_site_page(announcement_url, use_proxy=True)
                if response is None:
                    logger.error('网页请求错误')
                    continue
                if document['url'].endswith('doc'):
                    with open('./test/tmp.doc', 'wb') as f:
                        f.write(response.content)
                else:
                    with open('./test/tmp.docx', 'wb') as f:
                        f.write(response.content)
                if not os.path.exists('./test/tmp.docx'):
                    shell_str = config['soffice']['soffice_path'] + ' --headless --convert-to docx ' + \
                                './test/tmp.doc' + ' --outdir ./test/'
                    process = subprocess.Popen(shell_str.replace(r'(', r'\(').replace(r')', r'\)'),
                                               shell=True, stdout=subprocess.PIPE)
                    process.wait()

                with open('./test/tmp.docx', 'rb') as docx_file:
                    docx_content = docx_file.read()

                if db.parsed_data.find(
                        {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                    oss_file_map = {
                        'origin_url': announcement_url,
                        'oss_file_origin_url': announcement_url,
                        'origin_url_id': origin_url_id,
                        'oss_file_type': 'docx',
                        'oss_file_name': announcement_title,
                        'oss_file_content': docx_content,
                        'parsed': False
                    }
                    insert_response = db.parsed_data.insert_one(oss_file_map)
                    file_id = insert_response.inserted_id
                    oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.docx', docx_content)
                    db.sse_data.update_one({'_id': origin_url_id}, {'$set': {'status': 'parsed'}})
                else:
                    db.sse_data.update_one({'_id': origin_url_id}, {'$set': {'status': 'parsed'}})
                    file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                       'oss_file_origin_url': announcement_url})['_id']

                doc = docx.Document('./test/tmp.docx')
                full_text = []
                for para in doc.paragraphs:
                    full_text.append(para.text)
                content_text = '\n'.join(full_text)
                if '监管类型：监管工作函' in content_text:
                    announcement_title = re.search(r'标题：(.*?)\n', content_text).group(1).strip()
                    litigant = re.search(r'(证券代码[\s\S]*?)监管类型', content_text).group(1).strip() + '\n' + re.search(
                        r'(涉及对象.*?)\n', content_text).group(1).strip()
                    publish_date = re.split('-', re.search(r'处分日期：(.*?)\n', content_text).group(1).strip())
                    real_publish_date = str(int(publish_date[0])) + '年' + str(int(publish_date[1])) + '月' + str(
                        int(publish_date[2])) + '日'
                    punishment_decision = re.search(r'处理事由：([\s\S]*?)$', content_text).group(1).strip()

                    result_map = {
                        'announcementTitle': announcement_title,
                        'announcementOrg': '上交所',
                        'announcementDate': real_publish_date,
                        'announcementCode': '',
                        'facts': '',
                        'defenseOpinion': '',
                        'defenseResponse': '',
                        'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                        'punishmentBasement': '',
                        'punishmentDecision': punishment_decision,
                        'type': announcement_type,
                        'oss_file_id': file_id,
                        'status': 'not checked'
                    }
                    if db.announcement.find(
                            {'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
                        logger.info(result_map)
                        db.announcement.insert_one(result_map)
                        logger.info('上交所 数据解析 ' + ' -- 数据导入完成')
                    else:
                        logger.info('上交所 数据解析 ' + ' -- 数据已经存在')
                    db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                    logger.info('上交所 数据解析 ' + ' -- 修改parsed完成')
                    logger.info('删除TMP文件')
                    if os.path.exists('./test/tmp.doc'):
                        os.remove('./test/tmp.doc')
                    if os.path.exists('./test/tmp.docx'):
                        os.remove('./test/tmp.docx')
                else:
                    # TODO 少交所doc处理
                    if db.announcement.find({'announcementTitle': announcement_title}).count() > 0:
                        logger.info('上交所 数据解析 ' + ' -- 数据已经存在')
                        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                        logger.info('上交所 数据解析 ' + ' -- 修改parsed完成')
                        logger.info('删除TMP文件')
                        if os.path.exists('./test/tmp.doc'):
                            os.remove('./test/tmp.doc')
                        if os.path.exists('./test/tmp.docx'):
                            os.remove('./test/tmp.docx')
                    else:
                        logger.info('New Type exists ~')
        except Exception as exc:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logger.error('出错行数：%s' % str(exc_tb.tb_lineno))
            logger.error(exc)
            logger.info('删除TMP文件')
            if os.path.exists('./test/tmp.pdf'):
                os.remove('./test/tmp.pdf')
            if os.path.exists('./test/tmp.txt'):
                os.remove('./test/tmp.txt')
            if os.path.exists('./test/tmp/'):
                for each_txt in os.listdir('./test/tmp'):
                    os.remove('./test/tmp/' + each_txt)
                os.rmdir('./test/tmp')
            if os.path.exists('./test/tmp.doc'):
                os.remove('./test/tmp.doc')
            if os.path.exists('./test/tmp.docx'):
                os.remove('./test/tmp.docx')
            continue


def parse():
    parse_sse()


if __name__ == "__main__":
    parse()
