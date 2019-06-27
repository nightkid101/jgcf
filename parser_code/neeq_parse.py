from pymongo import MongoClient
import re
import os
import sys

from utility import cn2dig, get_year, request_site_page
from init import logger_init, config_init
from oss_utils import init_ali_oss, oss_add_file
from pdf2html import pdf_to_text

logger = logger_init('股转系统 数据解析')
config = config_init()
if config['mongodb']['dev_mongo'] == '1':
    db = MongoClient(config['mongodb']['ali_mongodb_url'], username=config['mongodb']['ali_mongodb_username'],
                     password=config['mongodb']['ali_mongodb_password'],
                     port=int(config['mongodb']['ali_mongodb_port']))[config['mongodb']['ali_mongodb_name']]

    company_db = MongoClient(config['mongodb']['ali_mongodb_url'], username=config['mongodb']['ali_mongodb_username'],
                             password=config['mongodb']['ali_mongodb_password'],
                             port=int(config['mongodb']['ali_mongodb_port']))[
        config['mongodb']['tzw_mongo_db_name']]
else:
    db = MongoClient(
        host=config['mongodb']['mongodb_host'],
        port=int(config['mongodb']['mongodb_port']),
        username=None if config['mongodb']['mongodb_username'] == '' else config['mongodb']['mongodb_username'],
        password=None if config['mongodb']['mongodb_password'] == '' else config['mongodb']['mongodb_password'])[
        config['mongodb']['mongodb_db_name']]

    company_db = MongoClient(
        host=config['mongodb']['mongodb_host'],
        port=int(config['mongodb']['mongodb_port']),
        username=None if config['mongodb']['mongodb_username'] == '' else config['mongodb']['mongodb_username'],
        password=None if config['mongodb']['mongodb_password'] == '' else config['mongodb']['mongodb_password'])[
        config['mongodb']['tzw_mongo_db_name']]

ali_bucket = init_ali_oss()


def parse_neeq():
    # download pdf files
    for each_neeq_document in db.neeq_data.find({'status': {'$nin': ['ignored']}}):
        announcement_url = each_neeq_document['url']
        announcement_title = each_neeq_document['title']

        if db.neeq_data.find({'url': announcement_url}).count() >= 2 and each_neeq_document['type'] != '纪律处分':
            logger.warning(announcement_url + ' ' + announcement_title + ' 在纪律处分与监管措施中一起出现')
            db.neeq_data.update_one({'_id': each_neeq_document['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('股转系统' + ' 重复数据' + ' -- 修改status完成')
            continue

        if '备案材料审查进度表' in announcement_title or \
                '撤销对' in announcement_title or \
                '关于对未按期披露2017年半年度报告的挂牌公司及相关信息披露责任人采取自律监管措施的公告' in announcement_title or \
                '关于对未按期披露2016年年度报告的挂牌公司及相关信息披露责任人采取自律监管措施的公告' in announcement_title or \
                '关于对未按期披露2017年年度报告的挂牌公司及相关信息披露责任人采取自律监管措施的公告' in announcement_title or \
                '关于对未按期披露2018年第一季度报告的挂牌公司及相关信息披露责任人采取自律监管措施的公告' in announcement_title:
            logger.warning(announcement_url + ' ' + announcement_title + ' 与监管处罚无关')
            db.neeq_data.update_one({'_id': each_neeq_document['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('股转系统' + ' 无关数据' + ' -- 修改status完成')
            continue

        if db.neeq_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('Url to parse: ' + announcement_url)

        response = request_site_page(announcement_url)
        if response is None:
            logger.error('网页请求错误 %s' % announcement_url)
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
                'origin_url_id': each_neeq_document['_id'],
                'oss_file_type': 'pdf',
                'oss_file_name': announcement_title,
                'oss_file_content': pdf_content,
                'parsed': False
            }
            insert_response = db.parsed_data.insert_one(oss_file_map)
            file_id = insert_response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.pdf', pdf_content)
            db.neeq_data.update_one({'_id': each_neeq_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.neeq_data.update_one({'_id': each_neeq_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'origin_url_id': each_neeq_document['_id']}})

        try:
            if each_neeq_document['type'] == '监管措施':
                announcement_code_text = re.search(r'^(全国中小企业股份转让系统文件\n)?(.*?)\n', content_text).group(2).strip()
                if '股转系统发' in announcement_code_text:
                    announcement_code = announcement_code_text
                else:
                    announcement_code = '股转系统发〔' + announcement_code_text[:4] + '〕' + announcement_code_text[4:] + '号'

                if re.search(r'(自律\n?监管\n?措\n?施的?决定|纪律\n?处分\n?的决定|'
                             r'约见谈话措施的决定|监管措施的决定|通报批评的决定|'
                             r'自律监管措施的决定的公告)' +
                             r'([\s\S]*?)'
                             r'(\n经査|\n经查|\n查明|\n首创证券推荐挂牌的世纪天鸿|'
                             r'\n世纪天鸿于2016年6月23日|\n2016年1月至2017年4月|'
                             r'经查明,拥湾汇)',
                             content_text):
                    litigant = re.search(r'(自律\n?监管\n?措\n?施的?决定|纪律\n?处分\n?的决定|'
                                         r'约见谈话措施的决定|监管措施的决定|通报批评的决定|'
                                         r'自律监管措施的决定的公告)' +
                                         r'([\s\S]*?)'
                                         r'(\n经査|\n经查|\n查明|\n首创证券推荐挂牌的世纪天鸿|'
                                         r'\n世纪天鸿于2016年6月23日|\n2016年1月至2017年4月|'
                                         r'经查明,拥湾汇)',
                                         content_text).group(2).strip()
                else:
                    try:
                        litigant = re.search(announcement_code_text +
                                             r'([\s\S]*?)'
                                             r'(\n经査|\n经查|\n查明|\n首创证券推荐挂牌的世纪天鸿|'
                                             r'\n世纪天鸿于2016年6月23日|\n2016年1月至2017年4月|'
                                             r'经查明,拥湾汇)',
                                             content_text).group(1).strip()
                    except Exception as e:
                        # logger.error(e)
                        litigant = re.search('((当事人)' +
                                             r'[\s\S]*?)'
                                             r'(\n经査|\n经查|\n查明|\n首创证券推荐挂牌的世纪天鸿|'
                                             r'\n世纪天鸿于2016年6月23日|\n2016年1月至2017年4月|'
                                             r'经查明,拥湾汇|'
                                             r'2018年2月至5月，雷磊以资金拆借方式|'
                                             r'2018年7月25日，我司发出《关于对雷蒙德（北京）)',
                                             content_text).group(1).strip()

                punishment_decision = re.search(
                    r'(((鉴于|基于)(以上|上述)(违规)?事实([和与及]情节)?.*?(根据|经|依据)|'
                    r'鉴于前述事实,根据|鉴于上述.*?事实和情节,根据|'
                    r'根据《全国中小企业股份转让系统业务规则\(试行\)》第6.1条|'
                    r'据《全国\n?中小企业股份转让系统业务规则\(试行\)》|'
                    r'鉴于你公司存在上述违规行为后,公司管理层能够积极整改|'
                    r'根据《业务规则》第6.1条之规定,我司决定|'
                    r'鉴于中道糖业存在上述违规事实,并考虑其在前两次发行|'
                    r'鉴于三联交通及财务总监方向勇存在上述违规事实,根据|'
                    r'鉴于.*?存在上述多项违规事实,违规情节恶劣,根据)'
                    r'[\s\S]*?)'
                    r'(\n全国股转|$)', content_text).group(1).strip()

                truth_text_str = litigant.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                     .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                     .replace(r'+', r'\+') + \
                                 r'([\s\S]*?)' \
                                 r'((。)[^。]*?行为[^。\n]*?违反|综上|你公\n司上述行为违反了|' + \
                                 punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                     .replace(r'[', r'\[').replace(r']', r'\]') \
                                     .replace(r'*', r'\*').replace(r'+', r'\+') + ')'

                truth_compiler = re.compile(truth_text_str, re.MULTILINE)
                truth_list = truth_compiler.findall(content_text)
                truth = '\n'.join([kk[0] for kk in truth_list]).replace('査', '查').strip()

                punishment_basis_text_str = truth_list[-1][0].replace(r'(', r'\(').replace(r')', r'\)') \
                                                .replace(r'.', r'\.') \
                                                .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                .replace(r'+', r'\+').replace(r'>', r'\>').replace(r'?', r'\?') \
                                            + r'([\s\S]*?)' \
                                            + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                                .replace(r'.', r'\.').replace(r'[', r'\[').replace(r']', r'\]') \
                                                .replace(r'*', r'\*').replace(r'+', r'\+')
                punishment_basis_compiler = re.compile(punishment_basis_text_str, re.MULTILINE)
                punishment_basis = punishment_basis_compiler.search(content_text).group(1).strip()
                if punishment_basis != '' and punishment_basis[0] == '。':
                    punishment_basis = punishment_basis[1:].strip()

                publish_date_text = re.search(
                    punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                    .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*').replace(r'+', r'\+')
                    + r'([\s\S]*?)$', content_text).group(1).strip()
                if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                    publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
                    m = re.match("([0-9零一ー二ニ两三四五六七八九十O-]+年)([0-9一ー二ニ两三四五六七八九十]+)月([0-9一ー二ニ两三四五六七八九十+]+)([号日])",
                                 publish_date)
                    real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(
                        cn2dig(m.group(3))) + '日'
                else:
                    # publish_date = each_neeq_document['publishDate'].split('-')
                    real_publish_date = ''
                    # str(int(publish_date[0])) + '年' + str(int(publish_date[1])) + '月' + \
                    #                 str(int(publish_date[2])) + '日'

                result_map = {
                    'announcementTitle': announcement_title,
                    'announcementOrg': '股转系统',
                    'announcementDate': real_publish_date,
                    'announcementCode': announcement_code,
                    'facts': truth,
                    'defenseOpinion': '',
                    'defenseResponse': '',
                    'litigant': litigant[:-1].strip() if litigant[-1] == '：' or litigant[
                        -1] == ':' else litigant.strip(),
                    'punishmentBasement': punishment_basis,
                    'punishmentDecision': punishment_decision,
                    'type': '监管措施',
                    'oss_file_id': file_id,
                    'status': 'not checked'
                }
                logger.info(result_map)
                if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
                    db.announcement.insert_one(result_map)
                    logger.info('股转系统 数据解析 ' + ' -- 数据导入完成')
                else:
                    logger.info('股转系统 数据解析 ' + ' -- 数据已经存在')
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('股转系统 数据解析 ' + ' -- 修改parsed完成')
                logger.info('删除TMP文件')
                if os.path.exists('./test/tmp.pdf'):
                    os.remove('./test/tmp.pdf')
            elif each_neeq_document['type'] == '纪律处分':
                announcement_code_text = re.search(r'^(全国中小企业股份转让系统文件\n)?(.*?)\n', content_text).group(2).strip()
                if '股转系统发' in announcement_code_text:
                    announcement_code = announcement_code_text
                else:
                    announcement_code = '股转系统发〔' + announcement_code_text[:4] + '〕' + announcement_code_text[4:] + '号'

                if re.search(r'(自律\n?监管\n?措\n?施的?决定|纪律\n?处分\n?的决定|'
                             r'约见谈话措施的决定|监管措施的决定|通报批评的决定|'
                             r'自律监管措施的决定的公告)' +
                             r'([\s\S]*?)'
                             r'(\n经査|\n经查|\n查明|\n首创证券推荐挂牌的世纪天鸿|'
                             r'\n世纪天鸿于2016年6月23日|\n2016年1月至2017年4月|'
                             r'经查明,拥湾汇)',
                             content_text):
                    litigant = re.search(r'(自律\n?监管\n?措\n?施的?决定|纪律\n?处分\n?的决定|'
                                         r'约见谈话措施的决定|监管措施的决定|通报批评的决定|'
                                         r'自律监管措施的决定的公告)' +
                                         r'([\s\S]*?)'
                                         r'(\n经査|\n经查|\n查明|\n首创证券推荐挂牌的世纪天鸿|'
                                         r'\n世纪天鸿于2016年6月23日|\n2016年1月至2017年4月|'
                                         r'经查明,拥湾汇)',
                                         content_text).group(2).strip()
                else:
                    litigant = re.search(announcement_code_text +
                                         r'([\s\S]*?)'
                                         r'(\n经査|\n经查|\n查明|\n首创证券推荐挂牌的世纪天鸿|'
                                         r'\n世纪天鸿于2016年6月23日|\n2016年1月至2017年4月|'
                                         r'经查明,拥湾汇)',
                                         content_text).group(1).strip()

                punishment_decision = re.search(
                    r'(((鉴于|基于)(以上|上述)(违规)?事实([和与及]情节)?.*?(根据|经|依据)|'
                    r'鉴于前述事实,根据|鉴于上述.*?事实和情节,根据|'
                    r'根据《全国中小企业股份转让系统业务规则\(试行\)》第6.1条|'
                    r'据《全国\n?中小企业股份转让系统业务规则\(试行\)》|'
                    r'鉴于你公司存在上述违规行为后,公司管理层能够积极整改|'
                    r'根据《业务规则》第6.1条之规定,我司决定|'
                    r'鉴于中道糖业存在上述违规事实,并考虑其在前两次发行|'
                    r'鉴于三联交通及财务总监方向勇存在上述违规事实,根据|'
                    r'鉴于.*?存在上述多项违规事实,违规情节恶劣,根据)'
                    r'[\s\S]*?)'
                    r'(\n全国股转|$)', content_text).group(1).strip()

                truth_text_str = litigant.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                     .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                     .replace(r'+', r'\+') + \
                                 r'([\s\S]*?)' \
                                 r'((。)[^。]*?行为[^。\n]*?违反|综上|你公\n司上述行为违反了|' + \
                                 punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                     .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                     .replace(r'+', r'\+') + ')'

                truth_compiler = re.compile(truth_text_str, re.MULTILINE)
                truth_list = truth_compiler.findall(content_text)
                truth = '\n'.join([kk[0] for kk in truth_list]).replace('査', '查').strip()

                punishment_basis_text_str = truth_list[-1][0].replace(r'(', r'\(').replace(r')', r'\)') \
                                                .replace(r'.', r'\.') \
                                                .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                .replace(r'+', r'\+').replace(r'>', r'\>').replace(r'?', r'\?') \
                                            + r'([\s\S]*?)' \
                                            + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                                .replace(r'.', r'\.').replace(r'[', r'\[').replace(r']', r'\]') \
                                                .replace(r'*', r'\*').replace(r'+', r'\+')
                punishment_basis_compiler = re.compile(punishment_basis_text_str, re.MULTILINE)
                punishment_basis = punishment_basis_compiler.search(content_text).group(1).strip().strip()
                if punishment_basis != '' and punishment_basis[0] == '。':
                    punishment_basis = punishment_basis[1:].strip()

                publish_date_text = re.search(
                    punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                    .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*').replace(r'+', r'\+')
                    + r'([\s\S]*?)$', content_text).group(1).strip()
                if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                    publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
                    m = re.match("([0-9零一ー二ニ两三四五六七八九十O-]+年)([0-9一ー二ニ两三四五六七八九十]+)月([0-9一ー二ニ两三四五六七八九十+]+)[号日]",
                                 publish_date)
                    real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(
                        cn2dig(m.group(3))) + '日'
                else:
                    # publish_date = each_neeq_document['announcementDate']
                    # real_publish_date = str(int(publish_date['year'])) + '年' + str(
                    #     int(publish_date['month'])) + '月' + \
                    #                     str(int(publish_date['day'])) + '日'
                    real_publish_date = ''

                result_map = {
                    'announcementTitle': announcement_title,
                    'announcementOrg': '股转系统',
                    'announcementDate': real_publish_date,
                    'announcementCode': announcement_code,
                    'facts': truth,
                    'defenseOpinion': '',
                    'defenseResponse': '',
                    'litigant': litigant[:-1].strip() if litigant[-1] == '：' or litigant[
                        -1] == ':' else litigant.strip(),
                    'punishmentBasement': punishment_basis,
                    'punishmentDecision': punishment_decision,
                    'type': '纪律处分',
                    'oss_file_id': file_id,
                    'status': 'not checked'
                }
                logger.info(result_map)
                if db.announcement.find(
                        {'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
                    db.announcement.insert_one(result_map)
                    logger.info('股转系统 数据解析 ' + ' -- 数据导入完成')
                else:
                    logger.info('股转系统 数据解析 ' + ' -- 数据已经存在')
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('股转系统 数据解析 ' + ' -- 修改parsed完成')
                logger.info('删除TMP文件')
                if os.path.exists('./test/tmp.pdf'):
                    os.remove('./test/tmp.pdf')
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


def parse_neeq_before():
    # 2016年5月6日之后的数据
    if db.neeq_data.find({'url': 'http://www.neeq.com.cn/disclosure/2016/0506/20160506084049196.docx'}).count() == 0:
        origin_url_insert_id = db.neeq_data.insert_one({
            'url': 'http://www.neeq.com.cn/disclosure/2016/0506/20160506084049196.docx',
            'publishDate': '',
            'title': '20160506_neeq',
            'type': '监管措施',
            'origin': '股转系统',
            'status': 'parsed'
        })
        origin_url_inserted_id = origin_url_insert_id.inserted_id
    else:
        origin_url_inserted_id = db.neeq_data.find(
            {'url': 'http://www.neeq.com.cn/disclosure/2016/0506/20160506084049196.docx'})[0]['_id']

    if db.parsed_data.find(
            {'origin_url': 'http://www.neeq.com.cn/disclosure/2016/0506/20160506084049196.docx',
             'oss_file_origin_url': 'http://www.neeq.com.cn/disclosure/2016/0506/20160506084049196.docx'}).count() == 0:
        response = request_site_page('http://www.neeq.com.cn/disclosure/2016/0506/20160506084049196.docx')
        if response.status_code == '404':
            return '', ''
        with open('./test/tmp.docx', 'wb') as out_file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    out_file.write(chunk)
        with open('./test/tmp.docx', 'rb') as docx_file:
            docx_content = docx_file.read()
        logger.info('删除TMP文件')
        if os.path.exists('./test/tmp.docx'):
            os.remove('./test/tmp.docx')
        oss_file_map = {
            'origin_url': 'http://www.neeq.com.cn/disclosure/2016/0506/20160506084049196.docx',
            'oss_file_origin_url': 'http://www.neeq.com.cn/disclosure/2016/0506/20160506084049196.docx',
            'origin_url_id': origin_url_inserted_id,
            'oss_file_type': 'docx',
            'oss_file_name': '20160506084049196',
            'oss_file_content': docx_content,
            'parsed': True
        }
        insert_response = db.parsed_data.insert_one(oss_file_map)
        file_id = insert_response.inserted_id
        oss_add_file(ali_bucket, str(file_id) + '/20160506084049196.docx', docx_content)
        db.neeq_data.update_one({'_id': origin_url_inserted_id}, {'$set': {'status': 'parsed'}})
    else:
        db.neeq_data.update_one({'_id': origin_url_inserted_id}, {'$set': {'status': 'parsed'}})
        file_id = db.parsed_data.find_one(
            {'origin_url': 'http://www.neeq.com.cn/disclosure/2016/0506/20160506084049196.docx',
             'oss_file_origin_url': 'http://www.neeq.com.cn/disclosure/2016/0506/20160506084049196.docx'})['_id']

    for each_punishment in db.punishAnnouncement.find(
            {'url': 'http://www.neeq.com.cn/disclosure/2016/0506/20160506084049196.docx', 'announcementOrg': '股转系统'}):
        result_map = {
            'announcementTitle': each_punishment['announcementTitle'],
            'announcementOrg': each_punishment['announcementOrg'],
            'announcementDate': each_punishment['announcementDate'],
            'announcementCode': each_punishment['announcementCode'],
            'facts': each_punishment['facts'],
            'defenseOpinion': each_punishment['defenseOpinion'],
            'defenseResponse': each_punishment['defenseResponse'],
            'litigant': each_punishment['litigant'],
            'punishmentBasement': each_punishment['punishmentBasement'],
            'punishmentDecision': each_punishment['punishmentDecision'],
            'type': '要闻',
            'oss_file_id': file_id,
            'status': 'checked'
        }
        if db.announcement.find(result_map).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('股转系统 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('股转系统 数据解析 ' + ' -- 数据已经存在')
    db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
    logger.info('股转系统' + ' 数据解析 ' + ' -- 修改parsed完成')


def parse():
    parse_neeq()
    parse_neeq_before()


if __name__ == "__main__":
    parse()
