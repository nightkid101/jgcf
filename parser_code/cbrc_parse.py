import re
import os

from pymongo import MongoClient
from init import logger_init, config_init
from utility import format_date, remove_special_char, table_to_list, request_site_page, remove_strip
from bs4 import BeautifulSoup as bs
from oss_utils import init_ali_oss, oss_add_file
from pdf2html import pdf_to_text
from urllib.parse import urljoin

logger = logger_init('银监会及其附属机构 数据解析')
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


# parse table content
def parse_table(file_id, table, origin_title, origin_publish_date):
    strip_list = ['', '\xa0', '-', '——', '———', '—', '/', '---', '----', '--', '-----', '------']
    content_table_list = table_to_list(table)

    announcement_code = person_name = person_company = company_name = legal_representative \
        = truth = punishment_basement = punishment_decision = organization = publish_date \
        = litigant = ''
    for each_row in content_table_list:
        if re.search(r'(行政)?处罚决定书?文书?号', remove_strip(each_row[0].strip())):
            announcement_code = each_row[-1].strip()
        if re.search(r'(被处罚当事人)', remove_strip(each_row[0].strip())) and \
                re.search(r'(个人)', remove_strip(each_row[1].strip())) and \
                re.search(r'(姓名|名称)', remove_strip(each_row[2].strip())):
            person_name = each_row[-1].strip()
        if re.search(r'(被处罚当事人)', remove_strip(each_row[0].strip())) and \
                re.search(r'(个人)', remove_strip(each_row[1].strip())) and \
                re.search(r'(单位)', remove_strip(each_row[2].strip())):
            person_company = each_row[-1].strip()
        if re.search(r'(被处罚当事人)', remove_strip(each_row[0].strip())) and \
                re.search(r'(单位)', remove_strip(each_row[1].strip())) and \
                re.search(r'(名称)', remove_strip(each_row[2].strip())):
            company_name = each_row[-1].strip()
        if re.search(r'(被处罚当事人)', remove_strip(each_row[0].strip())) and \
                re.search(r'(单位)', remove_strip(each_row[1].strip())) and \
                re.search(r'(法定代表人|主要负责人)', remove_strip(each_row[2].strip())):
            legal_representative = each_row[-1].strip()

        if re.search(r'(主要(违法)?违规事实|案由)', remove_strip(each_row[0].strip())):
            truth = each_row[-1].strip()
        if re.search(r'(^行政处罚依据$)', remove_strip(each_row[0].strip())):
            punishment_basement = each_row[-1].strip()
        if re.search(r'(^行政处罚决定$)', remove_strip(each_row[0].strip())):
            punishment_decision = each_row[-1].strip()
        if re.search(r'(作出.*?机关名称)', remove_strip(each_row[0].strip())):
            organization = each_row[-1].strip()
        if re.search(r'(作出.*?日期)', remove_strip(each_row[0].strip())):
            publish_date = each_row[-1].strip()

    if person_name not in strip_list:
        litigant += '个人姓名: ' + remove_strip(person_name) + '\n'
    if person_company not in strip_list:
        litigant += '个人就职单位: ' + remove_strip(person_company) + '\n'
    if company_name not in strip_list:
        litigant += '单位名称: ' + remove_strip(company_name) + '\n'
    if legal_representative not in strip_list:
        litigant += '单位法定代表人姓名: ' + remove_strip(legal_representative)

    # 这个页面的日期和机构是反的
    if announcement_code == '永银监罚决字〔2018〕34号':
        organization = '永州银监分局'
        publish_date = '2018年5月7日'
    organization = organization.replace('\n', '').strip()
    if organization == '中国银行业监督管理委员会湘潭分局':
        organization = '湘潭银监分局'
    if organization == '中国滨州监管分局':
        organization = '滨州银监分局'
    if organization == '中国监管会临沂监管分局':
        organization = '临沂银监分局'
    if organization == '陕西监管局':
        organization = '陕西银监局'
    if organization == '中国银行监督管理委员会乐山监管分局':
        organization = '乐山银监分局'
    if organization == '中国监管会临沂监管分局' or organization == '临汾监管分局' or organization == '中国银监员会临沂监管分局':
        organization = '临沂银监分局'
    if organization == '鸡西监管分局':
        organization = '鸡西银监分局'
    if re.match('中国(银监会|银行业?监督管理委员会|银行业监督委员会|银行业监管管理委员会)(.*?)(监管|银监)局', organization):
        organization = re.search(r'中国(银监会|银行业?监督管理委员会|银行业监督委员会|银行业监管管理委员会)(.*?)(监管|银监)局', organization).group(
            2).strip() + '银监局'
    elif re.match('中国(银监会|银行业?监督管理委员会|银行业监督委员会|银行业监管管理委员会)(.*?)(监管|银监)分局', organization):
        organization = re.search(r'中国(银监会|银行业?监督管理委员会|银行业监督委员会|银行业监管管理委员会)(.*?)(监管|银监)分局', organization).group(
            2).strip() + '银监分局'
    organization = organization.replace('中国银监会', '').replace('（根据授权实施行政处罚）', '') \
        .replace('（筹）', '').replace('名称', '').replace('。', '')
    if re.match('.*（.*）', organization):
        organization = organization

    if publish_date != '':
        publish_date = format_date(publish_date)
    else:
        publish_date = format_date(origin_publish_date)

    title = remove_strip(origin_title)

    result_map = {
        'announcementTitle': title,
        'announcementOrg': organization,
        'announcementDate': publish_date,
        'announcementCode': remove_strip(announcement_code),
        'facts': truth,
        'defenseOpinion': '',
        'defenseResponse': '',
        'litigant': litigant.strip(),
        'punishmentBasement': '',
        'punishmentDecision': ('依据' + punishment_basement + '，' + punishment_decision).replace('。，', '，').replace(
            '依据根据', '依据').replace('依据依据', '依据'),
        'type': '行政处罚决定',
        'oss_file_id': file_id,
        'status': 'not checked'
    }
    logger.info(result_map)
    if db.announcement.find({'announcementTitle': title, 'oss_file_id': file_id}).count() == 0:
        db.announcement.insert_one(result_map)
        logger.info('银监局 数据解析 ' + organization + ' -- 数据导入完成')
    else:
        logger.info('银监局 数据解析 ' + organization + ' -- 数据已经存在')
    db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
    logger.info('银监局 数据解析 ' + organization + ' -- 修改parsed完成')


# parse text content
def parse_text(file_id, text, origin_title, origin_publish_date, organization):
    logger.info(text)
    if re.search(r'^(中国银行业监督管理委员会上海监管局\n|'
                 '中国银行业监管管理委员会上海监管局\n|'
                 '中国银监会海南监管局行政处罚决定书\n|'
                 '中国银行业监督管理委员会\n *海南监管局行政处罚决定书\n|'
                 '中国银行业监督管理委员会天津监管局\n行政处罚决定书\n|'
                 '.*?青岛监管局|'
                 '行政处罚决定书\n|'
                 '.*?天津监管局\n行政处罚决定书|'
                 '中国银行业监督管理委员会三亚监管分局\n行政处罚决定书\n)(.*?)\n', text):
        announcement_code = re.search(r'^(中国银行业监督管理委员会上海监管局\n|'
                                      '中国银行业监管管理委员会上海监管局\n|'
                                      '中国银监会海南监管局行政处罚决定书\n|'
                                      '中国银行业监督管理委员会\n *海南监管局行政处罚决定书\n|'
                                      '中国银行业监督管理委员会天津监管局\n行政处罚决定书\n|'
                                      '.*?青岛监管局|'
                                      '行政处罚决定书\n|'
                                      '.*?天津监管局\n行政处罚决定书|'
                                      '中国银行业监督管理委员会三亚监管分局\n行政处罚决定书\n)(.*?)\n', text).group(2).strip()
        litigant = re.search(
            announcement_code.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.')
            .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*').replace(r'+', r'\+') +
            r'([\s\S]*?)' + r'(根据.*有关(规定|法律法规|法规)|违法事实和证据：\n)', text).group(1).strip().replace('\xa0', '')
    else:
        if re.search(r'^(.*?)\s*中国银行业监督管理委员会上海监管局行政处罚决定书', text):
            announcement_code = re.search(r'^(.*?)\s*中国银行业监督管理委员会上海监管局行政处罚决定书', text).group(1).strip()
            litigant = re.search(r'中国银行业监督管理委员会上海监管局行政处罚决定书' + r'([\s\S]*?)' +
                                 r'(根据.*有关(规定|法律法规|法规)|违法事实和证据：\n)', text).group(1).strip().replace('\xa0', '')
        else:
            announcement_code = ''
            litigant = re.search(r'(行政处罚决定书送达公告|意见告知书送达公告|行政处罚意见告知书的送达公告)([\s\S]*?)经查',
                                 text).group(1).strip().replace('：', '')

    punishment_decision = re.search(
        r'(((根据.*?(规定)?|综上)，.*?([作做])出(如下|以下|罚款人民币.*?万元的)(行政处罚|决定|处罚)|处罚决定：\n|'
        '根据.*?第.*?条.*?规定([，,])决定对.*?处以.*?行政处罚|'
        '根据.*?第.*?条.*?规定([，,])现(给予|对.*?作出).*?行政处罚|'
        '依据《中华人民共和国商业银行法》第五十条“商业银行办理业务，提供服务，按照规定收取手续费”以及七十三条第四款“商业银行有下列情形之一|'
        '((综合考虑|根据).*?)?(根据|按照).*?第.*?条.*?规定.*?(我局)?决定.*?给予.*?行政处罚|'
        '根据《中华人民共和国银行业监督管理法》第四十八条第（三）项的规定，我分?局拟?对你作出)'
        r'[\s\S]*?)\n'
        '(.*?自收到本(行政)?处罚决定书?之日|.*?如不服本行政处罚决定|'
        '处罚的履行方式和期限|根据《中华人民共和国行政处罚法》第三十一条、第三十二条的规定，你如对上述处罚意见有异议|'
        '逾期不提出申请的，视为放弃听证权利。)', text).group(1).strip()

    if '处罚依据：\n' in text or '处罚的依据：\n' in text:
        truth_text_str = '(违法事实和证据：\n)' \
                         r'([\s\S]*?)' \
                         '(处罚(的)?依据：\n|上述(事实|行为)有.*?等证据为证。)'
        truth_compiler = re.compile(truth_text_str, re.MULTILINE)
        truth_list = truth_compiler.findall(text)
        truth = '\n'.join([kk[1] for kk in truth_list])
        truth = re.sub('(\n|\r|\r\n)+', '\n', truth).strip()
        punishment_basis_str_list = [
            r'([^\n。；]*?)(问题|行为|事项|情况|事实|做法)([^\n。；\s]*?)违反.*?((的|之|等)(相关)?(规定|要求))?',
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile('(。\n；)' + '(' + punishment_basis_str + ')' +
                                               '.(\n?依据|\n?根据|\n?鉴于|\n二|\n?综合考虑)', re.MULTILINE)
        punishment_basis_list = punishment_basis_compiler.findall(text)

        punishment_basis = '；'.join([kk[1].strip() for kk in punishment_basis_list])
        punishment_decision_basis = re.search(r'处罚(的)?依据：\n([\s\S]*?)处罚决定：', text).group(2).strip()
        punishment_decision = re.search(r'处罚决定：([\s\S]*?)'
                                        '(.*?自收到本(行政)?处罚决定书?之日|.*?如不服本行政处罚决定|处罚的履行方式和期限)', text).group(1).strip()
        punishment_final_decision = ('依据' + punishment_decision_basis + '，' + punishment_decision).replace('。，', '，')
    else:
        truth_text_str = r'((经查|违法事实和证据：\n|（二）|二、)' \
                         r'[\s\S]*?)' \
                         r'(处罚(的)?依据：\n|上述(事实|行为)有.*?等证据为证。|根据.*第.*?条.*?规定.*?作出.*?处罚|' \
                         r'以上行为违反了《个人贷款管理暂行办法》第三条、第七条、|' \
                         r'[^。；\s]*?(问题|行为|事项|情况|事实|做法)[^。；\s]*?违反.*?(规定|要求))'

        truth_compiler = re.compile(truth_text_str, re.MULTILINE)
        truth_list = truth_compiler.findall(text)
        truth = '\n'.join([kk[0] for kk in truth_list])
        truth = re.sub('(\n|\r|\r\n)+', '\n', truth).strip()
        punishment_final_decision = punishment_decision

        if re.search(truth_list[-1][0].replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.')
                             .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*').replace(r'+', r'\+')
                     + r'\n?(上述(事实|行为)有.*?等证据为证。)([\s\S]*)$', text, re.MULTILINE):
            punishment_basis_text = r'\n' + \
                                    re.search(truth_list[-1][0].replace(r'(', r'\(').replace(r')', r'\)')
                                              .replace(r'.', r'\.').replace(r'[', r'\[').replace(r']', r'\]')
                                              .replace(r'*', r'\*').replace(r'+', r'\+') +
                                              r'\n?(上述(事实|行为)有.*?等证据为证。)'
                                              r'([\s\S]*)$', text, re.MULTILINE).group(3).strip() + '\n'
        else:
            punishment_basis_text = text
        punishment_basis_str_list = [
            r'([^\n。；]*?)(问题|行为|事项|情况|事实|做法)([^\n。；\s]*?)违反.*?((的|之|等)(相关)?(规定|要求))?',
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'([。\n；])' + '(' + punishment_basis_str + ')' +
                                               '.(\n?依据|\n?根据|\n?鉴于|\n二|\n?综合考虑)', re.MULTILINE)
        punishment_basis_list = punishment_basis_compiler.findall(punishment_basis_text)
        punishment_basis = '；'.join([kk[1].strip() for kk in punishment_basis_list])

    publish_date_text = re.search(
        punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.')
        .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*').replace(r'+', r'\+')
        + r'([\s\S]*?)$', text).group(1).strip()
    if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
        publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
        real_publish_date = format_date(publish_date)
    else:
        real_publish_date = format_date(origin_publish_date)

    result_map = {
        'announcementTitle': origin_title,
        'announcementOrg': organization,
        'announcementDate': real_publish_date,
        'announcementCode': remove_strip(announcement_code),
        'facts': truth,
        'defenseOpinion': '',
        'defenseResponse': '',
        'litigant': litigant,
        'punishmentBasement': punishment_basis,
        'punishmentDecision': punishment_final_decision,
        'type': '行政处罚决定',
        'oss_file_id': file_id,
        'status': 'not checked'
    }
    logger.info(result_map)
    if db.announcement.find({'announcementTitle': origin_title, 'oss_file_id': file_id}).count() == 0:
        db.announcement.insert_one(result_map)
        logger.info('银监局 数据解析 ' + organization + ' -- 数据导入完成')
    else:
        logger.info('银监局 数据解析 ' + organization + ' -- 数据已经存在')
    db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
    logger.info('银监局 数据解析 ' + organization + ' -- 修改parsed完成')


# 银监会解析
def parse_cbrc():
    for each_cbrc_document in db.cbrc_data.find({'origin': '银监会', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_cbrc_document['url']
        announcement_title = each_cbrc_document['title']

        if re.search(r'(^.*?行政处罚事项目录$|^.*?政府信息公开工作报告$|'
                     '^青海银监局2014年实施行政处罚基本情况$|'
                     '^上海银监局对部分银行信用卡业务的违规行为实施行政处罚$)', announcement_title):
            logger.warning(announcement_url + ' ' + announcement_title + ' 与监管处罚无关')
            db.cbrc_data.update_one({'_id': each_cbrc_document['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('银监会' + ' 无关数据' + ' -- 修改status完成')
            continue

        if db.cbrc_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'parsed': True}).count() == 1 and \
                db.announcement.find(
                    {'oss_file_id': db.parsed_data.find({'origin_url': announcement_url})[0]['_id']}).count() > 0:
            continue

        logger.info('Url to parse: ' + announcement_url + ' ' + announcement_title)

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误')
            continue
        content_soup = bs(content_response.text.encode(content_response.encoding).decode('utf-8'), 'lxml') \
            if content_response else bs('', 'lxml')

        if db.parsed_data.find(
                {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_cbrc_document['_id'],
                'oss_file_type': 'html',
                'oss_file_name': announcement_title,
                'oss_file_content': content_response.text.encode(content_response.encoding).decode('utf-8'),
                'parsed': False
            }
            insert_response = db.parsed_data.insert_one(oss_file_map)
            file_id = insert_response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                         content_response.text.encode(content_response.encoding).decode('utf-8'))
            db.cbrc_data.update_one({'_id': each_cbrc_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.cbrc_data.update_one({'_id': each_cbrc_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']

        if '行政处罚信息公开表' in announcement_title:
            content_table_list = table_to_list(content_soup.find(class_='MsoNormalTable'))
            content_list = []
            for each_row in content_table_list:
                for each_col in each_row:
                    if each_col not in content_list:
                        content_list.append(each_col)
            content_text = remove_special_char('\n'.join(content_list))

            title = announcement_title
            document_code = re.search(r'^行政处罚决定书文号([\s\S]*)\n被处罚当事人姓名或名称', content_text).group(1).strip()
            person_name = re.search(r'\n个人姓名([\s\S]*)\n单位\n?名称', content_text).group(1).strip()
            company_name = re.search(r'\n单位\n?名称([\s\S]*)\n法定代表人(（主要负责人）)?姓名', content_text).group(1).strip()
            legal_representative_compiler = re.compile(
                r'\n法定代表人(（主要负责人）)?姓名([\s\S]*)\n主要违法违规事实(\n)?（案由）', re.MULTILINE)
            legal_representative = legal_representative_compiler.search(content_text).group(2).strip()
            truth_compiler = re.compile(r'\n主要违法违规事实(\n)?（案由）([\s\S]*)\n行政处罚依据', re.MULTILINE)
            truth = truth_compiler.search(content_text).group(2).strip()

            punishment_basis = ''

            punishment_decision_basis = re.search(r'\n行政处罚依据([\s\S]*)\n行政处罚决定', content_text).group(1).strip()
            punishment_decision_compiler = re.compile(
                r'\n行政处罚决定([\s\S]*)\n作出处罚决定的\n?机关\n?名称', re.MULTILINE)
            punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

            final_punishment_decision = '依据' + punishment_decision_basis + '，' + punishment_decision
            final_punishment_decision = final_punishment_decision.replace('。，', '，').replace('，，', '，')
            publish_date = re.search(r'\n作出处罚决定的日期([\s\S]*)$', content_text).group(1).strip()

            litigant = ''
            if person_name != '':
                litigant += '个人姓名: ' + person_name + '\n'
            if company_name != '':
                litigant += '单位名称: ' + company_name + '\n'
            if legal_representative != '':
                litigant += '单位法定代表人姓名: ' + legal_representative

            result_map = {
                'announcementTitle': title.replace('\n', ''),
                'announcementOrg': '银保监会',
                'announcementDate': format_date(publish_date),
                'announcementCode': document_code,
                'facts': truth,
                'defenseOpinion': '',
                'defenseResponse': '',
                'litigant': litigant.strip(),
                'punishmentBasement': punishment_basis,
                'punishmentDecision': final_punishment_decision,
                'type': '行政处罚决定',
                'oss_file_id': file_id,
                'status': 'not checked'
            }
        else:
            content_text = content_soup.find(class_='Section0').text.strip() \
                if content_soup.find(class_='Section0') else content_soup.find(class_='Section1').text.strip()
            litigant_compiler = re.compile('\n(.*)：\n', re.MULTILINE)
            litigant = litigant_compiler.search(content_text).group(1)
            truth = re.search(r'经查，你(.*)。根据', content_text).group(1).strip()
            punishment_basis = re.search(r'。根据(.*)我会拟对你', content_text).group(1).strip()
            punishment_decision = re.search(r'我会拟对你(.*)', content_text).group(1).strip()
            publish_date = content_text.split('\n')[-1]
            document_code = re.search(r'银监罚告字〔\d{4}〕\d+号', content_text).group(0).strip()
            title = announcement_title
            result_map = {
                'announcementTitle': title.replace('\n', ''),
                'announcementOrg': '银保监会',
                'announcementDate': publish_date,
                'announcementCode': document_code,
                'facts': truth,
                'defenseOpinion': '',
                'defenseResponse': '',
                'litigant': litigant,
                'punishmentBasement': punishment_basis,
                'punishmentDecision': punishment_decision,
                'type': '行政处罚决定',
                'oss_file_id': file_id,
                'status': 'not checked'
            }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('银监会 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('银监会 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('银监会 数据解析 ' + ' -- 修改parsed完成')


# 银监(分)局解析
def parse_office_cbrc():
    for each_cbrc_document in db.cbrc_data.find({'origin': {'$nin': ['银监会']}, 'status': {'$nin': ['ignored']}},
                                                no_cursor_timeout=True).sort("_id", 1):
        announcement_url = each_cbrc_document['url']
        announcement_title = each_cbrc_document['title']

        if re.search(r'(^.*?行政处罚事项目录$|^.*?政府信息公开工作报告$|'
                     '^青海银监局2014年实施行政处罚基本情况$|'
                     '^上海银监局对部分银行信用卡业务的违规行为实施行政处罚$)', announcement_title):
            logger.warning(announcement_url + ' ' + announcement_title + ' 与监管处罚无关')
            db.cbrc_data.update_one({'_id': each_cbrc_document['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('银监(分)局' + ' 无关数据' + ' -- 修改status完成')
            continue

        # 处罚内容是图片
        if announcement_url in [
            'http://www.cbrc.gov.cn/hubei/docPcjgView/2003588FB00F4D449B1EB1A27C280B76/13.html',
            'http://www.cbrc.gov.cn/hubei/docPcjgView/159CF114958046F89349E1AC0B85E041/13.html',
            'http://www.cbrc.gov.cn/hubei/docPcjgView/7CE4C6A9654944A89D6350B0AC74063F/13.html',
            'http://www.cbrc.gov.cn/hubei/docPcjgView/A519F121B2514C2DB4D1FF7E82ED6A5C/13.html',
            'http://www.cbrc.gov.cn/hubei/docPcjgView/94E1B91509F14153919688B7993FCBD2/13.html',
            'http://www.cbrc.gov.cn/hubei/docPcjgView/33457283553E4220B610388818FEA9DC/13.html',
            'http://www.cbrc.gov.cn/hubei/docPcjgView/3209926860AA47CC925FC7AF1F209F28/13.html',
            'http://www.cbrc.gov.cn/hubei/docPcjgView/FAC1226622164366BBDE86B4DC415046/13.html',
            'http://www.cbrc.gov.cn/hubei/docPcjgView/4096E21FDC484FCF8B2A9DD88A59A7EF/13.html'
        ]:
            continue

        if db.cbrc_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'parsed': True}).count() == 1 and \
                db.announcement.find(
                    {'oss_file_id': db.parsed_data.find({'origin_url': announcement_url})[0]['_id']}).count() > 0:
            continue

        logger.info('Url to parse: ' + announcement_url + ' ' + announcement_title)

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误')
            continue
        content_soup = bs(content_response.text.encode(content_response.encoding).decode('utf-8'), 'lxml') \
            if content_response else bs('', 'lxml')

        announcement_table_list = content_soup.find_all(class_='MsoNormalTable') + content_soup.find_all(
            class_='MsoTableGrid')

        if len(announcement_table_list) > 0:
            if announcement_url in \
                    ['http://www.cbrc.gov.cn/liaoning/docPcjgView/AE058700118D418289A593678194B39C/21.html',
                     'http://www.cbrc.gov.cn/liaoning/docPcjgView/B87E91987DC646669766001838EFAC37/21.html',
                     'http://www.cbrc.gov.cn/liaoning/docPcjgView/B13042B03AFF4A03A15FF23A52A4644B/21.html',
                     'http://www.cbrc.gov.cn/liaoning/docPcjgView/80319C6B680B45B494E7B0BD47352747/21.html',
                     'http://www.cbrc.gov.cn/liaoning/docPcjgView/79A2A6CBF3B54223AAC7AF31A27A0A57/21.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/47A2A2FD6B8648A883526A2ECC36AE00/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/41443E6892314A3C94A630FD0BB8B7D4/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/9441257C49914418A284A8F0EFA4B756/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/F997F0D8DC294177AA12E7A26D30CF65/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/5E2FF6C871EC4E068B94A69C09AF3727/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/A7A39F8578194643A49B74CEE91E25CC/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/69A5570D9FF046C586AFBACE53F9611D/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/01A419E65BFC402B8EE52EDB5048C800/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/186D2CBA76314BCC8CEDDCB3EA998643/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/9EA2339A8CA24B9BA4D603023B2E9305/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/68323D8F442D4BF990397A0D489746DF/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/69A8E3B32CB64B3AB8FF73B03896C995/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/7B43945E610B4FE59ED5B673CE76D8F1/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/7E4321407D13435D99064AA007A538B7/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/C7068491491E47038634DEBA7162DFB5/20.html',
                     'http://www.cbrc.gov.cn/shaanxi/docPcjgView/4609A4C5AB914EB68E4D480D34CBAF45/20.html'
                     ]:
                final_announcement_table_list = announcement_table_list[:1]
            else:
                final_announcement_table_list = [ee for ee in announcement_table_list if
                                                 (len(ee.find_all(class_='MsoNormalTable')) == 0 and len(
                                                     ee.find_all(class_='MsoTableGrid')) == 0)]
        else:
            if len(content_soup.find_all(class_='Section0')) > 0:
                final_announcement_table_list = content_soup.find(class_='Section0').find_all('table')
            else:
                final_announcement_table_list = []

        if len(final_announcement_table_list) > 0:
            if db.parsed_data.find(
                    {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                oss_file_map = {
                    'origin_url': announcement_url,
                    'oss_file_origin_url': announcement_url,
                    'origin_url_id': each_cbrc_document['_id'],
                    'oss_file_type': 'html',
                    'oss_file_name': announcement_title,
                    'oss_file_content': content_response.text.encode(content_response.encoding).decode('utf-8'),
                    'parsed': False
                }
                insert_response = db.parsed_data.insert_one(oss_file_map)
                file_id = insert_response.inserted_id
                oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                             content_response.text.encode(content_response.encoding).decode('utf-8'))
                db.cbrc_data.update_one({'_id': each_cbrc_document['_id']}, {'$set': {'status': 'parsed'}})
            else:
                db.cbrc_data.update_one({'_id': each_cbrc_document['_id']}, {'$set': {'status': 'parsed'}})
                file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                   'oss_file_origin_url': announcement_url})['_id']

            # 广东处罚
            if announcement_url in [
                'http://www.cbrc.gov.cn/guangdong/docPcjgView/0C0C87BDA4C3431B925FF7BC2461FA55/17.html'
            ]:
                for each_tr in final_announcement_table_list[0].find_all('tr')[4:]:
                    title = '行政处罚信息公开表（' + each_tr.find_all('td')[2].text + '）'
                    publish_date = each_tr.find_all('td')[1].text[:4] + '年' + each_tr.find_all('td')[1].text[
                                                                              4:6] + '月' + each_tr.find_all('td')[
                                                                                               1].text[6:8] + '日'
                    publish_date = publish_date.replace('年0', '年').replace('月0', '月')
                    result_map = {
                        'announcementTitle': title.replace('\n', ''),
                        'announcementOrg': '广东银监局',
                        'announcementDate': publish_date,
                        'announcementCode': each_tr.find_all('td')[2].text.replace('\n', ''),
                        'facts': each_tr.find_all('td')[4].text,
                        'defenseOpinion': '',
                        'defenseResponse': '',
                        'litigant': each_tr.find_all('td')[3].text,
                        'punishmentBasement': '',
                        'punishmentDecision': (
                                '依据' + each_tr.find_all('td')[5].text +
                                '，' + each_tr.find_all('td')[6].text).replace('。，', '，').replace('，，', '，'),
                        'type': '行政处罚决定',
                        'oss_file_id': file_id,
                        'status': 'not checked'
                    }
                    if db.announcement.find({'announcementTitle': title, 'oss_file_id': file_id}).count() == 0:
                        db.announcement.insert_one(result_map)
                        logger.info('广东银监局 数据解析 ' + ' -- 数据导入完成')
                    else:
                        logger.info('广东银监局 数据解析 ' + ' -- 数据已经存在')
                    db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                    logger.info('广东银监局 数据解析 ' + ' -- 修改parsed完成')
            else:
                for each_table in final_announcement_table_list:
                    if each_table.text.strip() != '':
                        parse_table(file_id, each_table, announcement_title, each_cbrc_document['publishDate'])
        else:
            if len([each_a for each_a in content_soup.find(class_='n_word').find_all('a') if
                    each_a.attrs.get('href', '') != '']) > 0:
                for each_file_link in [each_a for each_a in content_soup.find(class_='n_word').find_all('a') if
                                       each_a.attrs.get('href', '') != '']:
                    if each_file_link.attrs['href'].endswith('.pdf'):
                        pdf_url = urljoin(announcement_url, each_file_link.attrs['href'])
                        pdf_response = request_site_page(pdf_url)
                        if not pdf_response:
                            logger.error('网页请求错误')
                            continue

                        with open('./test/tmp.pdf', 'wb') as f:
                            f.write(pdf_response.content)
                        content_text = pdf_to_text('./test/tmp.pdf')
                        oss_file_type = 'pdf'

                        with open('./test/tmp.pdf', 'rb') as pdf_file:
                            file_content = pdf_file.read()
                        os.remove('./test/tmp.pdf')
                        if db.parsed_data.find(
                                {'origin_url': announcement_url, 'oss_file_origin_url': pdf_url}).count() == 0:
                            oss_file_map = {
                                'origin_url': announcement_url,
                                'oss_file_origin_url': pdf_url,
                                'origin_url_id': each_cbrc_document['_id'],
                                'oss_file_type': oss_file_type,
                                'oss_file_name': announcement_title,
                                'oss_file_content': file_content,
                                'parsed': False
                            }
                            insert_response = db.parsed_data.insert_one(oss_file_map)
                            file_id = insert_response.inserted_id
                            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.' + oss_file_type,
                                         file_content)
                            db.cbrc_data.update_one({'_id': each_cbrc_document['_id']}, {'$set': {'status': 'parsed'}})
                        else:
                            db.cbrc_data.update_one({'_id': each_cbrc_document['_id']}, {'$set': {'status': 'parsed'}})
                            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                               'oss_file_origin_url': pdf_url})['_id']
                        if content_text.strip() == '':
                            continue
                        parse_text(file_id, content_text, announcement_title, each_cbrc_document['publishDate'],
                                   each_cbrc_document['origin'])
            else:
                if db.parsed_data.find(
                        {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                    oss_file_map = {
                        'origin_url': announcement_url,
                        'oss_file_origin_url': announcement_url,
                        'origin_url_id': each_cbrc_document['_id'],
                        'oss_file_type': 'html',
                        'oss_file_name': announcement_title,
                        'oss_file_content': content_response.text.encode(content_response.encoding).decode('utf-8'),
                        'parsed': False
                    }
                    insert_response = db.parsed_data.insert_one(oss_file_map)
                    file_id = insert_response.inserted_id
                    oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                                 content_response.text.encode(content_response.encoding).decode('utf-8'))
                    db.cbrc_data.update_one({'_id': each_cbrc_document['_id']}, {'$set': {'status': 'parsed'}})
                else:
                    db.cbrc_data.update_one({'_id': each_cbrc_document['_id']}, {'$set': {'status': 'parsed'}})
                    file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                       'oss_file_origin_url': announcement_url})['_id']

                if content_soup.find(class_='Section1'):
                    content_text = content_soup.find(class_='Section1').text.strip()
                elif content_soup.find(class_='WordSection1'):
                    content_text = content_soup.find(class_='WordSection1').text.strip()
                elif content_soup.find(class_='Section0'):
                    content_text = content_soup.find(class_='Section0').text.strip()
                else:
                    content_text = ''
                if content_text.strip() == '':
                    continue
                parse_text(file_id, content_text, announcement_title,
                           each_cbrc_document['publishDate'], each_cbrc_document['origin'])


def parse():
    parse_cbrc()
    parse_office_cbrc()


if __name__ == "__main__":
    parse()
