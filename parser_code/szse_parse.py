from pymongo import MongoClient
import re
import os
import sys
import docx
import subprocess

from init import logger_init, config_init
from utility import cn2dig, get_year, request_site_page, get_chinese_proportion
from oss_utils import init_ali_oss, oss_add_file
from pdf2html import pdf_to_text, pdf_ocr_to_text

logger = logger_init('深交所 数据解析')
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


def parse_szse():
    for document in db.szse_data.find({'status': {'$nin': ['ignored']}}, no_cursor_timeout=True):
        try:
            announcement_url = str(document['url'])
            announcement_title = document['title']
            origin_url_id = document['_id']
            announcement_type = document['type']

            if db.szse_data.find(
                    {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1 and \
                    db.announcement.find({'oss_file_id': db.parsed_data.find(
                        {'origin_url_id': origin_url_id})[0][
                        '_id']}).count() > 0:
                continue

            logger.info('Url to parse: ' + announcement_url)

            if document['url'].endswith('pdf') or document['url'].endswith('PDF'):
                response = request_site_page(announcement_url)
                if response is None:
                    logger.error('网页请求错误')
                    continue
                with open('./test/tmp.pdf', 'wb') as f:
                    f.write(response.content)
                content_text = pdf_to_text('./test/tmp.pdf')
                if_ocr_flag = False
                logger.info('pdf to text: \n' + content_text)

                if get_chinese_proportion(content_text)[0] < 0.6 or get_chinese_proportion(content_text)[1] < 30\
                        or announcement_url in \
                        ['http://reportdocs.static.szse.cn/UpFiles/jgsy/gkxx_jgsy_00040339134.pdf',
                         'http://reportdocs.static.szse.cn/UpFiles/jgsy/gkxx_jgsy_00016038838.pdf',
                         'http://reportdocs.static.szse.cn/UpFiles/jgsy/gkxx_jgsy_00076038350.pdf',
                         'http://reportdocs.static.szse.cn/UpFiles/zqjghj/zqjghj_b0aec72c-b44a-44cf-ae93-56f0138a080b.pdf',
                         'http://reportdocs.static.szse.cn/UpFiles/zqjghj/zqjghj_06773f55-8bb0-44b9-989f-9d11659e56dd.pdf',
                         'http://reportdocs.static.szse.cn/UpFiles/zqjghj/zqjghj_86007674-c461-426a-9250-88319b86b317.pdf',
                         'http://reportdocs.static.szse.cn/UpFiles/zqjghj/zqjghj_b6af0981-c2ad-4090-9595-e03f60c1a7dd.pdf',
                         'http://reportdocs.static.szse.cn/UpFiles/jgsy/gkxx_jgsy_00251244460.pdf']:
                    content_text, if_ocr_flag = pdf_ocr_to_text('./test/tmp.pdf')
                    content_text = content_text.replace('査', '查')
                    logger.info('ocr pdf to text: \n' + content_text)

                if content_text == '':
                    continue

                with open('./test/tmp.pdf', 'rb') as pdf_file:
                    pdf_content = pdf_file.read()

                if db.parsed_data.find(
                        {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                    if not if_ocr_flag:
                        oss_file_map = {
                            'origin_url': announcement_url,
                            'oss_file_origin_url': announcement_url,
                            'origin_url_id': origin_url_id,
                            'oss_file_type': 'pdf',
                            'oss_file_name': announcement_title,
                            'oss_file_content': pdf_content,
                            'parsed': False
                        }
                    else:
                        oss_file_map = {
                            'origin_url': announcement_url,
                            'oss_file_origin_url': announcement_url,
                            'origin_url_id': origin_url_id,
                            'oss_file_type': 'pdf',
                            'oss_file_name': announcement_title,
                            'oss_file_content': pdf_content,
                            'parsed': False,
                            'if_ocr': True,
                            'ocr_result': content_text
                        }
                    insert_response = db.parsed_data.insert_one(oss_file_map)
                    file_id = insert_response.inserted_id
                    oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.pdf', pdf_content)
                    db.szse_data.update_one({'_id': origin_url_id}, {'$set': {'status': 'parsed'}})
                else:
                    db.szse_data.update_one({'_id': origin_url_id}, {'$set': {'status': 'parsed'}})
                    if if_ocr_flag:
                        db.parsed_data.update_one({'origin_url': announcement_url,
                                                   'oss_file_origin_url': announcement_url},
                                                  {'$set': {'if_ocr': True, 'ocr_result': content_text}})
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
                        'announcementOrg': '深交所',
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
                    if document.get('number', '') != '':
                        announcement_code = document.get('number', '')
                        if re.search(r'\n(当事人[\s\S]*?)经查', content_text):
                            litigant = re.search(r'\n(当事人[\s\S]*?)经查', content_text).group(1).strip()
                        else:
                            litigant = re.search(r'\n(.*?)\n'
                                                 r'(经查|'
                                                 r'你公司作为公司债券发行人，未于规定时间之前披露\d{4}年半?年度报告)',
                                                 content_text).group(1).strip()
                    else:
                        document_code_compiler = re.compile(
                            r'\n((.*?关注函|.*?监管函).\d{4}.第.*?\d+.*?号)\n', re.MULTILINE)
                        if document_code_compiler.search(content_text):
                            announcement_code = document_code_compiler.search(content_text).group(1).strip()
                            litigant = re.search(
                                announcement_code.replace(r'(', r'\(').replace(r')', r'\)').
                                replace(r'.', r'\.').replace(r'[', r'\[').replace(
                                    r']', r'\]').replace(r'*', r'\*').replace(r'+', r'\+') + r'\n(.*?)\n',
                                content_text).group(1).strip()
                        else:
                            announcement_code = ''
                            litigant = re.search(r'\n(当事人[\s\S]*?)经查', content_text).group(1).strip()
                    punishment_decision = re.search(
                        r'(((根据|属于|基于|考虑|按照|依照|鉴于|综上|据此).*?(依据|根据|依照|按照).*?第.*?[条之].*'
                        r'(规定)?.*?((本所|我部)(另)?(做出|作出)(如下|以下).*?决定|'
                        r'对.*?予以公开谴责|我部决定|我部对|本所决定对.*作出|对.*?给予.*?处分|本所作出如下处分决定)|'
                        r'根据.*?规定，(本所|我部)做出如下.*?决定|'
                        r'鉴于上述违规行为，现对你公司予以书面警示，|'
                        r'但鉴于情节较轻，未造成严重后果，我部决定|公司应当引以为戒|'
                        r'鉴于.*?我部决定|我部对此予以关注。|(据此，)?我部决定对.*?予以监管关注。|'
                        r'对于公司和董事会秘书的上述违规事实和情节，我部予以监管关注。|因此我部决定对|我部对此表示关注。|我部对中科渝祥予以监管关注|'
                        r'考虑到你公司刚完成重大资产置换，新一届董事会对相关规则的掌握存在缺陷，根据《上海证券交易所纪律处分与监管措施实施办法》第9条和第64条的规定，我部对|'
                        r'(鉴于|基于)(上述|前述)(违规)?(事实|行为)和情节,(根据|经)|'
                        r'综上,经上海证券交易所\(以下筒称本所\)纪律处分委员\n会审核,根据|'
                        r'经上海证券交易所\(以下简称本所\)纪律处分委员会审核通\n过,根据|'
                        r'综上,根据《股票上市规则》第17.2条、第17.3条、第\n7.4条和《上海证券交易所纪律处分和监管措施实施办法》等\n相关规定|'
                        r'鉴于原告于2017年6月13日申请撤诉,法院于2017年6\n月14日裁定准予|'
                        r'鉴于上述事实和情节,根\n据|'
                        r'鉴于公司副总裁郝廷木违规买入的股票数量和金额较大,且\n同时构成两项违规,经|'
                        r'鉴于公司副总经理王飚违规卖出的股票数量和金额较大,经\n本所纪律处分委员会审核通过,根据|'
                        r'鉴于上述事实和情形,经本所纪律处分委员会审核通过,根\n据|'
                        r'鉴于上述事实和理由,我部根据|'
                        r'综上,根据《股票上市规则》第17.1条和《上海证券交易所纪\n律处分和监管措施实施办法》的有关规定,我部做出|'
                        r'本\n?所\n?希\n?望|请公司及控股股\n东充分重视上述问题|'
                        r'本所对此表示关注|请\n?你(们)?\n?(公司)?(董\n?事\n?会\n?)?充\n?分\n?重\n?视\n?上\n?述\n?[问可]\n?题|'
                        r'我部提醒你|本所要求你|本所再次提醒|请你公司及时改正|请你公司充分重视|请你院充分重视上述问题|'
                        r'请你公\n司、你公司实际控制人及其关联方充分重视上述问题|'
                        r'请你公司董事会及董事会\n秘书充分重视上述问题|同时,提醒你们|'
                        r'请你\n?公司董事会及相\n?关当\n?事人充分重视上述问题|'
                        r'请.*?充分重视上述问题|请你公司及控股\n股东充分重视上述问题|'
                        r'请你公\n?司\n?董事会充分重视上述问题|请你企业充分重视\n上述问题|'
                        r'你(公司)?\n?(董事会)?\n?应充\n?分重视\n?上述\n?问题|'
                        r'请你\n们充分重视上述问题|鉴于你的上述卖出行为属于误操作|'
                        r'鉴于你(公司)?是(操作错误|误操作)导致短线交易|我部对此表示|现对你公司出\n具监管函|'
                        r'请及时整改,尽快|我部现对你公司、\n?时任董事程圣德采取出具监管函的监管措施|'
                        r'请你公司董事会、\n?贾跃亭充分重视上述问题|'
                        r'请你公司董事会、实际控制人吴艳、王麒诚、董事会秘书\n方路遥充分重视上述问题|'
                        r'希望公司、东方集团及公司全体董事、监事、高级管理人员吸取|'
                        r'我\n?[部所](对(此|上述事项|上述问题))?予\n?以(书面警示|监管关注|关注)|'
                        r'鉴于上述违规行为,现对你公司予以书面警示|'
                        r'请你\n?公司\n?及实际控制人充分重视上述问题|'
                        r'希望你们吸取教训,严格遵守《证券法》《公司法》|'
                        r'请中\n科东海和中科芙蓉充分重视上述问题|'
                        r'请你公司重视上述问题|请充分重视上\n述问题|'
                        r'希望公司、鑫鼎盛控股及公司全体董事、监事、高级管理人员吸\n取教训|'
                        r'请你公司和相\n关当事人充分重视上述问题|请你公司及全体董事\n监事、高级管理人员严格遵守承诺,尽快披露|'
                        r'请你公司按本所《股票上市规则》第1.37条的规定补充覆行信\n息技露义务|'
                        r'请你公司\n?(充分|高度)重视上述(问题|事项)|请你公司董\n事会及相关当事人充分重视上述问题|'
                        r'现对你公司采取出具监管函的监管措施。你公司应当充分重视上|'
                        r'请你公司董事会及相关人员充\n分重视上述问题|'
                        r'请公司董事会充分重视上述\n问题|'
                        r'请你公司董事会及相关当事人\n充分重视上述问题|'
                        r'请你公司及全体董事、\n?监事、\n?高级管理人员(吸取教训|严格遵守承诺)|'
                        r'希望.*?吸取教训|\n同时，提醒你们|'
                        r'\n我部提醒：上市公司控股股东、实际控制人必须按照国家法律|'
                        r'请你们严格按照承诺及时将股份进行解除质押，转至公司董事会所设立的专门账户|'
                        # 为ocr结果匹配的规则
                        r'鉴于.*?上述违规事实[和及]情节,(根据|依据)本所.*?\n.*?第.*?条的(相关)?规定|'
                        r'鉴于上述违规事实及情节,依据本所《上市规则》规则第9.2\n条的规定|'
                        r'于上述违规事实及情节,依据本所《非公开发行公司债券业)'
                        r'[\s\S]*?)\n'
                        r'(特此函|深圳证券交易所|创业板公司管理部|中小板公司管理部|'
                        r'201\d{2,4}$)', content_text).group(1).strip()

                    truth_text_str = litigant.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.'). \
                                         replace(r'[', r'\[').replace(r']', r'\]'). \
                                         replace(r'*', r'\*').replace(r'+', r'\+') + \
                                     r'(:|：)?(\n[\s\S]*?)' \
                                     r'((\n|。)[^。\n]*?行为[^。\n]*?违反|综上|你公\n司上述行为违反了|' + \
                                     punishment_decision.replace(r'(', r'\(').replace(r')', r'\)'). \
                                         replace(r'.', r'\.').replace(r'[', r'\[').replace(r']', r'\]'). \
                                         replace(r'*', r'\*').replace(r'+', r'\+') + ')'

                    truth_compiler = re.compile(truth_text_str, re.MULTILINE)
                    truth_list = truth_compiler.findall(content_text)
                    truth = '\n'.join([kk[1] for kk in truth_list])

                    punishment_basis_text_str = \
                        truth_list[-1][1].replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                            .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                            .replace('+', '\+') + r'([\s\S]*?)' \
                        + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                            .replace(r'.', r'\.').replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
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
                        m = re.match("([0-9零一ー二ニ两三四五六七八九十O-]+年)([0-9一ー二ニ两三四五六七八九十]+)月([0-9一ー二ニ两三四五六七八九十+]+)(号|日)",
                                     publish_date)
                        if m:
                            real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(
                                cn2dig(m.group(3))) + '日'
                        else:
                            real_publish_date = ''
                    else:
                        if 'webStorageTime' in document.keys():
                            publish_date = document['webStorageTime']
                            real_publish_date = str(publish_date.year) + '年' + str(publish_date.month) + '月' + str(
                                publish_date.day) + '日'
                        else:
                            real_publish_date = ''

                    result_map = {
                        'announcementTitle': announcement_title,
                        'announcementOrg': '深交所',
                        'announcementDate': real_publish_date,
                        'announcementCode': announcement_code,
                        'facts': truth.strip(),
                        'defenseOpinion': '',
                        'defenseResponse': '',
                        'litigant': litigant[:-1].strip() if litigant[-1] == '：' or litigant[
                            -1] == ':' else litigant.strip(),
                        'punishmentBasement': punishment_basis,
                        'punishmentDecision': punishment_decision,
                        'type': announcement_type,
                        'oss_file_id': file_id,
                        'status': 'not checked'
                    }
                if db.announcement.find(
                        {'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
                    logger.info(result_map)
                    db.announcement.insert_one(result_map)
                    logger.info('深交所 数据解析 ' + ' -- 数据导入完成')
                else:
                    logger.info('深交所 数据解析 ' + ' -- 数据已经存在')

                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('深交所 数据解析 ' + ' -- 修改parsed完成')
                logger.info('删除TMP文件')
                os.remove('./test/tmp.pdf')
            elif document['url'].endswith('docx') or document['url'].endswith('doc'):
                response = request_site_page(announcement_url)
                if response is None:
                    logger.error('网页请求错误')
                    continue
                if document['url'].endswith('docx'):
                    with open('./test/tmp.' + 'docx', 'wb') as f:
                        f.write(response.content)
                else:
                    with open('./test/tmp.' + 'doc', 'wb') as f:
                        f.write(response.content)

                if not os.path.exists('./test/tmp.docx'):
                    shell_str = '/usr/local/bin/soffice --headless --convert-to docx ' + \
                                './test/tmp.doc' + ' --outdir ./test'
                    process = subprocess.Popen(shell_str.replace(r'(', r'\(').replace(r')', r'\)'),
                                               shell=True, stdout=subprocess.PIPE)
                    process.communicate()

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
                    db.szse_data.update_one({'_id': origin_url_id}, {'$set': {'status': 'parsed'}})
                else:
                    db.szse_data.update_one({'_id': origin_url_id}, {'$set': {'status': 'parsed'}})
                    file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                       'oss_file_origin_url': announcement_url})['_id']

                doc = docx.Document('./test/tmp.docx')
                full_text = []
                for para in doc.paragraphs:
                    full_text.append(para.text)
                content_text = '\n'.join(full_text)
                content_text = re.sub('\n+', '\n', content_text)
                logger.info(content_text)
                if '监管类型:监管工作函' in content_text:
                    announcement_title = re.search(r'标题:(.*?)\n', content_text).group(1).strip()
                    litigant = re.search(r'(证券代码[\s\S]*?)监管类型', content_text).group(1).strip() + '\n' + re.search(
                        r'(涉及对象.*?)\n', content_text).group(1).strip()
                    publish_date = document['webStorageTime']
                    real_publish_date = str(publish_date.year) + '年' + str(publish_date.month) + '月' + str(
                        publish_date.day) + '日'
                    punishment_decision = re.search(r'处理事由:([\s\S]*?)$', content_text).group(1).strip()

                    result_map = {
                        'announcementTitle': announcement_title,
                        'announcementOrg': '深交所',
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
                    if document['type'] == '监管措施':
                        if re.search(r'(监管|关注)\n?函\n(.*?)\n', content_text):
                            announcement_code = re.search(r'(监管|关注)\n?函\n(.*?)\n', content_text).group(2).strip()
                            litigant = re.search(
                                announcement_code.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.')
                                .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*')
                                .replace(r'+', r'\+') +
                                r'\n(.*?)\n', content_text).group(1).strip()
                        else:
                            announcement_code = ''
                            litigant = re.search(r'\n(当事人[\s\S]*?)经查', content_text).group(1).strip()
                    else:
                        announcement_code = document.get('number', '')
                        litigant = re.search(r'\n(当事人[\s\S]*?)经查', content_text).group(1).strip()

                    punishment_decision = re.search(
                        r'(((根据|属于|基于|考虑|按照|依照|鉴于|综上|据此).*?'
                        r'(依据|根据|依照|按照).*?第.*?[条之].*规定.*?((本所|我部)(另)?(做出|作出)(如下|以下).*?决定|'
                        r'对.*?予以公开谴责|我部决定|我部对|本所决定对.*?给予.*?处分|本所作出对.*?处分)|'
                        r'根据.*?规定，(本所|我部)做出如下.*?决定|'
                        r'但鉴于情节较轻，未造成严重后果，我部决定|公司应当引以为戒|'
                        r'鉴于.*?我部决定|我部对此予以关注。|(据此，)?我部决定对.*?予以监管关注。|'
                        r'对于公司和董事会秘书的上述违规事实和情节，我部予以监管关注。|因此我部决定对|我部对此表示关注。|我部对中科渝祥予以监管关注|'
                        r'考虑到你公司刚完成重大资产置换，新一届董事会对相关规则的掌握存在缺陷，根据《上海证券交易所纪律处分与监管措施实施办法》第9条和第64条的规定，我部对|'
                        r'(鉴于|基于)(上述|前述)(违规)?(事实|行为)和情节,(根据|经)|'
                        r'综上,经上海证券交易所\(以下筒称本所\)纪律处分委员\n会审核,根据|'
                        r'经上海证券交易所\(以下简称本所\)纪律处分委员会审核通\n过,根据|'
                        r'综上,根据《股票上市规则》第17.2条、第17.3条、第\n7.4条和《上海证券交易所纪律处分和监管措施实施办法》等\n相关规定|'
                        r'鉴于原告于2017年6月13日申请撤诉,法院于2017年6\n月14日裁定准予|'
                        r'鉴于上述事实和情节,根\n据|'
                        r'鉴于公司副总裁郝廷木违规买入的股票数量和金额较大,且\n同时构成两项违规,经|'
                        r'鉴于公司副总经理王飚违规卖出的股票数量和金额较大,经\n本所纪律处分委员会审核通过,根据|'
                        r'鉴于上述事实和情形,经本所纪律处分委员会审核通过,根\n据|'
                        r'鉴于上述事实和理由,我部根据|'
                        r'综上,根据《股票上市规则》第17.1条和《上海证券交易所纪\n律处分和监管措施实施办法》的有关规定,我部做出|'
                        r'本\n?所\n?希\n?望|请公司及控股股\n东充分重视上述问题|'
                        r'本所对此表示关注|请\n?你(们)?\n?(公司)?(董\n?事\n?会\n?)?充\n?分\n?重\n?视\n?上\n?述\n?[问可]\n?题|'
                        r'我部提醒你|本所要求你|本所再次提醒|请你公司及时改正|请你公司充分重视|请你院充分重视上述问题|'
                        r'请你公\n司、你公司实际控制人及其关联方充分重视上述问题|'
                        r'请你公司董事会及董事会\n秘书充分重视上述问题|同时,提醒你们|'
                        r'请你\n?公司董事会及相\n?关当\n?事人充分重视上述问题|'
                        r'请.*?充分重视上述问题|请你公司及控股\n股东充分重视上述问题|'
                        r'请你公\n?司\n?董事会充分重视上述问题|请你企业充分重视\n上述问题|'
                        r'你(公司)?\n?(董事会)?\n?应充\n?分重视\n?上述\n?问题|'
                        r'请你\n们充分重视上述问题|鉴于你的上述卖出行为属于误操作|'
                        r'鉴于你(公司)?是(操作错误|误操作)导致短线交易|我部对此表示|现对你公司出\n具监管函|'
                        r'请及时整改,尽快|我部现对你公司、\n?时任董事程圣德采取出具监管函的监管措施|'
                        r'请你公司董事会、\n?贾跃亭充分重视上述问题|'
                        r'请你公司董事会、实际控制人吴艳、王麒诚、董事会秘书\n方路遥充分重视上述问题|'
                        r'希望公司、东方集团及公司全体董事、监事、高级管理人员吸取|'
                        r'我\n?[部所](对(此|上述事项|上述问题))?予\n?以(书面警示|监管关注|关注)|'
                        r'鉴于上述违规行为,现对你公司予以书面警示|'
                        r'请你\n?公司\n?及实际控制人充分重视上述问题|'
                        r'希望你们吸取教训,严格遵守《证券法》《公司法》|'
                        r'请中\n科东海和中科芙蓉充分重视上述问题|'
                        r'请你公司重视上述问题|请充分重视上\n述问题|'
                        r'希望公司、鑫鼎盛控股及公司全体董事、监事、高级管理人员吸\n取教训|'
                        r'请你公司和相\n关当事人充分重视上述问题|'
                        r'请你公司\n充分重视上述问题|'
                        r'请公司董事会充分重视上述\n问题|'
                        r'请你公司及全体董事、监事、\n高级管理人员吸取教训,严格遵守《证券法》)'
                        r'[\s\S]*?)\n'
                        r'(特此函|( *)?深圳证券交易所)', content_text).group(1).strip()

                    truth_text_str = litigant.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                         .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                         .replace(r'+', r'\+') + \
                                     r'(\n[\s\S]*?)' \
                                     r'((\n|。)[^。\n]*?行为[^。\n]*?违反|综上|你公\n司上述行为违反了|' + \
                                     punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                         .replace(r'.', r'\.').replace('+', '\+') \
                                         .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') + ')'

                    truth_compiler = re.compile(truth_text_str, re.MULTILINE)
                    truth_list = truth_compiler.findall(content_text)
                    truth = '\n'.join([kk[0] for kk in truth_list]).replace('査', '查')

                    punishment_basis_text_str = truth_list[-1][0].replace(r'(', r'\(').replace(r')', r'\)') \
                                                    .replace(r'.', r'\.').replace(r'*', r'\*') \
                                                    .replace(r'[', r'\[').replace(r']', r'\]').replace(r'+', r'\+') \
                                                + r'([\s\S]*?)' \
                                                + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                                    .replace(r'.', r'\.').replace(r'*', r'\*') \
                                                    .replace(r'[', r'\[').replace(r']', r'\]').replace(r'+', r'\+')
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
                        m = re.match("([0-9零一ー二ニ两三四五六七八九十O○-]+年)([0-9一ー二ニ两三四五六七八九十]+)月([0-9一ー二ニ两三四五六七八九十+]+)(号|日)",
                                     publish_date)
                        real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(
                            cn2dig(m.group(3))) + '日'
                    else:
                        publish_date = document['webStorageTime']
                        real_publish_date = str(publish_date.year) + '年' + str(publish_date.month) + '月' + str(
                            publish_date.day) + '日'

                    result_map = {
                        'announcementTitle': announcement_title,
                        'announcementOrg': '深交所',
                        'announcementDate': real_publish_date,
                        'announcementCode': announcement_code,
                        'facts': truth.strip(),
                        'defenseOpinion': '',
                        'defenseResponse': '',
                        'litigant': litigant[:-1].strip() if litigant[-1] == '：' or litigant[
                            -1] == ':' else litigant.strip(),
                        'punishmentBasement': punishment_basis,
                        'punishmentDecision': punishment_decision,
                        'type': announcement_type,
                        'oss_file_id': file_id,
                        'status': 'not checked'
                    }
                if db.announcement.find(
                        {'announcementTitle': announcement_title, 'oss_file_id': file_id, }).count() == 0:
                    logger.info(result_map)
                    db.announcement.insert_one(result_map)
                    logger.info('深交所 数据解析 ' + ' -- 数据导入完成')
                else:
                    logger.info('深交所 数据解析 ' + ' -- 数据已经存在')
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('深交所 数据解析 ' + ' -- 修改parsed完成')
                logger.info('删除TMP文件')
                if os.path.exists('./test/tmp.doc'):
                    os.remove('./test/tmp.doc')
                if os.path.exists('./test/tmp.docx'):
                    os.remove('./test/tmp.docx')
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
    parse_szse()


if __name__ == "__main__":
    parse()
