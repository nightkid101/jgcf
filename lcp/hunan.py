import re
import os
import subprocess
import docx

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss
from init import config_init
from urllib.parse import urljoin

ali_bucket = init_ali_oss()
config = config_init()


def hunan_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '湖南保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if '行政处罚实施情况' in announcement_title or '行政处罚及其他监管措施实施情况通报' in announcement_title:
            logger.warning(announcement_url + ' ' + announcement_title + ' 与监管处罚无关')
            db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('湖南保监局' + ' 无关数据' + ' -- 修改status完成')
            continue

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('湖南保监局 ' + 'Url to parse: %s' % announcement_url)

        r = request_site_page(announcement_url)
        if r is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_soup = bs(r.content, 'lxml') if r else bs('', 'lxml')

        table_content = content_soup.find(id='tab_content')
        if not table_content:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_text = get_content_text(table_content.find_all('tr')[3])
        if content_text == '':
            continue
        title = table_content.find_all('tr')[0].text.strip()

        if content_text == '无':
            continue

        if '行政处罚公开信息' in title or '行政处罚事项' in title:
            if '.doc' in content_text:
                doc_link = urljoin(announcement_url, table_content.find_all('tr')[3].find('a').attrs['href'])
                response = request_site_page(doc_link)

                with open('./test/tmp.doc', 'wb') as f:
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
                        {'origin_url': announcement_url, 'oss_file_origin_url': doc_link}).count() == 0:
                    oss_file_map = {
                        'origin_url': announcement_url,
                        'oss_file_origin_url': doc_link,
                        'origin_url_id': each_circ_data['_id'],
                        'oss_file_type': 'docx',
                        'oss_file_name': announcement_title,
                        'oss_file_content': docx_content,
                        'parsed': False
                    }
                    insert_response = db.parsed_data.insert_one(oss_file_map)
                    file_id = insert_response.inserted_id
                    oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.docx', docx_content)
                    db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
                else:
                    db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
                    file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                       'oss_file_origin_url': doc_link})['_id']

                doc = docx.Document('./test/tmp.docx')
                result_map_list = []
                tables = doc.tables
                for table in tables:
                    for row in table.rows:
                        if len(row.cells) == 7 and row.cells[0].text != '序号':
                            real_title = '湖南银保监局行政处罚决定书（' + row.cells[2].text + '）'
                            result_map = {
                                'announcementTitle': real_title,
                                'announcementOrg': '湖南银保监局',
                                'announcementDate': '2007年' + row.cells[1].text if '2007' in row.cells[
                                    2].text else '2008年' + row.cells[1].text,
                                'announcementCode': row.cells[2].text,
                                'facts': '',
                                'defenseOpinion': '',
                                'defenseResponse': '',
                                'litigant': row.cells[3].text,
                                'punishmentBasement': row.cells[3].text + '上述行为违反了' + row.cells[3].text,
                                'punishmentDecision': '依据' + row.cells[5].text + '，' + '我局对' + row.cells[
                                    3].text + '作出以下处罚：' + row.cells[6].text,
                                'type': '行政处罚决定',
                                'oss_file_id': file_id,
                                'status': 'not checked'
                            }
                            logger.info(result_map)
                            if db.announcement.find(
                                    {'announcementTitle': result_map['announcementTitle'],
                                     'oss_file_id': file_id,
                                     'litigant': result_map['litigant']}).count() == 0:
                                db.announcement.insert_one(result_map)
                                logger.info('湖南保监局 数据解析 ' + ' -- 数据导入完成')
                            else:
                                logger.info('湖南保监局 数据解析 ' + ' -- 数据已经存在')
                            result_map_list.append(result_map)
                if len(result_map_list) > 0:
                    db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                    logger.info('湖南保监局 数据解析 ' + ' -- 修改parsed完成')
                else:
                    logger.warning('湖南保监局 数据解析 ' + ' -- 未解析完成')

                logger.info('删除tmp文件')
                os.remove('./test/tmp.doc')
                os.remove('./test/tmp.docx')
            else:
                if db.parsed_data.find(
                        {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                    oss_file_map = {
                        'origin_url': announcement_url,
                        'oss_file_origin_url': announcement_url,
                        'origin_url_id': each_circ_data['_id'],
                        'oss_file_type': 'html',
                        'oss_file_name': announcement_title,
                        'oss_file_content': r.text.encode(r.encoding).decode('utf-8'),
                        'parsed': False
                    }
                    insert_response = db.parsed_data.insert_one(oss_file_map)
                    file_id = insert_response.inserted_id
                    oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                                 r.text.encode(r.encoding).decode('utf-8'))
                    db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
                else:
                    db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
                    file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                       'oss_file_origin_url': announcement_url})['_id']

                out_table_content = table_content
                table_content = table_content.find_all('tr')[3].find('table')
                result_map_list = []
                static_tr = table_content.find_all('tr')[0]
                for tr in table_content.find_all('tr'):
                    if '行政处罚公开信息' not in tr.text and '行政处罚事项' not in tr.text and '文书编号' not in tr.text and \
                            len(tr.find_all('td')) > 1 and tr.find_all('td')[0].text != tr.find_all('td')[1].text:
                        if len(tr.find_all('td')) == 7:
                            if len(tr.find_all('td')) == len(static_tr.find_all('td')):
                                real_title = '湖南银保监局行政处罚决定书（' + tr.find_all('td')[1].text + '）'
                                if '/' in tr.find_all('td')[0].text:
                                    publish_date = tr.find_all('td')[0].text.split('/')[0] + '年' + \
                                                   tr.find_all('td')[0].text.split('/')[1] + '月' + \
                                                   tr.find_all('td')[0].text.split('/')[2] + '日'
                                else:
                                    publish_date = tr.find_all('td')[0].text.split('-')[0] + '年' + \
                                                   tr.find_all('td')[0].text.split('-')[1] + '月' + \
                                                   tr.find_all('td')[0].text.split('-')[2] + '日'

                                result_map = {
                                    'announcementTitle': real_title,
                                    'announcementOrg': '湖南银保监局',
                                    'announcementDate': publish_date,
                                    'announcementCode': tr.find_all('td')[1].text,
                                    'facts': tr.find_all('td')[2].text + tr.find_all('td')[3].text,
                                    'defenseOpinion': '',
                                    'defenseResponse': '',
                                    'litigant': tr.find_all('td')[2].text,
                                    'punishmentBasement': tr.find_all('td')[2].text + '上述行为违反了' + tr.find_all('td')[
                                        4].text,
                                    'punishmentDecision': '依据' + tr.find_all('td')[5].text + '，' + '我局对' +
                                                          tr.find_all('td')[
                                                              2].text + '作出以下处罚：' + tr.find_all('td')[6].text,
                                    'type': '行政处罚决定',
                                    'oss_file_id': file_id,
                                    'status': 'not checked'
                                }
                                static_tr = tr
                                if db.announcement.find_all(
                                        {'announcementTitle': result_map['announcementTitle'],
                                         'oss_file_id': file_id,
                                         'litigant': result_map['litigant']}).count() == 0:
                                    db.announcement.insert_one(result_map)
                                    logger.info('湖南保监局 数据解析 ' + ' -- 数据导入完成')
                                else:
                                    logger.info('湖南保监局 数据解析 ' + ' -- 数据已经存在')
                                result_map_list.append(result_map)
                            else:
                                if len(static_tr.find_all('td')) - len(tr.find_all('td')) == 2:
                                    real_title = '湖南银保监局行政处罚决定书（' + static_tr.find_all('td')[1].text + '）'
                                    if '/' in static_tr.find_all('td')[0].text:
                                        publish_date = static_tr.find_all('td')[0].text.split('/')[0] + '年' + \
                                                       static_tr.find_all('td')[0].text.split('/')[1] + '月' + \
                                                       static_tr.find_all('td')[0].text.split('/')[2] + '日'
                                    else:
                                        publish_date = static_tr.find_all('td')[0].text.split('-')[0] + '年' + \
                                                       static_tr.find_all('td')[0].text.split('-')[1] + '月' + \
                                                       static_tr.find_all('td')[0].text.split('-')[2] + '日'
                                    result_map = {
                                        'announcementTitle': real_title,
                                        'announcementOrg': '湖南银保监局',
                                        'announcementDate': publish_date,
                                        'announcementCode': static_tr.find_all('td')[1].text,
                                        'facts': tr.find_all('td')[0].text + tr.find_all('td')[1].text,
                                        'litigant': tr.find_all('td')[0].text,
                                        'punishmentBasement': tr.find_all('td')[0].text + '上述行为违反了' + tr.find_all('td')[
                                            2].text,
                                        'punishmentDecision': '依据' + tr.find_all('td')[3].text + '，' + '我局对' +
                                                              tr.find_all('td')[
                                                                  0].text + '作出以下处罚：' + tr.find_all('td')[4].text,
                                        'type': '行政处罚决定',
                                        'oss_file_id': file_id,
                                        'status': 'not checked',
                                        'defenseOpinion': '',
                                        'defenseResponse': ''
                                    }
                                    if db.announcement.find_all(
                                            {'announcementTitle': result_map['announcementTitle'],
                                             'oss_file_id': file_id,
                                             'litigant': result_map['litigant']}).count() == 0:
                                        db.announcement.insert_one(result_map)
                                        logger.info('湖南保监局 数据解析 ' + ' -- 数据导入完成')
                                    else:
                                        logger.info('湖南保监局 数据解析 ' + ' -- 数据已经存在')
                                    result_map_list.append(result_map)
                                else:
                                    real_title = '湖南银保监局行政处罚决定书（' + static_tr.find_all('td')[1].text + '）'
                                    if '/' in static_tr.find_all('td')[0].text:
                                        publish_date = static_tr.find_all('td')[0].text.split('/')[0] + '年' + \
                                                       static_tr.find_all('td')[0].text.split('/')[1] + '月' + \
                                                       static_tr.find_all('td')[0].text.split('/')[2] + '日'
                                    else:
                                        publish_date = static_tr.find_all('td')[0].text.split('-')[0] + '年' + \
                                                       static_tr.find_all('td')[0].text.split('-')[1] + '月' + \
                                                       static_tr.find_all('td')[0].text.split('-')[2] + '日'
                                    result_map = {
                                        'announcementTitle': real_title,
                                        'announcementOrg': '湖南银保监局',
                                        'announcementDate': publish_date,
                                        'announcementCode': static_tr.find_all('td')[1].text,
                                        'facts': static_tr.find_all('td')[2].text + tr.find_all('td')[0].text,
                                        'litigant': static_tr.find_all('td')[2].text,
                                        'punishmentBasement': static_tr.find_all('td')[2].text + '上述行为违反了' +
                                                              tr.find_all('td')[
                                                                  1].text,
                                        'punishmentDecision': '依据' + tr.find_all('td')[2].text + '，' + '我局对' +
                                                              static_tr.find_all('td')[2].text +
                                                              '作出以下处罚：' + tr.find_all('td')[3].text,
                                        'type': '行政处罚决定',
                                        'oss_file_id': file_id,
                                        'status': 'not checked',
                                        'defenseOpinion': '',
                                        'defenseResponse': ''
                                    }
                                    if db.announcement.find_all(
                                            {'announcementTitle': result_map['announcementTitle'],
                                             'oss_file_id': file_id,
                                             'litigant': result_map['litigant']}).count() == 0:
                                        db.announcement.insert_one(result_map)
                                        logger.info('湖南保监局 数据解析 ' + ' -- 数据导入完成')
                                    else:
                                        logger.info('湖南保监局 数据解析 ' + ' -- 数据已经存在')
                                    result_map_list.append(result_map)
                        else:
                            if len(tr.find_all('td')) == len(static_tr.find_all('td')):
                                real_title = '湖南银保监局行政处罚决定书（' + tr.find_all('td')[0].text + '）'
                                publish_date_text = out_table_content.find_all('tr')[1].text
                                publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
                                real_publish_date = publish_date.split('-')[0] + '年' + str(
                                    int(publish_date.split('-')[1])) + '月' + str(
                                    int(publish_date.split('-')[2])) + '日'
                                result_map = {
                                    'announcementTitle': real_title,
                                    'announcementOrg': '湖南银保监局',
                                    'announcementDate': real_publish_date,
                                    'announcementCode': tr.find_all('td')[0].text,
                                    'facts': tr.find_all('td')[1].text + tr.find_all('td')[2].text,
                                    'litigant': tr.find_all('td')[1].text,
                                    'punishmentBasement': tr.find_all('td')[1].text + '上述行为违反了' + tr.find_all('td')[
                                        3].text,
                                    'punishmentDecision': '依据' + tr.find_all('td')[4].text + '，' + '我局对' +
                                                          tr.find_all('td')[
                                                              1].text + '作出以下处罚：' + tr.find_all('td')[5].text,
                                    'type': '行政处罚决定',
                                    'oss_file_id': file_id,
                                    'status': 'not checked',
                                    'defenseOpinion': '',
                                    'defenseResponse': ''
                                }
                                static_tr = tr
                                if db.announcement.find_all(
                                        {'announcementTitle': result_map['announcementTitle'],
                                         'oss_file_id': file_id,
                                         'litigant': result_map['litigant']}).count() == 0:
                                    db.announcement.insert_one(result_map)
                                    logger.info('湖南保监局 数据解析 ' + ' -- 数据导入完成')
                                else:
                                    logger.info('湖南保监局 数据解析 ' + ' -- 数据已经存在')
                                result_map_list.append(result_map)
                    else:
                        static_tr = tr
                if len(result_map_list) > 0:
                    db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                    logger.info('湖南保监局 数据解析 ' + ' -- 修改parsed完成')
                else:
                    logger.warning('湖南保监局 数据解析 ' + ' -- 无数据')
        else:
            if db.parsed_data.find(
                    {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                oss_file_map = {
                    'origin_url': announcement_url,
                    'oss_file_origin_url': announcement_url,
                    'origin_url_id': each_circ_data['_id'],
                    'oss_file_type': 'html',
                    'oss_file_name': announcement_title,
                    'oss_file_content': r.text.encode(r.encoding).decode('utf-8'),
                    'parsed': False
                }
                insert_response = db.parsed_data.insert_one(oss_file_map)
                file_id = insert_response.inserted_id
                oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                             r.text.encode(r.encoding).decode('utf-8'))
                db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
            else:
                db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
                file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                   'oss_file_origin_url': announcement_url})['_id']

            document_code_compiler = re.compile(r'((湘保监罚|湘银保监罚决字).\d{4}.\d+号)')
            if document_code_compiler.search(content_text):
                document_code = document_code_compiler.search(content_text).group(1).strip()
                litigant_compiler = re.compile(
                    document_code.replace(r'[', r'\[').replace(r']', r'\]') +
                    r'\n([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|'
                                        r'一、|.*?(财务|记账)凭证(显示)?|'
                                        r'2016年1月1日至12月31日期间|'
                                        r'英大泰和财产保险股份有限公司|'
                                        r'你公司2016年通过“业务及管理费—差旅费”科目报销费用|'
                                        r'你公司2015年11月至2016年5月期间通过“间接理赔费—差旅费”报销费用|'
                                        r'你公司PDAA2015431100000228|'
                                        r'2015年1月至6月期间|'
                                        r'国华人寿保险股份有限公司（以下简称“国华人寿”）湖南分公司|'
                                        r'你公司2015年9月19\*号会计凭证列支|'
                                        r'.*?向我局递交了|'
                                        r'你公司80511201643110200|'
                                        r'中国平安财产保险股份有限公司|'
                                        r'.*?经查|'
                                        r'.*?进行现场检查|'
                                        r'2016年1月18日,湖南凤凰保险代理有限公司)')
                litigant = litigant_compiler.search(content_text).group(1).strip()
            else:
                if document_code_compiler.search(title):
                    document_code = document_code_compiler.search(title).group(1).strip()
                else:
                    document_code = ''
                litigant_compiler = re.compile(
                    r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|一|.*?(财务|记账)凭证(显示)?|2016年1月1日至12月31日期间|'
                                       r'英大泰和财产保险股份有限公司|你公司2016年通过“业务及管理费—差旅费”科目报销费用|'
                                       r'你公司2015年11月至2016年5月期间通过“间接理赔费—差旅费”报销费用|'
                                       r'你公司PDAA2015431100000228|2015年1月至6月期间|'
                                       r'国华人寿保险股份有限公司（以下简称“国华人寿”）湖南分公司|你公司2015年9月19\*号会计凭证列支|'
                                       r'.*?向我局递交了|'
                                       r'你公司80511201643110200|中国平安财产保险股份有限公司|'
                                       r'.*?经查|.*?进行现场检查|2016年1月18日,湖南凤凰保险代理有限公司)')
                litigant = litigant_compiler.search(content_text).group(1).strip()

            truth_text_str = r'((经查，|经查,|经检查，|经查实，|检查发现，|现场检查，发现|抽查，|经抽查，|经查.*?存在以下问题：|经查：|' \
                             r'一、|二、|三、)' \
                             r'([\s\S]*?))' \
                             r'(证明上述(违法)?事实的主要证据有|[^，。,；\n]*?上述行为.*?违反了.*?第.*?条(的)?规定|' \
                             r'你营业部通过改变车辆使用性质承保交强险和商业车险的行为违反了《中华人民共和国保险法》第一百零七条规定|' \
                             r'你公司的上述行为，违反了《中华人民共和国保险法》第一百零六条和第一百零七条的规定|' \
                             r'(上述|以上)事实(，)?有[\s\S]*?证据(证明)?(，足以认定)?。)'
            truth_compiler = re.compile(truth_text_str)
            truth_list = truth_compiler.findall(content_text)
            if len(truth_list) > 0:
                truth = '\n'.join([kk[0].strip() for kk in truth_list])
            else:
                truth_text_str = litigant.replace(r'*', r'\*') + r'([\s\S]*?)(证明上述(违法)?事实的主要证据有)'
                truth_compiler = re.compile(truth_text_str)
                truth = truth_compiler.search(content_text).group(1).strip()

            if '申辩' in content_text:
                defense_text_str = r'((针对.*?行为.*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?陈述申辩(意见)?|' \
                                   r'在法定陈述申辩期内，当事人放弃陈述申辩。|' \
                                   r'本案在审理过程中.*?提出陈述申辩|' \
                                   r'在规定期限内，当事人放弃陈述申辩且未提出听证申请。|' \
                                   r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见)中称|[^，。,；\n]*?在听证阶段提出|' \
                                   r'[^，。,；\n]*?在法定期限内(未)?提出(了)?(听证要求|陈述申辩|陈述申辩及听证要求)|' \
                                   r'你[^，。,；\n]*?在规定时限内(提出书面陈述申辩|申辩称|陈述申辩称)|' \
                                   r'你[^，。,；\n]*?在规定时间内向我局提出陈述申辩|' \
                                   r'你[^，。,；\n]*?在规定时间内未向我局提出听证，但向我局提出陈述申辩|' \
                                   r'你[^。；\n]*?辩称)' \
                                   r'([\s\S]*?))' \
                                   r'(因此，我局决定|' \
                                   r'我局经复核(认为|决定)|' \
                                   r'本案现已审理终结|' \
                                   r'我局经复查[^，。,；\n]*?情况|' \
                                   r'我局[^，。,；\n]*?认真复核|' \
                                   r'经研究，对[^，。,；\n]*?予以采纳。|' \
                                   r'我局认为.*?申辩理由|' \
                                   r'依据.*?我局认为.*?的申辩理由|' \
                                   r'经研究，我局认为.*?申辩意见|' \
                                   r'经我局审核，决定|' \
                                   r'我局认为，上述违法行为事实清楚、证据确凿、法律法规适当|' \
                                   r'我局对陈述申辩意见进行了复核|' \
                                   r'经我局审核|' \
                                   r'针对[^，。,；\n]*?的(陈述)?申辩意见，我局进行了核实|' \
                                   r'经查，我局认为|' \
                                   r'依据现场检查及听证情况|' \
                                   r'我局经研究认为|我局认为：在你签字审批的《呈批件（签报）》中|' \
                                   r'我局经复核，认为|' \
                                   r'我局经研究|' \
                                   r'我局认为)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense_list = defense_compiler.findall(content_text)
                if len(defense_list) != 0:
                    defense = defense_list[-1][0]
                    defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]').replace(r'(', r'\(') \
                                               .replace(r')', r'\)') \
                                           + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                              r'对[^，。,；\n]*?申辩(意见|理由)?(不予|予以)采纳|因此.*?申辩理由.*?成立。|' \
                                                              r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?))'
                    defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                    if defense_response_compiler.search(content_text):
                        defense_response = defense_response_compiler.search(content_text).group(1).strip()
                    else:
                        if '未' in defense:
                            defense_response = ''
                else:
                    defense_text_str = r'(\n?你[^，。,；\n]*?在规定(期限|时间|时限)内未(向我局)?(提出|提起)(听证申请和)?(陈述|申述)申辩(和听证要求|意见)?。\n?|' \
                                       r'你[^，。,；\n]*?在规定时限内未申请听证，也未提出陈述申辩。|你公司在规定期限内未提出陈述申辩，也未申请听证。|' \
                                       r'你公司在规定时限内未提出听证申请及陈述申辩。|你公司在规定时限内未提出听证和陈述申辩。|' \
                                       r'你在规定时限内未申请听证，在规定时限内提出了陈述申辩。|你[^，。,；\n]*?主动书面放弃陈述申辩(及听证)?权利|' \
                                       r'你在规定时间内未向我局提出\n陈述申辩。|你公司在规定时限内未向我局提出听证、陈述申辩。|' \
                                       r'你[^，。,；\n]*?在规定时间内未提出陈述、申辩。)'
                    defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                    defense = defense_compiler.search(content_text).group(1).strip()
                    defense_response = ''
            else:
                defense = defense_response = ''

            punishment_decision_text_str = r'(((我局认为)?[^。；\n]*?行为.*?((分别)?违反|构成)了?|' \
                                           r'综上，我局决定作出如下处罚)' \
                                           r'([\s\S]*?))' \
                                           r'(\n二、|\n三、|' \
                                           r'\n?你[^，。,；\n]*?在规定(期限|时间|时限)内未?(向我局)?(提出|提起)(听证申请和)?(陈述|申述)申辩(和听证要求|意见)?|' \
                                           r'你[^，。,；\n]*?在规定时限内未申请听证，也未提出陈述申辩。|你公司在规定期限内未提出陈述申辩，也未申请听证。|' \
                                           r'你公司在规定时限内未提出听证申请及陈述申辩。|你公司在规定时限内未提出听证和陈述申辩。|' \
                                           r'你在规定时限内未申请听证，在规定时限内提出了陈述申辩|你[^，。,；\n]*?在规定时限内(提出书面陈述申辩|申辩称)|' \
                                           r'你[^，。,；\n]*?主动书面放弃陈述申辩(及听证)?权利|你[^，。,；\n]*?在规定时限内未申请听证，但(提出)?陈述申辩称|' \
                                           r'你在规定时限内陈述申辩称|你在规定时间内向我局提出陈述申辩|你公司在规定时间内辩称|' \
                                           r'你[^，。,；\n]*?在规定时间内未向我局提出听证，但向我局提出陈述申辩|' \
                                           r'你在规定时间内未向我局提出\n陈述申辩。|你公司在规定时限内未向我局提出听证、陈述申辩。|' \
                                           r'你[^，。,；\n]*?在规定时限内申述申辩称|你[^，。,；\n]*?在规定时间内未提出陈述、申辩。|' \
                                           r'你[^，。,；\n]*?辩称|你[^，。,；\n]*?应在本处罚决定书送达之日|[^，。,；\n]*?如不服本处罚决定|' \
                                           r'[^，。,；\n]*?如对本处罚决定不服|' \
                                           r'\n当事人应当在接到本处罚决定书之日)'
            punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)

            punishment_decision_list = punishment_decision_compiler.findall(content_text)
            if len(punishment_decision_list) > 0:
                punishment_decision = '\n'.join([kk[0].strip() for kk in punishment_decision_list])

            punishment_basis_str_list = [
                r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)(违反|构成).*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            ]
            punishment_basis_str = '|'.join(punishment_basis_str_list)
            punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                   r'.(\n?依据|\n?根据|\n?鉴于)', re.MULTILINE)
            punishment_basis_list = punishment_basis_compiler.findall(content_text)
            punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

            publish_date_text = re.search(
                punishment_decision_list[-1][0].replace(r'(', r'\(').replace(r')', r'\)').
                replace(r'[', r'\[').replace(r']', r'\]').
                replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace(r'\n', '')
            if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1]
                m = re.match("([0-9零一二两三四五六七八九十〇○ＯO]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
                real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
            else:
                publish_date_text = table_content.find_all('tr')[1].text
                publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
                real_publish_date = publish_date.split('-')[0] + '年' + str(int(publish_date.split('-')[1])) + '月' + str(
                    int(publish_date.split('-')[2])) + '日'

            result_map = {
                'announcementTitle': title,
                'announcementOrg': '湖南银保监局',
                'announcementDate': real_publish_date,
                'announcementCode': document_code,
                'facts': truth,
                'defenseOpinion': defense,
                'defenseResponse': defense_response,
                'litigant': litigant[:-1] if litigant[-1] == '：' or litigant[-1] == ':' else litigant,
                'punishmentBasement': punishment_basis,
                'punishmentDecision': punishment_decision,
                'type': '行政处罚决定',
                'oss_file_id': file_id,
                'status': 'not checked'
            }
            logger.info(result_map)
            if db.announcement.find(
                    {'announcementTitle': result_map['announcementTitle'],
                     'oss_file_id': file_id,
                     'litigant': result_map['litigant']}).count() == 0:
                db.announcement.insert_one(result_map)
                logger.info('湖南保监局 数据解析 ' + ' -- 数据导入完成')
            else:
                logger.info('湖南保监局 数据解析 ' + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('湖南保监局 数据解析 ' + ' -- 修改parsed完成')
