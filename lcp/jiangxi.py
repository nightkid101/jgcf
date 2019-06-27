import re
import os
import subprocess
import patoolib
import docx
import shutil
from urllib.parse import urljoin

from init import config_init
from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss
from parser_code.cbrc_parse import parse_table

ali_bucket = init_ali_oss()
config = config_init()


def jiangxi_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '江西保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'parsed': True}).count() >= 1:
            continue

        logger.info('江西保监局 ' + 'Url to parse: %s' % announcement_url)

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

        if '2008年1-5月行政处罚决定' in title or '2008年以前行政处罚决定' in title:
            rar_link = [urljoin(announcement_url, each_link.attrs['href'])
                        for each_link in content_soup.find_all('a') if '.rar' in each_link.attrs['href']][0]
            response = request_site_page(rar_link)
            with open('./test/tmp.rar', 'wb') as out_file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        out_file.write(chunk)

            if not os.path.exists('./test/tmp'):
                os.mkdir('./test/tmp')
            patoolib.extract_archive('./test/tmp.rar', outdir='./test/tmp')

            doc_file_list = []
            for root, dirs, files in os.walk("./test/tmp", topdown=False):
                for name in files:
                    doc_file_list.append(os.path.join(root, name))

            for each_doc_file in doc_file_list:
                doc_title = re.split(r'[./]', each_doc_file)[-2]
                if not os.path.exists('./test/tmp/' + doc_title + '.docx'):
                    shell_str = config['soffice']['soffice_path'] + ' --headless --convert-to docx ' + \
                                each_doc_file + ' --outdir ./test/tmp'
                    process = subprocess.Popen(shell_str.replace(r'(', r'\(').replace(r')', r'\)'),
                                               shell=True, stdout=subprocess.PIPE)
                    process.wait()

                with open('./test/tmp/' + doc_title + '.docx', 'rb') as docx_file:
                    docx_content = docx_file.read()

                if db.parsed_data.find(
                        {'origin_url': announcement_url, 'oss_file_name': doc_title}).count() == 0:
                    oss_file_map = {
                        'origin_url': announcement_url,
                        'oss_file_origin_url': rar_link,
                        'origin_url_id': each_circ_data['_id'],
                        'oss_file_type': 'docx',
                        'oss_file_name': doc_title,
                        'oss_file_content': docx_content,
                        'parsed': False
                    }
                    insert_response = db.parsed_data.insert_one(oss_file_map)
                    file_id = insert_response.inserted_id
                    oss_add_file(ali_bucket, str(file_id) + '/' + doc_title + '.docx', docx_content)
                    db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
                else:
                    db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'parsed'}})
                    file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                       'oss_file_name': doc_title})['_id']

                doc = docx.Document('./test/tmp/' + doc_title + '.docx')
                full_text = []
                for para in doc.paragraphs:
                    full_text.append(para.text)
                content_text = '\n'.join(full_text)

                document_code_compiler = re.compile(r'(赣银?保监罚\n?.*?\d+\n?\d+\n?.*?\n?\d+\n?号)')
                document_code = document_code_compiler.search(content_text).group(1).strip()

                real_title = '中国银保监会江西监管局行政处罚决定书（' + document_code + '）'

                litigant_compiler = re.compile(
                    document_code.replace(r'[', r'\[').replace(r']', r'\]') +
                    r'\n([\s\S]*?)\n' + r'(经查|依据.*?的有关规定|抽查|'
                                        r'经抽查|经检查|.*?存在以下(问题|违法行为)|'
                                        r'.*?认定事实|.*?存在.*?(行为|业务数据不真实)|'
                                        r'你.*?于.*?期间|'
                                        r'经调查|'
                                        r'经我局查实|'
                                        r'你.*?于.*?业务经营|'
                                        r'.*?于.*?期间在展业过程|'
                                        r'你公司吉水营销服务部承保车牌号为赣D/38008货车商业机车险时|'
                                        r'你.*?进行.*?宣传|'
                                        r'.*?于.*?业务经营|'
                                        r'你公司.*?业务经营中|'
                                        r'.*?在.*?期间|'
                                        r'你公司于2006年1-\n?12月期间|'
                                        r'.*?在.*?业务经营中存在)')
                litigant = litigant_compiler.search(content_text).group(1).strip()

                truth_text_str = r'((经查|一、|二、|三、|经检查|.*?存在以下(问题|违法行为)|.*?认定事实|.*?存在.*?(行为|业务数据不真实)|你公司于.*?期间)' \
                                 r'([\s\S]*?))' \
                                 r'((，|，)?(综合)?(依据|鉴于|根据))'
                truth_compiler = re.compile(truth_text_str)
                truth_list = truth_compiler.findall(content_text)
                if len(truth_list) > 0:
                    truth = '\n'.join([kk[0].strip() for kk in truth_list])
                else:
                    truth_text_str = litigant + r'([\s\S]*?)' \
                                                r'((，|，)?(综合)?(依据|鉴于|根据))'
                    truth_compiler = re.compile(truth_text_str)
                    truth = truth_compiler.search(content_text).group(1).strip()

                if '申辩' in content_text:
                    defense_text_str = r'((针对.*?行为.*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?陈述申辩(意见)?|' \
                                       r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                                       r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见|陈述材料|申辩书)中称|[^，。,；\n]*?在听证阶段提出|' \
                                       r'[^，。,；\n]*?在法定期限内(未)?提出(了)?(听证要求|陈述申辩|陈述申辩及听证要求)|' \
                                       r'在法定期限内，当事人未提出|[^，。,；\n]*?提出了?陈述、申辩|[^，。,；\n]*?提出以下陈述申辩意见|' \
                                       r'[^，。,；\n]*?放弃陈述、申辩权利|[^，。,；\n]*?提出以下陈述(申辩理由|意见)|' \
                                       r'[^，。,；\n]*?陈述申辩材料中辩称|[^，。,；\n]*?陈述材料中提出|[^，。,；\n]*?提出以下申辩意见|' \
                                       r'[^，。,；\n]*?申辩中提出)' \
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
                                       r'我局认为|我局经核查|对此，我局认为|我局经审理认为|我局在处罚幅度裁量时)'
                    defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                    defense_list = defense_compiler.findall(content_text)
                    if len(defense_list) != 0:
                        defense = defense_list[-1][0].strip()
                        defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                               + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                                  r'对[^，。,；\n]*?申辩意见(不予|予以)采纳|因此.*?申辩理由.*?成立。|' \
                                                                  r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                                  r'不予采纳其陈述申辩理由。|维持原处罚决定。|不予采纳。|' \
                                                                  r'不予采纳.*?(陈述)?申辩(意见|理由)?。|' \
                                                                  r'我局认定你公司行为构成销售误导。|你公司的申辩不成立。))'
                        defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                        if defense_response_compiler.search(content_text):
                            defense_response = defense_response_compiler.search(content_text).group(
                                1).strip()
                        else:
                            if '未' in defense:
                                defense_response = ''
                    else:
                        defense_text_str = '([^。；\n]*?向.*?公告送达了《行政处罚事先告知书》.*?提出陈述申辩。|' \
                                           '我局依法于2012年5月25日对你公司送达了《行政处罚事先告知书》，你公司在规定的时间内未提出陈述和申辩意见，也未要求举行听证。)'
                        defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                        defense = defense_compiler.search(content_text).group(1).strip()
                        defense_response = ''
                else:
                    defense = defense_response = ''

                punishment_decision_text_str = r'(((依\n?据|根据)[^。；]*?第[^。；]*?条[\s\S]*?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你)|' \
                                               r'(我局)?决定.*?(作出|给予)(如下|以下)(行政)?处罚：|' \
                                               r'依据《保险公司管理规定》九十九条的规定，我局决定)' \
                                               r'([\s\S]*?))' \
                                               r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                               r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|.*?如对本处罚决定不服|' \
                                               r'请在接到本处罚决定书之日|.*?须于收到本处罚决定书之日|' \
                                               r'.*?应在接到本处罚决定书之日|.*?应严格按照保监发|.*?应该在收到本决定之日|' \
                                               r'.*?收到本处罚决定书之日|.*?应于收到本处罚决定书之日)'
                punishment_decision_compiler = re.compile(punishment_decision_text_str)
                punishment_decision_list = punishment_decision_compiler.findall(content_text)
                if len(punishment_decision_list) > 0:
                    punishment_decision = '\n'.join([kk[0].strip() for kk in punishment_decision_list])

                punishment_basis_str_list = [
                    r'((上\n?述|以上|该)(事实|实施)?行为)?违反[^。；]*?第[^，。,；]*?条[\s\S]*?',
                ]
                punishment_basis_str = '|'.join(punishment_basis_str_list)
                punishment_basis_compiler = re.compile(r'[。\n；，]' + '(' + punishment_basis_str + ')' +
                                                       '.(\n?依\n?据|\n?根据|\n?鉴于|\n?上述事实有现场检查确认书)', re.MULTILINE)
                punishment_basis_list = punishment_basis_compiler.findall(content_text)
                punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

                publish_date_text = re.search(
                    punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(
                        r']', r'\]').
                    replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')
                if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                    publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
                    m = re.match(
                        "([0-9零一二两三四五六七八九十〇○ＯOΟО]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?",
                        publish_date)
                    real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(
                        cn2dig(m.group(3))) + '日'
                else:
                    publish_date_text = table_content.find_all('tr')[1].text
                    publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
                    real_publish_date = publish_date.split('-')[0] + '年' + str(
                        int(publish_date.split('-')[1])) + '月' + str(
                        int(publish_date.split('-')[2])) + '日'

                result_map = {
                    'announcementTitle': real_title,
                    'announcementOrg': '江西银保监局',
                    'announcementDate': real_publish_date,
                    'announcementCode': document_code,
                    'facts': truth,
                    'defenseOpinion': defense,
                    'defenseResponse': defense_response,
                    'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                    'punishmentBasement': punishment_basis,
                    'punishmentDecision': punishment_decision,
                    'type': '行政处罚决定',
                    'oss_file_id': file_id,
                    'status': 'not checked'
                }
                logger.info(result_map)
                if db.announcement.find({'announcementTitle': real_title, 'oss_file_id': file_id}).count() == 0:
                    db.announcement.insert_one(result_map)
                    logger.info('江西保监局 数据解析 ' + ' -- 数据导入完成')
                else:
                    logger.info('江西保监局 数据解析 ' + ' -- 数据已经存在')
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('江西保监局 数据解析 ' + ' -- 修改parsed完成')

            logger.info('删除Tmp文件')
            os.remove('./test/tmp.rar')
            shutil.rmtree('./test/tmp', ignore_errors=True)
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

            if len(table_content.find_all(class_='MsoNormalTable')) > 0:
                for each_table in table_content.find_all(class_='MsoNormalTable'):
                    parse_table(file_id, each_table, title, each_circ_data['publishDate'])
            else:
                document_code_compiler = re.compile(r'(赣银?保监罚.\d{4}.\d+号)')
                if document_code_compiler.search(content_text):
                    document_code = document_code_compiler.search(content_text).group(1).strip()
                    litigant_compiler = re.compile(
                        document_code.replace(r'[', r'\[').replace(r']', r'\]') +
                        r'\n([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|'
                                            r'抽查|经抽查|.*?现场检查|'
                                            r'\n?.*?于\n?.*?\n?年.*?\n?期间|'
                                            r'2008年9月期间|'
                                            r'你公司于\n2008\n年\n1-4\n月\n期间|'
                                            r'你在担任.*?期间)')
                    litigant = litigant_compiler.search(content_text).group(1).strip()
                else:
                    if document_code_compiler.search(title):
                        document_code = document_code_compiler.search(title).group(1).strip()
                    else:
                        document_code = ''
                    litigant_compiler = re.compile(r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|2008年9月期间|'
                                                                      r'你公司于\n2008\n年\n1-4\n月\n期间|'
                                                                      r'你在担任.*?期间)')
                    litigant = litigant_compiler.search(content_text).group(1).strip()

                truth_text_str = r'((经查)' \
                                 r'([\s\S]*?))' \
                                 r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                 r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                                 r'依据|' \
                                 r'你作为.*?负有直接责任|' \
                                 r'我局决定)'
                truth_compiler = re.compile(truth_text_str)
                if truth_compiler.search(content_text):
                    truth = truth_compiler.search(content_text).group(1).strip()
                else:
                    truth_text_str = litigant + r'([\s\S]*?)' \
                                                r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                                r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                                                r'你作为.*?对.*?负有直接责任。|' \
                                                r'[^，。,；\n]*?依据)'
                    truth_compiler = re.compile(truth_text_str)
                    truth = truth_compiler.search(content_text).group(1).strip()

                if '申辩' in content_text:
                    defense_text_str = r'((针对.*?行为.*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?陈述申辩(意见)?|' \
                                       r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                                       r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见)中称|[^，。,；\n]*?在听证阶段提出|' \
                                       r'[^，。,；\n]*?在法定期限内(未)?提出(了)?(听证要求|陈述申辩|陈述申辩及听证要求))' \
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
                                       r'依据现场检查及听证情况)'
                    defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                    defense_list = defense_compiler.findall(content_text)
                    if len(defense_list) != 0:
                        defense = defense_list[-1][0].strip()
                        defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                               + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                                  r'对[^，。,；\n]*?申辩意见(不予|予以)采纳|因此.*?申辩理由.*?成立。|' \
                                                                  r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?))'
                        defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                        if defense_response_compiler.search(content_text):
                            defense_response = defense_response_compiler.search(content_text).group(1).strip()
                        else:
                            if '未' in defense:
                                defense_response = ''
                    else:
                        defense_text_str = '([^。；\n]*?向.*?公告送达了《行政处罚事先告知书》.*?提出陈述申辩。|' \
                                           '我局依法于2012年5月25日对你公司送达了《行政处罚事先告知书》，你公司在规定的时间内未提出陈述和申辩意见，也未要求举行听证。)'
                        defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                        defense = defense_compiler.search(content_text).group(1).strip()
                        defense_response = ''
                else:
                    defense = defense_response = ''

                punishment_decision_text_str = r'(((依据|根据)[^。；]*?第[^。；]*?条[^。；]*?(规定)?.?' \
                                               r'(我局)?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你)|' \
                                               r'我局决定.*?作出(如下|以下)(行政)?处罚：|' \
                                               r'依据\n?.*?\n?第.*?\n?的规定，我局决定|' \
                                               r'依据《保险营销员管理规定》，给予|' \
                                               r'我局决定(作出|给予))' \
                                               r'([\s\S]*?))' \
                                               r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                               r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|当事人如对本处罚决定不服|' \
                                               r'当事人应当在收到本处罚决定书之日|[^。；\n]*?在收到本处罚决定书之日|请你在收到本处罚决定书之日|' \
                                               r'你公司须于收到本处罚决定书之日|二○○九年九月一日)'

                punishment_decision_compiler = re.compile(punishment_decision_text_str)
                punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

                punishment_basis_str_list = [
                    r'[^。；\n]*?(问题|行为|事项|情况|事实)[^。；]*?违反[^。；]*?第[^。；]*?条[\s\S]*?((的|之|等)(相关)?规定)?',
                    r'协助保险营销员考试作弊\n的行为，违反了\n《保险营销员管理规定》第五十六\n的规定。\n你作为.*?对该行为负有直接责任',
                    r'中国人寿永丰县支公司应当规范保险销售从业人员的销售行为，严禁保险销售从业人员在保险销售活动中给予投保人保险合同约定以外的利益。对此，中国人寿永丰县支公司违反了《保险销售从业人员监管办法》第二十四条的规定',
                    r'袁红香违反了《中华人民共和国保险法》（2009年修订）第一百三十一条第（四）项的规定',
                    r'经查，你任.*?期间，该公司[\s\S]*?违反[\s\S]*?',
                    r'经查，你任中国人寿保险股份有限公司崇仁县支公司经理期间，该公司\n2009\n年\n1-4\n月\n开展银邮业务时存在\n'
                    r'财务数据不真实的\n行为，违反了\n《中华人民共和国保险法》\n的相关规定，你作为该支公司经理负有直接责任',
                    r'经查，你公司于\n2009\n年\n1-4\n月\n开展银邮业务时存在\n财务数据不真实\n的行为，该行为违反了\n《中华人民共和国保险法》\n的相关规定',
                    r'上述行为既不符合\n《关于保险代理（经纪）机构投保职业责任保险有关事宜的通知》（保监发〔\n2005\n〕\n27\n号）\n的有关要求，也不符合《\n保险专业代理机构监管规定》第四十条\n的有关规定'
                ]
                punishment_basis_str = '|'.join(punishment_basis_str_list)
                punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                       '.(\n?依据|\n?根据|\n?鉴于|\n?我局决定)', re.MULTILINE)
                punishment_basis_list = punishment_basis_compiler.findall(content_text)
                punishment_basis = '；'.join(
                    [kk[0].replace(litigant.split('\n')[-1], '').strip() for kk in punishment_basis_list])

                publish_date_text = re.search(
                    punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
                    replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')
                if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                    publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
                    m = re.match("([0-9零一二两三四五六七八九十〇○ＯOΟО]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
                    real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
                else:
                    publish_date_text = table_content.find_all('tr')[1].text
                    publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
                    real_publish_date = publish_date.split('-')[0] + '年' + str(int(publish_date.split('-')[1])) + '月' + str(
                        int(publish_date.split('-')[2])) + '日'

                result_map = {
                    'announcementTitle': title,
                    'announcementOrg': '江西银保监局',
                    'announcementDate': real_publish_date,
                    'announcementCode': document_code,
                    'facts': truth,
                    'defenseOpinion': defense,
                    'defenseResponse': defense_response,
                    'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                    'punishmentBasement': punishment_basis,
                    'punishmentDecision': punishment_decision,
                    'type': '行政处罚决定',
                    'oss_file_id': file_id,
                    'status': 'not checked'
                }
                logger.info(result_map)
                if db.announcement.find({'announcementTitle': title, 'oss_file_id': file_id}).count() == 0:
                    db.announcement.insert_one(result_map)
                    logger.info('江西保监局 数据解析 ' + ' -- 数据导入完成')
                else:
                    logger.info('江西保监局 数据解析 ' + ' -- 数据已经存在')
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('江西保监局 数据解析 ' + ' -- 修改parsed完成')
