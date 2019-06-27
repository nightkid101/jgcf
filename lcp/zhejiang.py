import re
import os
import subprocess
import docx
from urllib.parse import urljoin

from init import config_init
from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()
config = config_init()


def zhejiang_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '浙江保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('浙江保监局 ' + 'Url to parse: %s' % announcement_url)

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

        if '.doc' in content_text and len(table_content.find_all('tr')[3].find_all('a')) > 0:
            doc_link = urljoin(announcement_url, table_content.find_all('tr')[3].find('a').attrs['href'])
            file_response = request_site_page(doc_link)

            with open('./test/tmp.doc', 'wb') as f:
                f.write(file_response.content)
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
            full_text = []
            for para in doc.paragraphs:
                full_text.append(para.text)
            content_text = '\n'.join(full_text)

            logger.info('删除tmp文件')
            os.remove('./test/tmp.doc')
            os.remove('./test/tmp.docx')

            document_code_compiler = re.compile(r'(([浙温]保监罚|浙银保监筹?罚决字)\n?.\d{4}.\d+(-\d+)?\n?号\n)')
            if document_code_compiler.search(content_text):
                document_code = document_code_compiler.search(content_text).group(1).strip()
                litigant_compiler = re.compile(
                    document_code.replace(r'[', r'\[').replace(r']', r'\]') + r'([\s\S]*?)\n' +
                    r'(经查|依据.*?有关规定|.*?进行了(现场|举报)检查|在.*?专项检查活动期间|我局对.*?检查过程中发现|'
                    r'.*?一案.*?现已调查终结|阳光人寿保险股份有限公司舟山中心支公司委托未取得合法资格的机构)')
                litigant = litigant_compiler.search(content_text).group(1).strip()
            else:
                document_code_compiler = re.compile(r'(([浙温]保监罚|浙银保监筹罚决字|浙银保监罚决字).\d{4}.\d+(-\d+)?号)')
                if document_code_compiler.search(announcement_title):
                    document_code = document_code_compiler.search(announcement_title).group(1).strip()
                else:
                    document_code = ''
                litigant_compiler = re.compile(r'^([\s\S]*?)\n' + r'(经查|依据.*?有关规定|.*?进行了(现场|举报)检查|在.*?专项检查活动期间|'
                                                                  r'我局对.*?检查过程中发现)')
                litigant = litigant_compiler.search(content_text).group(1).strip()

            truth_text_str = r'(经查，|经查,|经检查，|经查实，|检查发现，|现场检查，发现|违法事实和证据|违法事实和依据|' \
                             r'违规事实和证据|违法事实和证据|存在以下违法行为:|行政处罚的事实、理由及依据。)' \
                             r'([\s\S]*?)' \
                             r'((上述|以上)(违法)?事实(,|，)?有.*?等证据(材料)?证明(,|，|。)(足以认定。)?|' \
                             r'\n.*?行政处罚依据及(行政)?处罚决定|\n.*?行政处罚依据及拟作出的行政处罚|\n.*?处罚依据及处罚决定|' \
                             r'上述事实，有.*?等材料予以证实。|' \
                             r'(对此|据此).我局(决定)?作出(如|以)下(行政)?处罚(决定)?|' \
                             r'上述行为，违反了.*?第.*?条.*?的规定|根据.*?，我局决定作出以下行政处罚：|' \
                             r'上述行为违反了.*?第.*?条的规定.*对此负有直接责任|' \
                             r'\n.*?上述(事实)?行为.*?违反了.*?第.*?条.*?(的规定)?|' \
                             r'以上行为.*?违反了.*?的规定)'
            truth_compiler = re.compile(truth_text_str)
            truth = truth_compiler.search(content_text).group(2).strip()

            if '申辩' in content_text:
                defense_text_str = r'((针对.*?行为.*?申辩意见|[^，。,；\n]*?提出.*?陈述申辩(意见)?|' \
                                   r'[^，。,；\n]*?向我局(报送|递交)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                                   r'[^，。,；\n]*?在(申辩材料|陈述申辩)中称：|当事人.*?书面.*?权利|' \
                                   r'当事人提出了陈述申辩|[^，。,；\n]*?递交了.*?|' \
                                   r'[^，。,；\n]*?提出了听证要求，我局依法举行了公开听证)' \
                                   r'([\s\S]*?))' \
                                   r'(因此，我局决定|' \
                                   r'我局经复核认为|' \
                                   r'现本案已审理终结|' \
                                   r'本案现已审理终结|' \
                                   r'我局经复查[^，。,；\n]*?情况|' \
                                   r'我局[^，。,；\n]*?认真复核|' \
                                   r'经研究，对[^，。,；\n]*?予以采纳。|' \
                                   r'我局认为.*?申辩理由|' \
                                   r'依据.*?我局认为.*?的申辩理由|' \
                                   r'经研究，我局认为.*?申辩意见|' \
                                   r'我局对陈述申辩意见进行了(复核|审核)|' \
                                   r'我局将另行依法处理。|' \
                                   r'我局对此进行了复核。|' \
                                   r'并对湖州银行股份有限公司的听证申辩意见和违法事实进行了审核)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense_list = defense_compiler.findall(content_text)
                if len(defense_list) != 0:
                    defense = defense_list[-1][0]
                    defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                           + r'((.*?)' + r'(本案现已审理终结。|现本案已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                         r'对[^，。,；\n]*?申辩意见(不予|予以)采纳。|因此.*?申辩理由.*?成立。|' \
                                                         r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                         r'我局将另行依法处理。))'
                    defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                    if defense_response_compiler.search(content_text):
                        defense_response = defense_response_compiler.search(content_text).group(1).strip()
                    else:
                        if '未' in defense:
                            defense_response = ''
            else:
                defense = defense_response = ''

            punishment_decision_text_str = r'(我局决定作出如下处罚：|综上，我局决定出如下处罚：|行政处罚依据及行政处罚决定|我局决定作出以下行政处罚：|' \
                                           r'行政处罚依据及处罚决定|行政处罚依据及拟作出的行政处罚|处罚依据及处罚决定|我局拟作出以下行政处罚：|' \
                                           r'我局作出以下行政处罚：|根据.*?第.*?条.*?规定，决定|综上，两项合计决定|依据.*?第.*?条.*?规定，决定|' \
                                           r'依据.*?第.*?条.*?规定，我局|我局决定作出以下行政处罚：|综上，合计|根据.*?第.*?条.*?规定.我局决定|' \
                                           r'我局决定作出如下行政处罚:|我局作出以下处罚决定：|我局决定作出如下行政处罚：|决定给予你单位以下行政处罚：)' \
                                           r'([\s\S]*?)' \
                                           r'(当事人应当在接到本处罚决定书之日|当事人如对本处罚决定不服|本处罚决定书自送达之日起执行|你公司如不服本处罚决定|' \
                                           r'\n.*?处罚的履行方式和期限|\n.*?如不服本行政处罚规定|当事人应在收到本处罚决定书之日|如不服本处罚决定)'
            punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
            punishment_decision = punishment_decision_compiler.search(content_text).group(2).strip()

            punishment_basis_str_list = [
                r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
                r'上述行为，违反了考试纪律',
                r'上述行为，违反了考场纪律',
                r'上述行为违反了《保险营销员管理规定》第二十七条的规定',
                r'上述第\n1\n项行为违反了《中华人民共和国保险法》第一百一十六条第八项的规定，第\n2\n、\n3\n项行为违反了《中华人民共和国保险法》第八十六条的规定',
                r'上述第\n1\n项行为违反了《中华人民共和国保险法》第一百一十六条第八项的规定，第\n2\n项行为违反了《中华人民共和国保险法》第八十六条的规定'
            ]
            punishment_basis_str = '|'.join(punishment_basis_str_list)
            punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                   r'.(\n*?依据|\n*?根据|\n*?鉴于|另查明，中宏人寿保险股份有限公司宁波分公司)', re.MULTILINE)
            punishment_basis_list = punishment_basis_compiler.findall(content_text)
            punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

            publish_date_text = re.search(
                punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
                replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')
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
                'announcementTitle': announcement_title,
                'announcementOrg': '浙江银保监局',
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

            title = table_content.find_all('tr')[0].text.strip()
            document_code_compiler = re.compile(r'(([浙温]保监罚|浙银保监筹?罚决字).?\d{4}.?\d+(-\d+)?号\n)')
            if document_code_compiler.search(content_text):
                document_code = document_code_compiler.search(content_text).group(1).strip()
                litigant_compiler = re.compile(document_code.replace(r'[', r'\[').replace(r']', r'\]') +
                                               r'([\s\S]*?)\n' +
                                               r'(经查|依据.*?有关规定|.*?进行了(现场|举报)检查|在.*?专项检查活动期间|我局对.*?检查过程中发现|'
                                               r'.*?一案.*?现已调查终结|本局于 2012 年3月 26日作出)')
                litigant = litigant_compiler.search(content_text).group(1).strip()
            else:
                document_code_compiler = re.compile(r'(([浙温]保监罚|浙银保监筹?罚决字).?\d{4}.?\d+(-\d+)?号)')
                if document_code_compiler.search(title):
                    document_code = document_code_compiler.search(title).group(1).strip()
                else:
                    document_code = ''
                litigant_compiler = re.compile(r'^([\s\S]*?)\n' + r'(经查|依据.*?有关规定|.*?进行了(现场|举报)检查|在.*?专项检查活动期间|'
                                                                  r'我局对.*?检查过程中发现)')
                litigant = litigant_compiler.search(content_text).group(1).strip()

            litigant = litigant.replace('行政处罚决定书', '').strip()

            truth_text_str = r'(经查，|经查,|经检查，|经查实，|检查发现，|现场检查，发现|违法事实和证据|违法事实和依据|' \
                             r'违规事实和证据|违法事实和证据|存在以下违法行为:|行政处罚的事实、理由及依据。|本局于 2012 年3月 26日作出)' \
                             r'([\s\S]*?)' \
                             r'((上述|以上)(违法)?事实(,|，)?有.*?等证据(材料)?证明(,|，|。)(足以认定。)?|' \
                             r'\n.*?行政处罚依据及(行政)?处罚决定|\n.*?行政处罚依据及拟作出的行政处罚|\n.*?处罚依据及处罚决定|' \
                             r'上述事实，有.*?等材料予以证实。|' \
                             r'(对此|据此).我局(决定)?作出(如|以)下(行政)?处罚(决定)?|' \
                             r'上述行为，违反了.*?第.*?条.*?的规定|根据.*?，我局决定作出以下行政处罚：|' \
                             r'上述行为违反了.*?第.*?条的规定.*对此负有直接责任|' \
                             r'\n.*?上述(事实)?行为.*?违反了.*?第.*?条.*?(的规定)?|' \
                             r'以上行为.*?违反了.*?的规定|' \
                             r'上述事实的证据材料主要有.*?等。证据充分，足以认定。|' \
                             r'上述违法事实有.*?等(证据材料|材料)。证据充分，足以认定。|' \
                             r'依据《中华人民共和国行政强制法》第五十四条的规定)'
            truth_compiler = re.compile(truth_text_str)
            truth = truth_compiler.search(content_text).group(2).strip()

            if '申辩' in content_text:
                if announcement_url in ['http://zhejiang.circ.gov.cn/web/site39/tab3594/info209781.htm']:
                    defense = defense_response = ''
                else:
                    defense_text_str = r'((针对.*?行为.*?申辩意见|[^，。,；\n]*?提出.*?陈述申辩(意见)?|' \
                                       r'[^，。,；\n]*?向我局(报送|递交)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                                       r'[^，。,；\n]*?在(申辩材料|陈述申辩)中称：|当事人.*?书面.*?权利|' \
                                       r'当事人提出了陈述申辩|[^，。,；\n]*?递交了.*?|' \
                                       r'[^，。,；\n]*?提出了听证要求，我局依法举行了公开听证)' \
                                       r'([\s\S]*?))' \
                                       r'(因此，我局决定|' \
                                       r'我局经复核认为|' \
                                       r'现本案已审理终结|' \
                                       r'本案现已审理终结|' \
                                       r'我局经复查[^，。,；\n]*?情况|' \
                                       r'我局[^，。,；\n]*?认真复核|' \
                                       r'经研究，对[^，。,；\n]*?予以采纳。|' \
                                       r'我局认为.*?申辩理由|' \
                                       r'依据.*?我局认为.*?的申辩理由|' \
                                       r'经研究，我局认为.*?申辩意见|' \
                                       r'我局对陈述申辩意见进行了(复核|审核)|' \
                                       r'我局将另行依法处理。|' \
                                       r'我局对此进行了复核。|' \
                                       r'并对湖州银行股份有限公司的听证申辩意见和违法事实进行了审核)'
                    defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                    defense_list = defense_compiler.findall(content_text)
                    if len(defense_list) != 0:
                        defense = defense_list[-1][0]
                        defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                               + r'(([\s\S]*?)' + r'(本案现已审理终结。|现本案已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                                  r'对[^，。,；\n]*?申辩意见(不予|予以)采纳。|因此.*?申辩理由.*?成立。|' \
                                                                  r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                                  r'我局将另行依法处理。))'
                        defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                        if defense_response_compiler.search(content_text):
                            defense_response = defense_response_compiler.search(content_text).group(1).strip()
                        else:
                            if '未' in defense:
                                defense_response = ''
            else:
                defense = defense_response = ''

            punishment_decision_text_str = r'(((依据|根据)[^。；\n]*?第?[^。；\n]*?条' \
                                           r'[^。；\n]*?(规定)?.?(我局)?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你|对.*?予以)|' \
                                           r'我局经复核认为|我局(决定|拟)?.*?作出(如下|以下)(行政)?处罚|' \
                                           r'综上，合计拟对你公司|' \
                                           r'根据《中国保险监督管理委员会行政处罚程序规定》，决定给予|' \
                                           r'依据《中华人民共和国行政强制法》第五十四条的规定，现依法向你催告)' \
                                           r'([\s\S]*?))' \
                                           r'(当事人应当在接到本处罚决定书之日|当事人如对本处罚决定不服|本处罚决定书自送达之日起执行|你公司如不服本处罚决定|' \
                                           r'\n.*?处罚的履行方式和期限|\n.*?如不服本行政处罚规定|当事人应在收到本处罚决定书之日|如不服本处罚决定|收到本催告书后)'
            punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
            punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

            punishment_basis_str_list = [
                r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
                r'上述行为，违反了考试纪律',
                r'上述行为违反了《保险营销员管理规定》第二十七条的规定',
                r'上述行为,致使富瑞得代理所提交的2005年4季度、2006年3季度等监管报表为虚假报表, 违反了《保险代理机构管理规定》第一百零七条的规定。颜坤良作为公司负责人对公司的违法行为负有直接责任',
                r'以上行为，违反了《浙江省保险代理从业人员资格考试考场纪律》的规定'
            ]
            punishment_basis_str = '|'.join(punishment_basis_str_list)
            punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                   r'.(\n*?依据|\n*?根据|\n*?鉴于|另查明，中宏人寿保险股份有限公司宁波分公司|\n?为此，我局决定作出)',
                                                   re.MULTILINE)
            punishment_basis_list = punishment_basis_compiler.findall(content_text)
            punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

            publish_date_text = re.search(r'\n(.*?)$', content_text).group(1).replace('\n', '')
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
                'announcementOrg': '浙江银保监局',
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
        if db.announcement.find(
                {'announcementTitle': result_map['announcementTitle'], 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('浙江保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('浙江保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('浙江保监局 数据解析 ' + ' -- 修改parsed完成')
