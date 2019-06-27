import re

from utility import cn2dig, get_year, request_site_page, get_content_text, table_to_list, format_date, remove_strip
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def shenzhen_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '深圳保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('深圳保监局 ' + 'Url to parse: %s' % announcement_url)

        r = request_site_page(announcement_url)
        if r is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_soup = bs(r.content, 'lxml') if r else bs('', 'lxml')

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

        table_content = content_soup.find(id='tab_content')
        if not table_content:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_text = get_content_text(table_content.find_all('tr')[3])
        if content_text == '':
            continue
        title = table_content.find_all('tr')[0].text.strip()

        if '行政处罚信息主动公开事项' in title or '行政处罚事项' in title:
            result_map_list = []
            table_value_list = table_to_list(table_content.find_all('tr')[3].find('table'))
            announcement_code = ''
            new_result_map = {}
            for index, each_origin_row in enumerate(table_value_list):
                each_row = []
                for i in each_origin_row:
                    if i not in each_row:
                        each_row.append(i)
                if '文号' in each_row[0] or '行政处罚信息主动公开事项' in each_row[0] or '处罚决定书' in each_row[0]:
                    continue
                if remove_strip(each_row[0]).strip() != announcement_code:
                    if new_result_map != {}:
                        punishment_basis_compiler = re.compile(r'(。|\n|；|^)' + r'(([^\n。；]*?)违反.*?第.*?条.*?规定)' +
                                                               '.(\n?依据|\n?根据|\n?鉴于|\n?上述事实有现场检查确认书)', re.MULTILINE)
                        punishment_basis_list = punishment_basis_compiler.findall(new_result_map['punishmentDecision'])
                        punishment_basis = '；'.join([kk[1].strip() for kk in punishment_basis_list])
                        new_result_map['punishmentBasement'] = punishment_basis
                        logger.info(new_result_map)
                        if db.announcement.find({'announcementTitle': new_result_map['announcementTitle'],
                                                 'oss_file_id': new_result_map['oss_file_id']}).count() == 0:
                            db.announcement.insert_one(new_result_map)
                            logger.info('深圳保监局 数据解析 ' + ' -- 数据导入完成')
                        else:
                            logger.info('深圳保监局 数据解析 ' + ' -- 数据已经存在')
                        result_map_list.append(new_result_map)
                    announcement_code = remove_strip(each_row[0]).strip()
                    this_punishment_decision = each_row[1].strip() + '：' + \
                                               each_row[3].strip() + ' ' + each_row[4].strip()
                    new_result_map = {
                        'announcementTitle': '深圳银保监局行政处罚信息主动公开事项（' + announcement_code + '）',
                        'announcementOrg': '深圳银保监局',
                        'announcementDate': format_date(each_row[-1].strip()),
                        'announcementCode': announcement_code,
                        'facts': each_row[1].strip() + '：' + each_row[2].strip(),
                        'defenseOpinion': '',
                        'defenseResponse': '',
                        'litigant': each_row[1].strip(),
                        'punishmentBasement': '',
                        'punishmentDecision': this_punishment_decision,
                        'type': '行政处罚决定',
                        'oss_file_id': file_id,
                        'status': 'not checked'
                    }
                else:
                    new_result_map['litigant'] += '，' + each_row[1].strip()
                    new_result_map['facts'] += '\n' + each_row[1].strip() + '：' + each_row[2].strip()
                    this_punishment_decision = each_row[1].strip() + '：' + \
                                               each_row[3].strip() + ' ' + each_row[4].strip()
                    new_result_map['punishmentDecision'] += '\n' + this_punishment_decision
                if index == len(table_value_list) - 1:
                    punishment_basis_compiler = re.compile(r'(。|\n|；|^)' + r'(([^\n。；]*?)违反.*?第.*?条.*?规定)' +
                                                           '.(\n?依据|\n?根据|\n?鉴于|\n?上述事实有现场检查确认书)', re.MULTILINE)
                    punishment_basis_list = punishment_basis_compiler.findall(new_result_map['punishmentDecision'])
                    punishment_basis = '；'.join([kk[1].strip() for kk in punishment_basis_list])
                    new_result_map['punishmentBasement'] = punishment_basis
                    logger.info(new_result_map)
                    if db.announcement.find({'announcementTitle': new_result_map['announcementTitle'],
                                             'oss_file_id': new_result_map['oss_file_id']}).count() == 0:
                        db.announcement.insert_one(new_result_map)
                        logger.info('深圳保监局 数据解析 ' + ' -- 数据导入完成')
                    else:
                        logger.info('深圳保监局 数据解析 ' + ' -- 数据已经存在')
                    result_map_list.append(new_result_map)
            if len(result_map_list) > 0:
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('深圳保监局 数据解析 ' + '一共有%d条数据' % len(result_map_list))
                logger.info('深圳保监局 数据解析 ' + ' -- 修改parsed完成')
            else:
                logger.info('深圳保监局 数据解析 ' + ' -- 没有数据')
        else:
            document_code_compiler = re.compile(r'(深银?保监[处罚].\d{4}.\d+号)')
            if document_code_compiler.search(content_text):
                document_code = document_code_compiler.search(content_text).group(1).strip()
                if '送达行政处罚决定书的公告' in title or '文书送达公告' in title:
                    litigant_compiler = re.compile(r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|抽查|经抽查'
                                                                      r'|经我局查明|我局已于|我局自|经我局查实)')
                    litigant = litigant_compiler.search(content_text).group(1).strip()
                else:
                    litigant_compiler = re.compile(
                        document_code.replace(r'[', r'\[').replace(r']', r'\]') +
                        r'\n([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|'
                                            r'经我局查明|'
                                            r'我局已于|'
                                            r'我局自|'
                                            r'.*?进行了(现场检查|专项检查)|'
                                            r'.*?\n?至\n?.*?|'
                                            r'2006年4月4日)')
                    litigant = litigant_compiler.search(content_text).group(1).strip()
            else:
                if document_code_compiler.search(title):
                    document_code = document_code_compiler.search(title).group(1).strip()
                else:
                    document_code = ''
                litigant_compiler = re.compile(r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|经我局查明|我局已于|我局自|'
                                                                  r'.*?进行了(现场检查|专项检查)|.*?\n?至\n?.*?|'
                                                                  r'2006年4月4日)')
                litigant = litigant_compiler.search(content_text).group(1).strip()

            truth_text_str = r'((经查|经检查|检查发现|抽查|经抽查|经我局查明|经我局查实)' \
                             r'([\s\S]*?))' \
                             r'((我局认为，)?[^，。,；\n]*?(上述|以上).*?(事实|行为|问题)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                             r'以上有.*?等证据为证。|' \
                             r'(根据|依据)\n?.*?第.*?条|' \
                             r'根据\n《\n保险营销员管理规定》第\n55\n条\n的规定|' \
                             r'依照《保险统计管理暂行管理规定》第38条的规定，我局决定|' \
                             r'依据《中华人民共和国保险法》一百五十条的规定，我局决定|' \
                             r'该行为违反了《中华人民共和国保险法》第107条|' \
                             r'上述行为违反了《中华人民共和国保险法》第一百四十五条、《保险代理机构管理规定》第一百二十九条和第一百三十八条|' \
                             r'上述行为违反了《中华人民共和国保险法》第一百四十五条、《保险代理机构管理规定》第一百二十九条、第一百三十八条和第一百四十三条的规定|' \
                             r'上述行为违反了《中华人民共和国保险法》第一百四十五条、《保险经纪机构管理规定》第一百二十二条和第一百二十九条的规定|' \
                             r'上述行为违反了《保险代理机构管理规定》第一百二十五条|' \
                             r'基于你公司以上违规行为的性质、危害，以及你公司在我局检查过程中存在的拖延提供会计凭证|' \
                             r'按照《保险法》第147条关于“对提供虚假的报告、报表、文件和资料的|' \
                             r'(针对|对于).*?上述(违法)?违规行为|' \
                             r'基于以上问题|' \
                             r'因与你无法联系)'
            truth_compiler = re.compile(truth_text_str)
            if truth_compiler.search(content_text):
                truth = truth_compiler.search(content_text).group(1).strip()
            else:
                truth_text_str = litigant + r'([\s\S]*?)' \
                                            r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                            r'以上有.*?等证据为证。|' \
                                            r'作为该分公司分管银行保险业务的副总经理，你对上述违规问题负有直接责任。|' \
                                            r'(根据|依据)\n?.*?第.*?条|' \
                                            r'根据\n《\n保险营销员管理规定》第\n55\n条\n的规定|' \
                                            r'针对你公司存在的上述违规行为)'
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
                    defense = defense_list[-1][0]
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
                    defense_text_str = '(你至今未向我局受领该《行政处罚事先告知书》，也未向我局提出陈述申辩。)'
                    defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                    defense = defense_compiler.search(content_text).group(1).strip()
                    defense_response = ''
            else:
                defense = defense_response = ''

            punishment_decision_text_str = r'(((依据|根据).*?第?.*?条.*?(规定)?.?(我局)?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你|现责令)|' \
                                           r'我局经复核认为|我局决定.*?作出(如下|以下)(行政)?处罚：|' \
                                           r'依照《保险统计管理暂行管理规定》第38条的规定，我局决定|' \
                                           r'依据《中华人民共和国保险法》，我局对你公司做出如下处理|' \
                                           r'按照《保险法》第147条关于“对提供虚假的报告、报表、文件和资料的|' \
                                           r'依据《中华人民共和国保险法》一百五十条的规定，我局决定|' \
                                           r'按照《保险法》第147条关于“对提供虚假的报告、报表、文件和资料的，尚不构成罪犯的，由保险监督管理机构责令改正，处以十万元以上五十万元以下的罚款|' \
                                           r'据此，决定给予你公司以下行政处罚|' \
                                           r'依据《中华人民共和国保险法》，我局决定对你公司做出如下处罚：|' \
                                           r'针对你公司存在的上述违规行为，根据《保险法》第139条、第145条、第147条、第150条和《交强险条例》第38\n?条的规定，决定给予|' \
                                           r'针对上述违规行为，我局拟对你公司进行如下行政处罚|' \
                                           r'根据\n《\n保险营销员管理规定》第\n55\n条\n的规定|' \
                                           r'根据《保险代理机构管理规定》第一百四十一条第（一）\n?项的规定，我局现责令|' \
                                           r'依据《中华人民共和国保险法》第一百四十七条第（一）项和《保险经纪机构管理规定》第一百三十三条第（一）项\n?的规定，我局现责令|' \
                                           r'鉴于你公司能及时纠正上述违法行为，根据《中华人民共和国行政处罚法》第二十七\n?条第一款第（一）项，?' \
                                           r'中国保监会《保险专业代理机构监管规\n?定》第八十条第一款第（一）项、第（二）项的规定，我局决定|' \
                                           r'因与你无法联系)' \
                                           r'([\s\S]*?))\n' \
                                           r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                           r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|如对本处理决定不服|.*?应于接到本处罚决定书之日|' \
                                           r'.*?应在接到本处罚决定书之日|.*?于收到本决定书之日|.*?应于收到本处罚决定书之日|请在接到本决定书之日|' \
                                           r'因你下落不明，我局无法与你取得联系|请.*?在接到本处罚决定书之日|请.*?在收到本处罚决定书之日|' \
                                           r'请.*?收到本决定书之日|你公司应在收到本处罚决定书之日|' \
                                           r'联席人)'

            punishment_decision_compiler = re.compile(punishment_decision_text_str)
            punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

            punishment_basis_str_list = [
                r'([^\n。]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反([^\n。\s]*?)第([^\n。\s]*?)条([\s\S]*?)',
                r'[^\n。；]*?违反了[^\n。；]*?第[^\n。；]*?条[^\n。；]*?',
                r'上述行为违反了\n《\n保险营销员管理规定\n》第36条\n的规定',
                r'上述行为违反了《保险代理机构管理规定》，你作为总经理，对深圳市鼎盛保险代理公司的上述行为负有直接责任',
                r'你公司对于上述保费资金的收付及所谓“业务费用”的支付等均未纳入账内进行会计核算，也未能提供相关凭证和证明资料，属帐外经营，'
                r'违反了《保险代理机构管理规定》第一百零七条“保险代理机构及其分支机构报送的报表、报告和资料应当及时、准确、完整”的规定。\n'
                r'你公司对于上述业务未建立详细的业务记录，也未进行业务档案或专门帐簿管理，违反了《保险代理机构管理规定》第九十六条“保险代理机构及其分支机构应当建立完整规范的业务档案”的规定'
            ]
            punishment_basis_str = '|'.join(punishment_basis_str_list)
            punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                   r'.(\n?(针对.*?上述违规行为.*?|基于以上问题.*?|对以上违规行为.*?)?依据|'
                                                   r'\n?(针对.*?上述违规行为.*?|基于以上问题.*?)?根据|'
                                                   r'\n?鉴于|\n二、|\n?对于你公司上述违规行为|\n?依照|\n三、|\n四、|\n五、|\n六、|\n七、|\n八、|'
                                                   r'\n2、|\n3、|\n4、|\n?以上有.*?等证据为证|\n（二）|\n（三）|\n（四）|'
                                                   r'\n（五）|\n?据此|\n?于“保险机构应当根据)', re.MULTILINE)
            punishment_basis_list = punishment_basis_compiler.findall(content_text)
            punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

            publish_date_text = re.search(r'\n(.*?)$', content_text).group(1).replace('\n', '')
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
                'announcementOrg': '深圳银保监局',
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
                logger.info('深圳保监局 数据解析 ' + ' -- 数据导入完成')
            else:
                logger.info('深圳保监局 数据解析 ' + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('深圳保监局 数据解析 ' + ' -- 修改parsed完成')
