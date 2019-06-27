import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def guizhou_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '贵州保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('贵州保监局 ' + 'Url to parse: %s' % announcement_url)

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

        if '行政处罚事项' in title:
            sub_table_content = table_content.find_all('tr')[3].find_all('table')[0]
            result_map_list = []
            for tr in sub_table_content.find_all('tr'):
                if '行政处罚公开信息' not in tr.text and '行政处罚事项' not in tr.text and '处罚决定文号' not in tr.text and \
                        len(tr.find_all('td')) > 1 and tr.find_all('td')[0].text != tr.find_all('td')[1].text:
                    real_title = '贵州银保监局行政处罚决定书（' + tr.find_all('td')[0].text + '）'
                    real_publish_date = tr.find_all('td')[2].text.split('-')[0] + '年' + \
                                        tr.find_all('td')[2].text.split('-')[1] + '月' + \
                                        tr.find_all('td')[2].text.split('-')[2] + '日'

                    result_map = {
                        'announcementTitle': real_title,
                        'announcementOrg': '贵州银保监局',
                        'announcementDate': real_publish_date,
                        'announcementCode': tr.find_all('td')[0].text,
                        'facts': tr.find_all('td')[1].text + tr.find_all('td')[6].text,
                        'defenseOpinion': '',
                        'defenseResponse': '',
                        'litigant': tr.find_all('td')[1].text,
                        'punishmentBasement': tr.find_all('td')[1].text + '上述行为违反了' + tr.find_all('td')[4].text,
                        'punishmentDecision': '依据' + tr.find_all('td')[5].text + '，' + '我局对' + tr.find_all('td')[
                            1].text +
                                              '作出以下处罚：' + tr.find_all('td')[3].text,
                        'type': '行政处罚决定',
                        'oss_file_id': file_id,
                        'status': 'not checked'
                    }
                    logger.info(result_map)
                    if db.announcement.find({'announcementTitle': real_title, 'oss_file_id': file_id}).count() == 0:
                        db.announcement.insert_one(result_map)
                        logger.info('贵州保监局 数据解析 ' + ' -- 数据导入完成')
                    else:
                        logger.info('贵州保监局 数据解析 ' + ' -- 数据已经存在')
                    result_map_list.append(result_map)

            if len(result_map_list) > 0:
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('贵州保监局 数据解析 ' + ' -- 修改parsed完成')
            else:
                logger.info('贵州保监局 数据解析 ' + ' -- 没有数据')
        else:
            document_code_compiler = re.compile(r'(黔保监罚字.\d{4}.\d+.*?号)')
            if document_code_compiler.search(content_text):
                document_code = document_code_compiler.search(content_text).group(1).strip()
                litigant_compiler = re.compile(
                    document_code.replace(r'[', r'\[').replace(r']', r'\]') +
                    r'\n([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|'
                                        r'.*?存在.*?行为|.*进行检查|我局.*?检查时|'
                                        r'.*?经营活动|经核查|.*?进行了核查|'
                                        r'.*?担任.*?期间|'
                                        r'.*?未经我局批准，擅自|'
                                        r'你公司未经保险监管部门批准)')
                litigant = litigant_compiler.search(content_text).group(1).strip()
            else:
                if document_code_compiler.search(title):
                    document_code = document_code_compiler.search(title).group(1).strip()
                    litigant_compiler = re.compile(r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|.*?存在.*?行为|'
                                                                      r'.*进行检查|我局.*?检查时|.*?担任.*?期间|你公司未经保险监管部门批准|'
                                                                      r'.*?经营活动|经核查|.*?进行了核查|.*?未经我局批准，擅自)')
                    litigant = litigant_compiler.search(content_text).group(1).strip()
                else:
                    document_code = ''
                    litigant_compiler = re.compile(r'行政处罚信息.(.*?).$')
                    litigant = litigant_compiler.search(title).group(1).strip()

            truth_text_str = r'((经查|二、|三、|经核查)' \
                             r'([\s\S]*?))' \
                             r'((我局认为，)?(上述|以上).*?(事实|行为|问题)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                             r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                             r'违反|依据|上述行为违反了)'
            truth_compiler = re.compile(truth_text_str)
            truth_list = truth_compiler.findall(content_text)
            if len(truth_list) > 0:
                truth = '\n'.join([each_truth[0] for each_truth in truth_list])
            else:
                truth_text_str = litigant.replace(r'*', r'\*') + \
                                 r'([\s\S]*?)' \
                                 r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                 r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                                 r'上述行为违反了|依据《中华人民共和国保险法》第一百零九条、第一百三十七条、第一百四十七条的规定|' \
                                 r'依据《保险法》第一百四十四条的规定)'
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
                                   r'依据现场检查及听证情况|' \
                                   r'对上述陈述申辩意见，我局认为)'
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
                    defense_text_str = r'(你公司在陈述申辩意见中未对违法事实、处罚理由及依据提出异议。|' \
                                       r'你公司在规定期限内未提出陈述申辩意见。|' \
                                       r'你机构在陈述申辩意见中未对违法事实、处罚理由及依据提出异议。|' \
                                       r'你公司及陆忠豪在规定期限内未提出陈述申辩意见。|' \
                                       r'你在规定期限内未提出陈述申辩意见。|' \
                                       r'[^，。,；\n]*?在规定期限内未提出陈述申辩意见。|' \
                                       r'你在规定期限内未要求听证，也未提出陈述申辩意见。|' \
                                       r'可在收到本告知书之日起10日内向我局提交书面的陈述书和申辩书。逾期视为放弃陈述和申辩。)'
                    defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                    defense = defense_compiler.search(content_text).group(1).strip()
                    defense_response = ''
            else:
                defense = defense_response = ''

            punishment_decision_text_str = r'(((依据|根据|依照)[^。\n]*?第[^。\n]*?条[^。\n]*?(规定)?.?' \
                                           r'(我局)?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你)|' \
                                           r'我局经复核认为|我局(决定)?.*?作出(如下|以下)(行政)?(处罚)?处罚)' \
                                           r'([\s\S]*?))' \
                                           r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                           r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|$|当事人应当在本处罚决定书送达之日|' \
                                           r'请你在本处罚决定书送达之日|请.*?在接到本处罚决定书之日|' \
                                           r'请.*?在本处罚决定书送达之日|请在接到本处罚决定之日|如你支公司对我局认定的违法事实)'

            punishment_decision_compiler = re.compile(punishment_decision_text_str)
            punishment_decision_list = punishment_decision_compiler.findall(content_text)
            punishment_decision = '\n'.join(
                [each_punishment_decision[0] for each_punishment_decision in punishment_decision_list])

            punishment_basis_str_list = [
                r'([^\n。；]*?)(问题|行为|事项|情况|事实)[^。；]*?违反[^。；]*?第.*?条.*?((的|之|等)(相关)?规定)?',
                r'华安财险贵州分公司聘任不具有任职资格的人员违反了《保险法》八十一条第一款、第二款和《任职资格规定》第四条第（三）项、第（五）项的规定',
                r'人保财险印江支公司强制投保人订立商业保险合同违反了《交强险条例》第十三条第二款',
                r'上述行为违反了《中华人民共和国保险法》第一百二十二条和第一百三十四条的规定',
                r'上述行为违反了《中华人民共和国保险法》八十六条',
                r'经查，你公司于2006年7月1日至9月30日期间，通过弄虚作假套取现金支付非法代理及相关人员手续费24.9万元，违反了《中华人民共和国保险法》第一百二十二条和第一百三十四条的规定',
                r'你支公司于2005年11月15日，未经我局批准，擅自将支公司及下辖南北大街营销服务部分别由我局批设地址镇宁县城关李家井8号、'
                r'镇宁县南北大街黄果树商城内迁至镇宁县南北大街中段中国农业发展银行镇宁县支行办公大楼二楼，违反了《保险法》第八十二条的规定'
            ]
            punishment_basis_str = '|'.join(punishment_basis_str_list)
            punishment_basis_compiler = re.compile('(。|\n|；|^)' + '(' + punishment_basis_str + ')' +
                                                   '.(\n?依据|\n?根据|\n?鉴于|\n?依照)', re.MULTILINE)
            punishment_basis_list = punishment_basis_compiler.findall(content_text)
            punishment_basis = '；'.join([kk[1].strip() for kk in punishment_basis_list])

            publish_date_text = re.search(
                punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
                replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')

            if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
                m = re.match("([0-9零一二两三四五六七八九十〇○ＯOΟО]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?",
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
                'announcementTitle': title,
                'announcementOrg': '贵州银保监局',
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
                logger.info('贵州保监局 数据解析 ' + ' -- 数据导入完成')
            else:
                logger.info('贵州保监局 数据解析 ' + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('贵州保监局 数据解析 ' + ' -- 修改parsed完成')
