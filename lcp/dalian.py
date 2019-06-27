import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def dalian_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '大连保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('大连保监局 ' + 'Url to parse: %s' % announcement_url)

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

        if '行政处罚明细' in title or '行政处罚事项' in title:
            sub_table_content = table_content.find_all('tr')[3].find_all('table')[0]
            result_map_list = []
            if title == '大连保监局2011年8月行政处罚事项':
                for tr in sub_table_content.find_all('tr'):
                    if '行政处罚公开信息' not in tr.text and '行政处罚事项' not in tr.text and '处罚决定文号' not in tr.text and \
                            len(tr.find_all('td')) > 1 and tr.find_all('td')[0].text != tr.find_all('td')[1].text:
                        real_title = '大连保监局行政处罚决定书（' + tr.find_all('td')[0].text + '）'
                        publish_date_text = tr.find_all('td')[2].text

                        real_publish_date = publish_date_text.split('-')[0] + '年' + \
                                            publish_date_text.split('-')[1] + '月' + \
                                            publish_date_text.split('-')[2] + '日'

                        result_map = {
                            'announcementTitle': real_title,
                            'announcementOrg': '大连保监局',
                            'announcementDate': real_publish_date,
                            'announcementCode': tr.find_all('td')[0].text,
                            'facts': tr.find_all('td')[1].text + tr.find_all('td')[6].text,
                            'defenseOpinion': '',
                            'defenseResponse': '',
                            'litigant': tr.find_all('td')[1].text,
                            'punishmentBasement': tr.find_all('td')[1].text + '上述行为违反了' + tr.find_all('td')[4].text,
                            'punishmentDecision': '依据' + tr.find_all('td')[5].text + '，' +
                                                  '我局对' + tr.find_all('td')[1].text +
                                                  '作出以下处罚：' + tr.find_all('td')[3].text,
                            'type': '行政处罚决定',
                            'oss_file_id': file_id,
                            'status': 'not checked'

                        }
                        logger.info(result_map)
                        if db.announcement.find({'announcementTitle': title, 'oss_file_id': file_id}).count() == 0:
                            db.announcement.insert_one(result_map)
                            logger.info('大连保监局 数据解析 ' + ' -- 数据导入完成')
                        else:
                            logger.info('大连保监局 数据解析 ' + ' -- 数据已经存在')
                        result_map_list.append(result_map)
                if len(result_map_list) > 0:
                    db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                    logger.info('大连保监局 数据解析 ' + ' -- 修改parsed完成')
                else:
                    logger.warning('大连保监局 数据解析 ' + ' -- 没有数据')
            else:
                for tr in sub_table_content.find_all('tr'):
                    if '行政处罚公开信息' not in tr.text and '行政处罚事项' not in tr.text and '处罚决定文号' not in tr.text and \
                            len(tr.find_all('td')) > 1 and tr.find_all('td')[0].text != tr.find_all('td')[1].text:
                        real_title = '大连银保监局行政处罚决定书（' + tr.find_all('td')[0].text + '）'
                        publish_date_text = table_content.find_all('tr')[1].text
                        if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                            publish_date = re.findall(r'.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
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
                            'announcementTitle': real_title,
                            'announcementOrg': '大连银保监局',
                            'announcementDate': real_publish_date,
                            'announcementCode': tr.find_all('td')[0].text,
                            'facts': tr.find_all('td')[1].text + tr.find_all('td')[5].text,
                            'defenseOpinion': '',
                            'defenseResponse': '',
                            'litigant': tr.find_all('td')[1].text,
                            'punishmentBasement': tr.find_all('td')[1].text + '上述行为违反了' + tr.find_all('td')[3].text,
                            'punishmentDecision': '依据' + tr.find_all('td')[4].text + '，' + '我局对' + tr.find_all('td')[
                                1].text + '作出以下处罚：' + tr.find_all('td')[2].text,
                            'type': '行政处罚决定',
                            'oss_file_id': file_id,
                            'status': 'not checked'
                        }
                        logger.info(result_map)
                        if db.announcement.find({'announcementTitle': title, 'oss_file_id': file_id}).count() == 0:
                            db.announcement.insert_one(result_map)
                            logger.info('大连保监局 数据解析 ' + ' -- 数据导入完成')
                        else:
                            logger.info('大连保监局 数据解析 ' + ' -- 数据已经存在')
                        result_map_list.append(result_map)
                if len(result_map_list) > 0:
                    db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                    logger.info('大连保监局 数据解析 ' + ' -- 修改parsed完成')
                else:
                    logger.warning('大连保监局 数据解析 ' + ' -- 没有数据')
        else:
            document_code_compiler = re.compile(r'((连保监罚(告)?|大银保监罚决字).\d{4}.\d+号|连保监罚\d{4}.\d+.号)')
            if document_code_compiler.search(content_text):
                document_code = document_code_compiler.search(content_text).group(1).strip()
                litigant_compiler = re.compile(
                    document_code.replace(r'[', r'\[').replace(r']', r'\]') +
                    r'\n([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|经调查|'
                                        r'中国人民财产保险股份有限公司大连市|'
                                        r'你公司于2010年10 月31日聘任朱利为国际保险部总经理|'
                                        r'你公司于2010年8 月下旬|'
                                        r'.*?(现场检查|综合性检查|案件调查|举报信反映)|'
                                        r'你公司于.*?期间|'
                                        r'近日，我局在处理投诉人郑金涛投诉你公司委托非法)')
                litigant = litigant_compiler.search(content_text).group(1).strip()
            else:
                if document_code_compiler.search(title):
                    document_code = document_code_compiler.search(title).group(1).strip()
                else:
                    document_code = ''
                litigant_compiler = re.compile(r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|经调查|'
                                                                  r'中国人民财产保险股份有限公司大连市|'
                                                                  r'你公司于2010年10 月31日聘任朱利为国际保险部总经理|'
                                                                  r'你公司于2010年8 月下旬|'
                                                                  r'.*?(现场检查|综合性检查|案件调查|举报信反映)|'
                                                                  r'你公司于.*?期间|近日，我局在处理投诉人郑金涛投诉你公司委托非法)')
                litigant = litigant_compiler.search(content_text).group(1).strip()

            litigant = litigant.replace('行政处罚决定书', '').replace('行政处罚事先告知书', '').strip()

            truth_text_str = r'((经查|经检查|检查发现|抽查|经抽查|经查明|存在下列(违法)?违规行为)' \
                             r'([\s\S]*?))' \
                             r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                             r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                             r'违反了.*?第.*?条|' \
                             r'二、处理意见|' \
                             r'违反了\n《保险法》第一百二十二条|' \
                             r'你公司的以上行为违反了《保险兼业代理管理暂行办法》第十三条|' \
                             r'根据《保险统计管理暂行规定》第三十八条的规定)'
            truth_compiler = re.compile(truth_text_str)
            if truth_compiler.search(content_text):
                truth = truth_compiler.search(content_text).group(1).strip()
            else:
                truth_text_str = litigant.replace(r'*', r'\*') \
                                 + r'([\s\S]*?)' \
                                   r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                   r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                                   r'我局于2014年12月10日以当面送达的方式第一次向你送达了行政处罚事先告知书|' \
                                   r'你在收到行政处罚事先告知书后，在法定时限内提出了如下陈述申辩意见|' \
                                   r'依据.*?第.*?条)'
                truth_compiler = re.compile(truth_text_str)
                truth = truth_compiler.search(content_text).group(1).strip()

            if '申辩' in content_text:
                defense_text_str = r'((针对.*?(行为|事实).*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?陈述申辩(意见)?|' \
                                   r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                                   r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见)中称|[^，。,；\n]*?在听证阶段提出|' \
                                   r'[^，。,；\n]*?在法定期限内(未)?提出(了)?(听证要求|陈述申辩|陈述申辩及听证要求)|' \
                                   r'你在收到行政处罚事先告知书后，在法定时限内提出了如下陈述申辩意见|' \
                                   r'你向我局提出申辩意见)' \
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
                                   r'我局.*?补充调查|' \
                                   r'经研究|我局认为)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense_list = defense_compiler.findall(content_text)
                if len(defense_list) != 0:
                    defense = defense_list[-1][0].strip()
                    defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                           + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                              r'对[^，。,；\n]*?申辩意见(不予|予以)采纳|因此.*?申辩理由.*?成立。|' \
                                                              r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                              r'经查不能作为减轻处罚的依据。|申辩意见不予采纳。|该申辩意见不成立。))'
                    defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                    if defense_response_compiler.search(content_text):
                        defense_response = defense_response_compiler.search(content_text).group(1).strip()
                    else:
                        if '未' in defense:
                            defense_response = ''
                else:
                    defense_text_str = '(可于2010年5月20日前到我局进行申辩，或于2010年5月20日前向我局提交书面的陈述书和申辩书。逾期视为放弃陈述和申辩。)'
                    defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                    defense = defense_compiler.search(content_text).group(1).strip()
                    defense_response = ''
            else:
                defense = defense_response = ''

            punishment_decision_text_str = r'(((依据|根据|依照)[^。；\n]*?第?.*?条.*?(规定)?.?' \
                                           r'(我局)?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你|对)|' \
                                           r'我局决定.*?作出(如下|以下)(行政)?处罚：|决定作出如下处罚|' \
                                           r'依据《中华人民共和国行政处罚法》第二十七条之规定、\n《中华人民共和国保险法》第一百六十二条、第一百七十二条之规定，我局决定|' \
                                           r'依据《行政处罚法》\n第二十七条、《保险法》第一百五十条规定，决定给予|' \
                                           r'依据《保险\n公司管理规定》第九十九条的规定，决定给予)' \
                                           r'([\s\S]*?))' \
                                           r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                           r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|当事人如对本处罚决定不服|' \
                                           r'请在接到本处罚决定之日起15日|如你公司对我局认定的违法事实)'

            punishment_decision_compiler = re.compile(punishment_decision_text_str)
            punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

            punishment_basis_str_list = [
                r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
                r'我局认为，中人保险经纪有限公司大连分公司违反了《中华人民共和国保险法》（2009年2月28日修订）第一百三十一条第（八）的规定',
                r'我局认为，中国大地财产保险股份有限公司大连市旅顺支公司违反了《中华人民共和国保险法》（2009年2月28日修订）第八十六条的规定',
                r'经查，你机构在内控制度及执行方面、业务合规性方面存在违规行为，\n违反了《保险公司管理规定》第五十六条、第六十七条的规定',
                r'2014年10月，我局对你公司开展了现场检查，你公司以营业场所租赁他用，业务台账、财务账册、原始单证无法找到为由，'
                r'拒绝向我局提供检查资料，导致我局无法正常开展现场检查工作，妨碍了监管部门依法监督检查，违反了《保险法》第一百五十六条的规定，属情节严重',
                r'2014年10月，我局对大连久久保险代理有限公司开展了现场检查，大连久久保险代理有限公司以营业场所租赁他用，业务台账、财务账册、原始单证无法找到为由，'
                r'拒绝向我局提供检查资料，导致我局无法正常开展现场检查工作，妨碍了监管部门依法监督检查，违反了《保险法》第一百五十六条的规定'
            ]
            punishment_basis_str = '|'.join(punishment_basis_str_list)
            punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                   '.(\n?依据|\n?根据|\n?鉴于|\n?属|\n?上述违法事实|\n?依照)', re.MULTILINE)
            punishment_basis_list = punishment_basis_compiler.findall(content_text)
            punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

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
                'announcementOrg': '大连银保监局',
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
                logger.info('大连保监局 数据解析 ' + ' -- 数据导入完成')
            else:
                logger.info('大连保监局 数据解析 ' + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('大连保监局 数据解析 ' + ' -- 修改parsed完成')
