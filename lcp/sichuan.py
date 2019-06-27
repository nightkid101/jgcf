import re

from utility import cn2dig, get_year, request_site_page, get_content_text, table_to_list, format_date
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def sichuan_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '四川保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if '行政处罚实施情况' in announcement_title:
            logger.warning(announcement_url + ' ' + announcement_title + ' 与监管处罚无关')
            db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('四川保监局' + ' 无关数据' + ' -- 修改status完成')
            continue

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('四川保监局 ' + 'Url to parse: %s' % announcement_url)

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

        if '行政处罚事项' in title or '行政处罚公开信息' in title:
            table_list = table_to_list(table_content.find_all('tr')[3].find_all('table')[-1])
            result_map_list = []
            for each_table_list in table_list:
                if each_table_list[0] == '序号':
                    continue
                result_map_list.append({
                    'announcementTitle': '四川银保监局行政处罚决定书（' + each_table_list[2] + '）',
                    'announcementOrg': '四川银保监局',
                    'announcementDate': format_date(each_table_list[1]),
                    'announcementCode': each_table_list[2],
                    'facts': each_table_list[4],
                    'defenseOpinion': '',
                    'defenseResponse': '',
                    'litigant': each_table_list[3],
                    'punishmentBasement': each_table_list[5],
                    'punishmentDecision': '依据' + each_table_list[6] + '，' + each_table_list[7],
                    'type': '行政处罚决定',
                    'oss_file_id': file_id,
                    'status': 'not checked'
                })
            logger.info(result_map_list)
            if len(result_map_list) > 0:
                logger.info('四川保监局' + '解析 -- 一共有%d条数据' % len(result_map_list))
                db.announcement.insert_many(result_map_list)
                logger.info('四川保监局' + '解析 -- 数据导入完成')
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('四川保监局' + '解析 -- 修改parsed完成')
            else:
                logger.warning('四川保监局' + '解析 -- 解析未能完成')
        else:
            if '处罚决定书送达公告' in title or '事先告知书送达公告' in title:
                document_code_compiler = re.compile(r'(川保监公告.\d{4}.\d+号)')
                document_code = document_code_compiler.search(title).group(1).strip()
                litigant_compiler = re.compile(r'^([\s\S]*?)：\n' + '(经查|经检查|依据.*?有关规定)')
                litigant = litigant_compiler.search(content_text).group(1).replace('四川保监局行政处罚事先告知书送达公告', '').strip()
            else:
                document_code_compiler = re.compile(r'((川|宜宾)银?保监罚决?字?.\d{4}.\d+号)')
                if document_code_compiler.search(content_text):
                    document_code = document_code_compiler.search(content_text).group(1).strip()
                    litigant_compiler = re.compile(
                        document_code.replace(r'[', r'\[').replace(r']', r'\]') + r'\n([\s\S]*?)\n' +
                        r'(经查|经检查|依据.*?有关规定|'
                        r'.*?期间|经我局查实|'
                        r'.*?存在.*?违[法规]行为|'
                        r'.*?委托未取得合法资格的个人从事保险销售活动|'
                        r'.*?我局对.*?(开展|进行)了.*?现场检查|'
                        r'财政部驻四川财政监察专员办事处检查发现|'
                        r'根据举报，我局查实)')
                    litigant = litigant_compiler.search(content_text).group(1).strip()
                else:
                    if document_code_compiler.search(title):
                        document_code = document_code_compiler.search(title).group(1).strip()
                    else:
                        document_code = ''
                    litigant_compiler = re.compile(r'^([\s\S]*?)\n' +
                                                   r'(经查|经检查|依据.*?有关规定|.*?期间|.*?存在.*?违[法规]行为|'
                                                   r'.*?委托未取得合法资格的个人从事保险销售活动|经我局查实|'
                                                   r'.*?我局对.*?(开展|进行)了.*?现场检查|'
                                                   r'财政部驻四川财政监察专员办事处检查发现|'
                                                   r'根据举报，我局查实)')
                    litigant = litigant_compiler.search(content_text).group(1).strip()

            truth_text_str = r'(经查，|经查,|经检查，|经查实，|检查发现，|现场检查，发现|检查发现：|现场检查。检查发现|我局查实|经我局查实，)' \
                             r'([\s\S]*?)' \
                             r'(上述(违法|违规)?(事实|行为)(及相关人员责任)?(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                             r'根据《中华人民共和国保险法》第一百七十一条的规定|当事人唐晓峰在申辩材料中辩称:|' \
                             r'中国平安财产保险股份有限公司成都市锦城支公司的上述违法行为中|' \
                             r'(该|你)公司.*?存在的.*?的.*?行为|上述行为违反了.*?第.*?条.*?规定|' \
                             r'2009年10月1日前经营活动中存在的上述违法行为，违反了《中华人民共和国保险法》（修订前）第122条的规定|' \
                             r'上述行为分别违反了《人身保险新型产品信息披露管理办法》（保监会令〔2009〕3号）第10条和第33条的规定|' \
                             r'，违反了《中华人民共和国保险法》（2009年2月28日修订）第一百三十三条、《保险专业代理机构监管规定》第六十一条的规定|' \
                             r'你的行为违反了《中华人民共和国保险法》（2009年2月28日修订）第一百二十二条的规定|' \
                             r'分别违反了《保险专业代理机构监管规定》第四十九条、第六十一条和《中华人民共和国保险法》（2009年2月28日修订）第一百三十三条的规定|' \
                             r'上述行为分别违反了《中华人民共和国保险法》第116条第（八）款和《保险统计管理暂行规定》（保监会令〔2004〕11号）第25条的规定|' \
                             r'上述行为违反了《中华人民共和国保险法》第116条第（十\n三）项和《机动车交通事故责任强制保险条例》第10条的规定|' \
                             r'，应该按照该法第一百七十三条予以处罚|' \
                             r'你公司的上述违法行为中，保费收入不真实、车辆使用费不真实的行为违反了《中华人民共和国保险法》（2009年2月28日修订）第八十六条的规定，应当按照该法第一百七十二条予以处罚|' \
                             r'上述行为分别违反了《中华人民共和国保险法》（2002年修正版）第122条、《中华人民共和国保险法》' \
                             r'（2009年修订版）第116条规定和《保险统计管理暂行规定》（保监会令（2004）11号）第25条规定|' \
                             r'上述行为违反了《中华人民共和国保险法》（修订前，下同）第106条的规定|' \
                             r'上述行为违反了原《保险法》第107条、第122条及《保险营销员管理规定》第43条的规定，新《保险法》第86条、第136条也作出了相应规定|' \
                             r'该行为违反了《保险代理机构管理规定》第2条的规定。依据《保险代理机构管理规定》第130条第一款的规定|' \
                             r',该行为违反《保险代理机构管理规定》第56条的规定|' \
                             r'上述行为违反了《中华人民共和国保险法》第122条的规定|' \
                             r'上述行为违反了《中华人民共和国保险法》第107条的规定|' \
                             r'依据《保险代理机构管理规定》第141条，我局决定|' \
                             r'上述行为违反了《中华人民共和国保险法》第132条|' \
                             r'上述行为违反了《中华人民共和国保险法》第80条规定)'
            truth_compiler = re.compile(truth_text_str)
            if truth_compiler.search(content_text):
                truth = truth_compiler.search(content_text).group(2).strip()
            else:
                truth_text_str = litigant + r'([\s\S]*?)' \
                                            r'(\n上述(违规|违法)?(事实|行为)(及相关人员责任)?(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                            r'上述行为分别违反了《保险营销员管理规定》第三十六条第（十八）项和第（二十一）项的规定|' \
                                            r'上述行为违反了《中华人民共和国保险法》（修订前，下同）第107条的规定|' \
                                            r'综上，决定对你公司处以罚款人民币壹万伍仟元（¥15,000.00）的行政处罚。)'
                truth_compiler = re.compile(truth_text_str)
                truth = truth_compiler.search(content_text).group(1).strip()

            if '申辩' in content_text:
                defense_text_str = r'((针对.*?行为.*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?陈述申辩(意见)?|' \
                                   r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                                   r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见)中(辩)?称|[^，。,；\n]*?在听证阶段提出|' \
                                   r'[^，。,；\n]*?在法定期限内(未)?提出(了)?(听证要求|陈述申辩|陈述申辩及听证要求)|' \
                                   r'在规定的?期限内，[^，。,；\n]*?提交了(陈述申辩材料|陈述申辩书面材料|书面陈述申辩材料))' \
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
                                   r'经复核)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense_list = defense_compiler.findall(content_text)
                if len(defense_list) != 0:
                    defense = defense_list[-1][0]
                    defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                           + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                              r'对[^，。,；\n]*?申辩意见(不予|予以)采纳。|因此.*?申辩理由.*?成立。|' \
                                                              r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                              r'不予处罚的情形。|足以认定其并非违法行为的直接责任人。|' \
                                                              r'我局对.*?请求不予采纳。))'
                    defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                    if defense_response_compiler.search(content_text):
                        defense_response = defense_response_compiler.search(content_text).group(1).strip()
                    else:
                        if '未' in defense:
                            defense_response = ''
                else:
                    defense_text_str = r'(你享有陈述和申辩的权利，可在送达之日起10个工作日内将陈述和申辩的书面材料提交至中国保险监督管理委员会四川监管局。逾期视为放弃陈述权和申辩权。|' \
                                       r'在规定的?期限内.*?(未|没有)进行陈述和?申辩。|' \
                                       r'在规定的期限内.*?既未进行陈述和申辩，也未要求举行听证。)'
                    defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                    defense = defense_compiler.search(content_text).group(1).strip()
                    defense_response = ''
            else:
                defense = defense_response = ''

            punishment_decision_text_str = r'(((依据|根据|按照).*?第.*?条.*?规定，(我局决定)?|依据前述法律规定，我局决定|' \
                                           r'我局经复核认为|我局决定.*?作出(如下|以下)(行政)?处罚：|' \
                                           r'依据《中华人民共和国保险法》第172条、《保险营销员管理规定》第57条，我局决定|' \
                                           r'依据《保险营销员管理规定》第57条，我局决定|' \
                                           r'依据《中华人民共和国保险法》第147条和《行政处罚法》第27条，我局决定|' \
                                           r'依据《中华人民共和国保险法》第145条的规定，我局决定|' \
                                           r'依据《保险代理机构管理规定》第141条，我局决定|' \
                                           r'依据《中华人民共和国保险法》第145条和《保险代理机构管理规定》第138条的规定，我局决定|' \
                                           r'依据《中华人民共和国保险法》第145条，我局决定)' \
                                           r'([\s\S]*?))' \
                                           r'(\n.*?本处罚决定书之日|现依法向你公告送达上述决定，自公告之日起经过60日视为送达。|' \
                                           r'如不服本处罚决定|请严格按照《关于调整非税收入执行单位和管理办法的通知》（川保监发〔2007〕212号）规定)'
            punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
            punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

            punishment_basis_str_list = [
                r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
                r'我局认为，阳光人寿四川分公司利用保险代理人，从事以虚构保险中介业务方式套取费用等违法活动，违反了《保险法》第116条第（十）项的规定',
                r'我局认为，人保隆昌支公司故意编造未曾发生的保险事故、故意夸大已经发生的保险事故的损失程度进行虚假理赔，骗取保险金或者牟取其他不正当利益，违反了《保险法》第116条第（六）项的规定',
                r'经查，成都五丰保险代理有限公司存在编制并提供虚假的业务台帐的违法违规行为，违反了《中华人民共和国保险法》'
                r'（2009年2月28日修订）第一百三十三条、《保险专业代理机构监管规定》第六十一条的规定。你时任成都五丰保险代理有限公司总经理，系对该违法违规行为直接负责的主管人员',
                r'经查，成都五丰保险代理有限公司存在编制并提供虚假的业务台帐的违法违规行为，违反了《中华人民共和国保险法》'
                r'（2009年2月28日修订）第一百三十三条、《保险专业代理机构监管规定》第六十一条的规定。你时任成都五丰保险代理有限公司业务主任，系该违法违规行为的直接责任人员'
            ]
            punishment_basis_str = '|'.join(punishment_basis_str_list)
            punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                   '.(\n?依据|\n?根据|\n?鉴于|\n?(应当)?按照)', re.MULTILINE)
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
                'announcementTitle': title,
                'announcementOrg': '四川银保监局',
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
                    {'announcementTitle': result_map['announcementTitle'],
                     'oss_file_id': file_id,
                     'litigant': result_map['litigant']}).count() == 0:
                db.announcement.insert_one(result_map)
                logger.info('四川保监局 数据解析 ' + ' -- 数据导入完成')
            else:
                logger.info('四川保监局 数据解析 ' + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('四川保监局 数据解析 ' + ' -- 修改parsed完成')
