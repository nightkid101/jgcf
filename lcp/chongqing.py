import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def chongqing_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '重庆保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if '行政处罚情况统计表' in announcement_title:
            logger.warning(announcement_url + ' ' + announcement_title + ' 与监管处罚无关')
            db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('重庆保监局' + ' 无关数据' + ' -- 修改status完成')
            continue

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('重庆保监局 ' + 'Url to parse: %s' % announcement_url)

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
            continue
        else:
            document_code_compiler = re.compile(r'((渝保监罚|渝银保监筹?罚决字).\d{4}.\d+号)')
            if document_code_compiler.search(content_text):
                document_code = document_code_compiler.search(content_text).group(1).strip()
                litigant_compiler = re.compile(
                    document_code.replace(r'[', r'\[').replace(r']', r'\]') + r'\n([\s\S]*?)\n'
                    + r'(经查|经检查|依据.*?的有关规定|'
                      r'.*?[于对].*?现场检查|'
                      r'.*?[于对].*?进行了现场检查|'
                      r'[\s\S]*?[于对].*?进行了.*?专项检查|'
                      r'.*?在.*?业务经营中|'
                      r'经抽查.*?发现)')
                litigant = litigant_compiler.search(content_text).group(1).strip()
            else:
                if document_code_compiler.search(title):
                    document_code = document_code_compiler.search(title).group(1).strip()
                else:
                    document_code = ''
                litigant_compiler = re.compile(r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|'
                                                                  r'.*?[于对].*?现场检查|'
                                                                  r'.*?[于对].*?进行了现场检查|'
                                                                  r'.*?[于对].*?进行了.*?专项检查|'
                                                                  r'.*?在.*?期间|.*?在.*?业务经营中|经抽查.*?发现)')
                litigant = litigant_compiler.search(content_text).group(1).replace('中国保监会重庆监管局行政处罚决定书', '').strip()

            truth_text_str = r'(经查，|经查,|经检查，|经查实，|检查发现，|现场检查，发现|经查实,)' \
                             r'([\s\S]*?)' \
                             r'((上述|以上)(事实|行为)(,|，)?有?[\s\S]*?等证据(在案)?证明.(足以认定。)?|' \
                             r'我局认为|' \
                             r'(上述|以上)(事实|行为)(,|，)?违反了.*?第.*?条的规定|' \
                             r'(依|根)据.*?第.*?条(的?规定)?，我局决定|作为保险兼业代理机构，你单位并不具有保险经纪公司的职能。)'
            truth_compiler = re.compile(truth_text_str)
            if truth_compiler.search(content_text):
                truth = truth_compiler.search(content_text).group(2).strip()
            else:
                truth_text_str = litigant.replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                     .replace(r'(', r'\(').replace(r')', r'\)') \
                                 + r'([\s\S]*?)' \
                                   r'((上述|以上)(事实|行为)(,|，)?有?[\s\S]*?等证据(在案)?证明.(足以认定。)?|' \
                                   r'依据.*?第.*?条(的规定)?，我局决定)'
                truth_compiler = re.compile(truth_text_str)
                truth = truth_compiler.search(content_text).group(1).strip()

            if '申辩' in content_text:
                defense_text_str = r'((针对.*?行为.*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?(陈述)?申辩(意见)?|' \
                                   r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                                   r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见)中称|[^，。,；\n]*?在听证阶段提出|' \
                                   r'[^，。,；\n]*?在法定期限内(未)?提出(了)?(听证要求|陈述申辩|陈述申辩及听证要求)|' \
                                   r'[^，。,；\n]*?主要陈述申辩理由为|[^，。,；\n]*?(陈述)?申辩认为|' \
                                   r'我局依法对你公司送达了《行政处罚事先告知书》，你公司提出申辩意见|[^，。,；\n]*?辩称)' \
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
                                   r'我局逐项复核后认为|' \
                                   r'针对.*?(陈述申辩|申辩意见|申辩).*?我局认为|对[^，。,；\n]*?的申辩意见，我局认为)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense_list = defense_compiler.findall(content_text)
                if len(defense_list) != 0:
                    defense = defense_list[-1][0]
                    defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                           + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                              r'对[^，。,；\n]*?申辩(意见)?(不予|予以)(采纳|采信)|因此.*?申辩理由.*?成立。|' \
                                                              r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                              r'[^，。,；\n]*?申辩事由(不)?符合.*?的条件|我局不予采信。|' \
                                                              r'以违法手段获得业绩增长危害市场公平竞争，应予否定。|' \
                                                              r'已是从轻处罚。|应依法从重处罚。|可以从轻处罚。|' \
                                                              r'但公司依法不具有减轻处罚情节。|可以适当从轻处罚。|' \
                                                              r'我局不予采信.*?的申辩意见。|已考虑了从轻处罚因素。|' \
                                                              r'我局在裁量时已充分考虑。|对当事人吴刚的申辩不予采信。|' \
                                                              r'可以在事先告知的拟处罚意见的基础上适当下调罚款金额。|' \
                                                              r'不予采信公司申辩意见。))'
                    defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                    if defense_response_compiler.search(content_text):
                        defense_response = defense_response_compiler.search(content_text).group(1).strip()
                    else:
                        if '未' in defense:
                            defense_response = ''
                else:
                    defense_text_str = r'([^。；\n]*?向.*?公告送达了《行政处罚事先告知书》.*?提出陈述申辩。|' \
                                       r'我局依法于2012年5月25日对你公司送达了《行政处罚事先告知书》，你公司在规定的时间内未提出陈述和申辩意见，也未要求举行听证。)'
                    defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                    defense = defense_compiler.search(content_text).group(1).strip()
                    defense_response = ''
            else:
                defense = defense_response = ''

            punishment_decision_text_str = r'(((根据|依据).*?第.*?条.*?(我局)?(决定|责令|对)|' \
                                           r'依据《中华人民共和国保险法》第一百六十一的规定|' \
                                           r'依据该条规定，我局决定|依据《保险法》第一百六十二的规定，我局决定|' \
                                           r'依据《农业保险条例》第二十九的规定，我局决定|' \
                                           r'依据《保险法》第162的规定，我局决定|' \
                                           r'综上，我局作出如下处罚)' \
                                           r'([\s\S]*?))' \
                                           r'(当事人应当在接到本处罚决定书之日|.*?如对本处罚决定不服|本处罚决定书自送达之日起执行|' \
                                           r'你公司如不服本处罚决定|请你.*?在接到本处罚决定书之日|如不服本处罚决定|请你公司在接到本处罚决定书之日|' \
                                           r'请.*?在接到本(行政)?处罚决定书之日|.*?应在接到本处罚决定书之日|你如对本处罚决定不服|' \
                                           r'当事人如对我局认定的违法事实、处罚理由及依据有异议)'
            punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
            punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

            punishment_basis_str_list = [
                r'([^\n。；]*?)(问题|行为|事项|情况|事实|我局认为)?([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
                r'盛信代理公司上述行为构成未保持职业责任保险的有效性和连续性，违反《保险专业代理机构监管规定》第三十七条的规定。袁平作为盛信代理公司总经理，对上述违法行为负有直接主管责任',
                r'泰康人寿江津中支使用引人误解的宣传材料，违反《保险公司管理规定》第四十五条的规定',
                r'我局认为.*?属于不按规定使用备案保险条款.*?',
                r'我局认为，保险公司应在《保险法》规定的时限内履行赔偿保险金义务。依据《保险法》第二十五条的规定，赔偿保险金的数额确定的，保险人应当于60日内予以赔付。人保财险重庆市分公司因工作人员失误，后续未及时采取正确处理措施等原因，导致在收齐人伤索赔资料后100余天，未向被保险人支付赔款，超过《保险法》规定的最长60日的赔付期限，构成拒不依法履行保险合同约定的赔偿保险金义务的行为，违反《保险法》第一百一十六条第（五）项的规定',
                r'我局认为，你公司记账凭证记载的支出广告费、会议费等费用与真实情况不符，构成了编制虚假资料的违法行为，且涉案金额巨大，情节特别严重',
                r'我局认为，上述行为已构成组织保险营销员考试作弊的违法行为，且涉及营销员较多，违法手段恶劣',
                r'上述行为违反了《保险法》（1995年6月30日第八届全国人民代表大会常务委员会第十四次会议通过，根据2002年10月28日第九届全国人民代表大会常务委员会第三十次会议《关于修改〈中华人民共和国保险法〉的决定》修正，下同）第一百二十二条的规定。你作为长寿区支公司经理，对该违法行为负有直接责任',
                r'上述行为违反了《保险法》（1995年6月30日第八届全国人民代表大会常务委员会第十四次会议通过，根据2002年10月28日第九届全国人民代表大会常务委员会第三十次会议《关于修改〈中华人民共和国保险法〉的决定》修正，下同）第一百二十二条的规定。你作为渝中区支公司银保部经理，对该违法行为负有直接责任',
                r'我局认为，阳光财险重庆分公司的上述行为构成未按照规定使用经备案的保险条款费率的行为.*?',
                r'我局认为，车险电销专用产品属于中国保监会审批条款，保险公司必须以直销形式销售，不得委托、雇用保险中介机构销售车险电销专用产品，不得支付手续费等中介费用。国寿财险重庆市分公司对中介渠道销售的商业车险业务使用电销专用条款，不符合电销专用条款的使用范围.*?',
                r'平安产险长寿支公司在被责令整改后继续不按规定使用车险电销专用条款，违法情节严重',
                r'安顺代理南岸分公司为了获取利益，明知是电销专用产品却仍然销售，不符合车险电销专用产品只能以直销形式销售的规定',
                r'.*?对中介渠道销售的商业车险业务使用电销专用条款，不符合电销专用条款的使用范围(。.*?直接实施了上述违法行为，负有直接(主管)?责任)?',
                r'我局认为，上述行为构成拖延承保交强险，违反《机动车交通事故责任强制保险条例》第十条的规定',
                r'我局认为，以上行为构成编制虚假的报告、报表、文件、资料的违法行为',
                r'我局认为，以上行为构成编制虚假的报告、报表、文件、资料的违法行为，徐红伟作为公司执行董事兼总经理，对该行为负有直接主管责任',
                r'我局认为，上述行为已构成组织保险营销员考试作弊的违法行为，且涉及营销员较多，违法手段恶劣。你作为支公司经理，知晓该违法行为而不加以制止，对该违法行为负有直接责任',
                r'我局认为，上述行为已构成组织保险营销员考试作弊的违法行为，且涉及营销员较多，违法手段恶劣。你是上述违法行为的直接参与者，对该违法行为负有直接责任',
                r'我局于2012年9月18日、25日依法向人保财险开县支公司及责任人黄勇军送达《行政处罚事先告知书》，认定人保财险开县支公司在收齐索赔资料后未依法及时对保险责任作出核定、无理扣减应赔付的医疗费、未赔付后期治疗费的行为构成拒不依法履行保险合同约定的赔偿保险金义务，违反《保险法》第一百一十六条第（五）项的规定',
                r'我局认为，上述行为构成未按照规定使用经批准的保险条款和保险费率的违法行为，黄千里作为太保产险重庆分公司营业五部业务科长，主要负责电话销售业务的管理，对该行为负有直接主管责任',
                r'我局认为，上述行为构成未按照规定使用经批准的保险条款和保险费率的违法行为。该行为是经张辉鹏审核同意后执行的，张辉鹏对该行为负有直接主管责任',
                r'我局于2012年9月18日、25日依法向人保财险开县支公司及责任人黄勇军送达《行政处罚事先告知书》，认定人保财险开县支公司在收齐索赔资料后未依法及时对保险责任作出核定、无理扣减应赔付的医疗费、未赔付后期治疗费的行为构成拒不依法履行保险合同约定的赔偿保险金义务，违反《保险法》第一百一十六条第（五）项的规定',
                r'人保财险重庆市分公司因工作人员失误，后续未及时采取正确处理措施等原因，导致在收齐人伤索赔资料后100余天，未向被保险人支付赔款，超过《保险法》规定的最长60日的赔付期限，构成拒不依法履行保险合同约定的赔偿保险金义务的行为，违反《保险法》第一百一十六条第（五）项的规定',
                r'上述行为违反了《保险法》（\n1995年6月30日\n第八届全国人民代表大会常务委员会第十四次会议通过，根据\n2002年10月28日\n第九届全国人民代表大会常务委员会第三十次会议《关于修改〈中华人民共和国保险法〉的决定》修正，下同）第一百二十二条的规定',
            ]
            punishment_basis_str = '|'.join(punishment_basis_str_list)
            punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                   r'.(\n?依据|\n?根据|\n?鉴于|\n?我局依法向.*?送达了|'
                                                   r'\n?我局于.*?依法向.*?送达了|\n?人保财险开县支公司提出申辩意见|'
                                                   r'\n?黄勇军提出申辩意见|\n?我局于2012年9月18日)', re.MULTILINE)
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
                'announcementOrg': '重庆银保监局',
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
            logger.info('重庆保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('重庆保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('重庆保监局 数据解析 ' + ' -- 修改parsed完成')
