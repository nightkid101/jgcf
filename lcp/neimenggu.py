import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def neimenggu_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '内蒙古保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('内蒙古保监局 ' + 'Url to parse: %s' % announcement_url)

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

        document_code_compiler = re.compile(r'((内保监罚|内银保监筹便函).*?\d{4}.*?\d+.*?号)')
        if document_code_compiler.search(content_text):
            document_code = document_code_compiler.search(content_text).group(1).strip()
            litigant_compiler = re.compile(
                document_code.replace(r'[', r'\[').replace(r']', r'\]') +
                r'\n([\s\S]*?)\n' + r'(经查|经检查|依据.*?有关规定|抽查'
                                    r'|经抽查|一、经查|一、 经查|'
                                    r'.*?我分局检查组对.*?进行了现场检查|'
                                    r'.*?(我局|内蒙古保监局).*?对.*?(专项检查|现场检查|投诉检查|举报检查|举报核查|业务检查|'
                                    r'被举报事项进行了检查|信访核查|专项核查|延伸检查|迁址事项的核查|立案调查)|'
                                    r'.*?考生.*?代替考生.*?参加.*?考试|'
                                    r'.*?代替.*?参加.*?考试|'
                                    r'2009年5月至2010年6月，李吉庆在中国人民人寿保险股份有限公司呼和浩特分公司共投保了19份保险|'
                                    r'在2006年6月的保险中介专项检查)')
            litigant = litigant_compiler.search(content_text).group(1).replace('中国保监会内蒙古监管局行政处罚决定书', '').strip()
        else:
            if document_code_compiler.search(title):
                document_code = document_code_compiler.search(title).group(1).strip()
            else:
                document_code = ''
            litigant_compiler = re.compile(r'^([\s\S]*?)\n' +
                                           r'(经查|经检查|依据.*?有关规定|抽查|经抽查|一、经查|'
                                           r'.*?我分局检查组对.*?进行了现场检查|一、 经查|'
                                           r'.*?(我局|内蒙古保监局).*?对.*?(专项检查|现场检查|投诉检查|举报检查|举报核查|'
                                           r'业务检查|被举报事项进行了检查|信访核查|专项核查|延伸检查|迁址事项的核查|立案调查)|'
                                           r'.*?代替.*?参加.*?考试|'
                                           r'2009年5月至2010年6月，李吉庆在中国人民人寿保险股份有限公司呼和浩特分公司共投保了19份保险|'
                                           r'在2006年6月的保险中介专项检查)')
            litigant = litigant_compiler.search(content_text).group(1).replace('中国保监会内蒙古监管局行政处罚决定书', '').strip()

        truth_text_str = '(检查发现，该公司存在以下违法违规行为：\n委托未取得合法资格的机构或个人从事保险销售活动。|' \
                         '检查发现，该公司存在以下违法违规行为：\n该公司委托未取得合法资格的机构或个人从事保险销售活动|' \
                         '检查发现，该公司存在以下违法违规行为：\n该公司截留保险费行为违反了《保险法》第一百一十六条第七项规定|' \
                         '检查发现，该公司存在以下违法违规行为：\n该公司财务数据不真实，不实列支营业费用的行为违反了《保险法》第八十六条第二款规定。)'
        truth_compiler = re.compile(truth_text_str)
        if truth_compiler.search(content_text):
            truth = truth_compiler.search(content_text).group(1).strip()
        else:
            truth_text_str = r'((经查|经检查|检查发现|现场检查，发现|抽查|经抽查|' \
                             r'存在下列违法行为：|核查发现，)' \
                             r'([\s\S]*?))' \
                             r'((我局认为，)?(上述|以上).*?(事实|行为|事实).*?(,|，)?有.*?等.*?证明(,|，|。)?(足以认定。)?|' \
                             r'(,|，|。|\n|\s)(依据|按照)|\n.*?辩称|上述行为，情节严重，我局决定|' \
                             r'你公司未对我局行政处罚事先告知书提出申辩意见|' \
                             r'经研究你公司申辩意见)'
            truth_compiler = re.compile(truth_text_str)
            # truth_list = truth_compiler.findall(content_text)
            if truth_compiler.search(content_text):
                truth = truth_compiler.search(content_text).group(1).strip()
            else:
                truth_text_str = litigant + \
                                 r'([\s\S]*?)' \
                                 r'((我局认为，)?(上述|以上).*?(事实|行为|事实).*?(,|，)?有.*?等证据(在案)?(证明)?(,|，|。)(足以认定。)?|' \
                                 r'我局已?向.*?下达了)'
                truth_compiler = re.compile(truth_text_str)
                truth = truth_compiler.search(content_text).group(1).strip()

        if '申辩' in content_text:
            defense_text_str = r'((针对.*?行为.*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?陈述申辩(意见)?|' \
                               r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                               r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见)中称|[^，。,；\n]*?在听证阶段提出|' \
                               r'[^，。,；\n]*?在法定期限内(未)?提出(了)?(听证要求|陈述申辩|陈述申辩及听证要求)|' \
                               r'[^，。,；\n]*?提交书面(陈述)?申辩材料|[^，。,；\n、]*?辩称|[^，。,；\n]*?公司申辩|你公司提出的申辩|' \
                               r'你公司辩称：印制违规计划书为代理人个人行为，且未造成明显或潜在客户利益损失)' \
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
                               r'我局对[^，。,；\n]*?申辩意见做了认真复核|' \
                               r'经审核|经研究|经核查|经查|' \
                               r'我局对[^，。,；\n]*?申辩意见进行了研究|' \
                               r'你公司申辩理由均为任命白洁为执行董事后的情形|' \
                               r'你公司没有提供有效证据|' \
                               r'张成富购买保单为国寿呼和浩特分公司承保|' \
                               r'我局认为，你公司股权转让，既转让权利，也转让义务|' \
                               r'你公司未提供有效证据证明|采纳申辩意见|' \
                               r'我局认为|内蒙古保监局认为|经复核|对你公司的申辩意见)'
            defense_compiler = re.compile(defense_text_str, re.MULTILINE)
            defense_list = defense_compiler.findall(content_text)
            if len(defense_list) != 0:
                defense = defense_list[-1][0].strip()
                defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') + \
                                       r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                        r'[^，。,；\n]*?申辩(意见|理由)(不予|予以|部分)采纳|因此.*?申辩.*?成立。|' \
                                                        r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                        r'其它申辩意见不予采纳。|有从轻处罚的依据|决定部分采纳你公司申辩意见。|' \
                                                        r'综上，你公司关于主管人员处罚的申辩意见不予采纳。|' \
                                                        r'已对你公司从轻处罚。|故不予采纳。|不采纳申辩意见。|' \
                                                        r'不予采纳你公司申辩意见。|采纳申辩意见|我局决定免予对该公司法定代表人的行政处罚。|' \
                                                        r'申辩理由不予认可。|申辩意见不予采纳。|我局不予采纳。|' \
                                                        r'故你公司提出的申辩意见不予采纳。|故维持原处罚决定。))'
                defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                if defense_response_compiler.search(content_text):
                    defense_response = defense_response_compiler.search(content_text).group(1).strip()
                else:
                    if '未' in defense:
                        defense_response = ''
            else:
                defense_text_str = '([^。；\n]*?向.*?公告送达了《行政处罚事先告知书》.*?提出陈述申辩。|' \
                                   '我局依法于2012年5月25日对你公司送达了《行政处罚事先告知书》，你公司在规定的时间内未提出陈述和申辩意见，也未要求举行听证。|' \
                                   '你公司未对我局行政处罚事先告知书提出申辩意见|上述三人未向我局提交申辩意见|' \
                                   '你公司未按规定使用经批准或者备案的保险条款、保险费率的申辩意见予以采纳。)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                if defense_compiler.search(content_text):
                    defense = defense_compiler.search(content_text).group(1).strip()
                    defense_response = ''
                else:
                    defense = ''
                    defense_response_text_str = '(你公司未按规定使用经批准或者备案的保险条款、保险费率的申辩意见予以采纳。|' \
                                                '你公司利用保险兼业代理人虚构中介业务支取手续费的申辩意见予以采纳。|' \
                                                '公司申辩意见予以采纳|步行街邮政支局没有提供可以减轻处罚的相关证据，申辩理由不予采纳。)'
                    defense_response_compiler = re.compile(defense_response_text_str, re.MULTILINE)
                    defense_response = defense_response_compiler.search(content_text).group(1).strip()
        else:
            defense = defense_response = ''

        punishment_decision_text_str = r'(((依据|根据|按照)[^。；\n]*?第[^。；\n]*?条[^。；\n]*?(规定)?.?(我局)?' \
                                       r'(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你)|' \
                                       r'我局决定.*?作出(如下|以下)(行政)?处罚|综上，我局作出如下处罚|' \
                                       r'依据本条规定，我局决定|上述行为，情节严重，我局决定|' \
                                       r'依据本法第[^。；\n]*?条规定，决定对你|依据本法第一百七十四规定，我局决定|' \
                                       r'依据本法第一百六十七条第一项规定，决定对|' \
                                       r'依据《保险营销员管理规定》，决定给予|' \
                                       r'上述行为违反了《中华人民共和国保险法》第一百三十一条规定，“保险代理人、保险经纪人及其从业人员在办理保险业务活动中不得有下列行为：' \
                                       r'（二）隐瞒与保险合同有关的重要情况”，依据本法第一百七十四条规定，“个人保险代理人违反本法规定的，由保险监督管理机构给予警告，' \
                                       r'可以并处二万元以下的罚款；情节严重的，处二万元以上十万元以下的罚款，并可以吊销其资格证书”，我局决定)' \
                                       r'([\s\S]*?))' \
                                       r'(请在本处罚决定书送达之日|[^。；\n]*?应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                       r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|[^。；\n]*?对本处罚决定不服|' \
                                       r'如不服本决定|望你公司认真吸取经验教训，切实增强依法合规经营意识)'

        punishment_decision_compiler = re.compile(punishment_decision_text_str)
        punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

        punishment_basis_str_list = [
            r'([^\n。]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'你的上述行为，违法了保险法该项规定',
            r'上述行为情节严重。中国银行内蒙古分行违反了《中华人民共和国保险法》第一百三十二条、第八十六条，中国银行呼和佳地支行违反了《中华人民共和国保险法》第一百三十一条第一款规定',
            r'一、未按照规定建立会计科目和会计帐簿，违反了《保险代理机构管理规定》第九十五条。\n'
            r'二、未对主营业务收入做帐务处理，部分会计凭证未附原始票据和明细清单以及会计凭证缺失，违反了《保险中介公司会计核算办法》的有关规定',
            r'一、未按照规定建立会计科目和会计帐簿，违反了《保险代理机构管理规定》第九十五条。\n'
            r'二、2006年6月以前，一直未使用《保险中介服务统一发票》，违反了《国家税务总局、中国保监会关于规范保险中介服务发票管理有关问题的通知》',
            r'一、未按照规定建立会计科目和会计帐簿，违反了《保险代理机构管理规定》第九十五条。\n'
            r'二、任免高级管理人员未及时报告，违反了《保险代理机构管理规定》第七十四条。\n三、报送的报表数据不真实，违反了《保险代理机构管理规定》第一百零七条',
            r'上述行为，违法了《中华人民共和国保险法》第一百二十五条规定',
            r'对公司未按照规定缴存保证金或者投保职业责任保险违反了《保险法》第一百二十四条行为',
            r'上述事实行为违反了《保险代理机构管理规定》、《国家税务总局、中国保监会关于规范保险中介服务发票管理有关问题的通知》',
            r'上述行为情节严重。中国银行内蒙古分行违反了《中华人民共和国保险法》第一百三十二条、第八十六条，中国银行呼和佳地支行违反了《中华人民共和国保险法》第一百三十一条第一款规定'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'[。\n；、]' + '(' + punishment_basis_str + ')' +
                                               r'.(\n?(应当)?依据|\n?根据|\n?鉴于|\n?按照|'
                                               r'上述行为，情节严重，我局决定|\n人寿财险内蒙古分公司在申辩材料中称|'
                                               r'\n中国人寿呼和浩特分公司在申辩材料中称|\n人保财险呼市分公司在申辩材料中称|'
                                               r'\n?二、未|\n?三、未|\n?四、未|\n?五、转让|\n?六、编制|七、不)',
                                               re.MULTILINE)
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
            'announcementOrg': '内蒙古银保监局',
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
            logger.info('内蒙古保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('内蒙古保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('内蒙古保监局 数据解析 ' + ' -- 修改parsed完成')
