import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def guangdong_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '广东保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('广东保监局 ' + 'Url to parse: %s' % announcement_url)

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

        document_code_compiler = re.compile(r'((粤保监[罚发]|粤银保监罚决字|'
                                            r'粤银保监\(筹\)罚决字|粤银保监\(筹\)罚告字).\d{4}.\d+号|粤保监发.2007.280)')
        if document_code_compiler.search(content_text):
            document_code = document_code_compiler.search(content_text).group(1).strip()
            litigant_compiler = re.compile(
                document_code.replace(r'[', r'\[').replace(r']', r'\]').replace(r'(', r'\(').replace(r')', r'\)') +
                r'\n([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|'
                                    r'抽查|经抽查|'
                                    r'根据.*经查|'
                                    r'一|'
                                    r'.*?期间|.*现场检查|'
                                    r'2011年12月24日和2012年3月17日|'
                                    r'.*?存在.*?行为|'
                                    r'你中心支公司2007年4月起|'
                                    r'我局于\n2007年12月11日|'
                                    r'我局自|'
                                    r'.*?进行了调查。|'
                                    r'.*进行了检查)')
            litigant = litigant_compiler.search(content_text).group(1).strip()
        else:
            if document_code_compiler.search(title):
                document_code = document_code_compiler.search(title).group(1).strip()
            else:
                document_code = ''
            litigant_compiler = re.compile(r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|根据.*经查|'
                                                              r'一|.*?期间|.*现场检查|2011年12月24日和2012年3月17日|'
                                                              r'.*?存在.*?行为|你中心支公司2007年4月起|我局于\n2007年12月11日|'
                                                              r'我局自|.*?进行了调查。|.*进行了检查)')
            litigant = litigant_compiler.search(content_text).group(1).strip()

        litigant = litigant.replace('中国保监会广东监管局行政处罚决定书', '').strip()

        truth_text_str = r'((经查，|经查,|经检查，|经查实，|检查发现，|现场检查，发现|抽查，|经抽查，|经查.*?存在以下问题：)' \
                         r'([\s\S]*?))' \
                         r'((我局认为，)?(上述|以上|该).*?(事实|行为|事实|情况)(,|，)?(有|由).*?等?(相关)?(证据)?(材料|资料)?(在案)?证明.?(足以认定。)?|' \
                         r'(我局认为，|综上，)?[^，。,；\n]*?(上述|以上).*?(行为|问题|事实|情况|做法).*?违反.*?第.*条.*?(的规定)?|' \
                         r'请在接到本处罚决定书之日起15日|' \
                         r'我局于.*?送达了|' \
                         r'该中心支公司委托资格证过期代理人展业并为资格证过期代理人上工号的行为|' \
                         r'\n二、.*?行为|' \
                         r'你[^，。,；\n]*?的行为[^。；\n]*?违反|' \
                         r'该行为违反了|' \
                         r'依据《保险营销员管理规定》第五十四条的规定|' \
                         r'[^，。,；\n]*?上述行为违反|' \
                         r'新捷胜汽修厂非法经营保险代理业务违反了《中华人民共和国保险法》（2002年修正，下同）第一百三十二条的规定|' \
                         r'\n三、根据|上述向银行支付手续费以外的费用，导致财务业务数据不真实的行为|' \
                         r'依据《中华人民共和国保险法》第一百四十七条第（一）项的规定|' \
                         r'你在.*?任.*?对.*?负有直接责任|' \
                         r'依据《保险公司管理规定》第九十九条“保险机构或者其工作人员违反本规定|' \
                         r'你公司存在提供位于广东省信宜市人民北路433号的经营场所作为未经保险监管部门批准的车险出单点经管的行为|' \
                         r'依照《保险法》第一百四十九条“违反本法规定|' \
                         r'你在未获得朱祖英等六位投保人同意的情况下代替上述六人签署|' \
                         r'你在代理投保人吴静敏的保险业务的过程中存在摹仿客户字迹代为签名|' \
                         r'依据.*?第.*?条.*?的规定|' \
                         r'根据中华湛江出具的《关于石均民同志任职湛江的情况说明》|' \
                         r'依据《中华人民共和国保险法》第一百五十条规定)'
        truth_compiler = re.compile(truth_text_str)
        if truth_compiler.search(content_text):
            truth = truth_compiler.search(content_text).group(1).strip()
        else:
            truth_text_str = litigant + r'([\s\S]*?)' \
                                        r'((我局认为，)?(上述|以上|该).*?(事实|行为|问题|情况)(,|，)?(有|由).*?等?(相关)?证据(材料|资料)?(在案)?证明.?(足以认定。)?|' \
                                        r'(我局认为，|综上，)?[^，。,；\n]*?(上述|以上).*?(行为|问题|事实|情况).*?违反.*?第.*条.*?(的规定)?|' \
                                        r'二、对于.*?行为|' \
                                        r'依据.*?第.*?条.*?的规定)'
            truth_compiler = re.compile(truth_text_str)
            truth = truth_compiler.search(content_text).group(1).strip()

        if '申辩' in content_text:
            defense_text_str = r'((针对.*?行为.*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?陈述申辩(意见)?|' \
                               r'[^，。,；\n]*?向我局(报送|递交|提出|提交)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                               r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见)中称|[^，。,；\n]*?在听证阶段提出|' \
                               r'[^，。,；\n]*?在法定期限内(未)?提出(了)?(听证要求|陈述申辩|陈述申辩及听证要求)|' \
                               r'你公司于2018年8月24日提出申辩。|' \
                               r'[^，。,；\n]*?提出.*?陈述和申辩|' \
                               r'[^，。,；\n]*?提交了?陈述申辩(材料|报告)|' \
                               r'[^。；\n]*?(辩|提交报告)称|' \
                               r'[^，。,；\n]*?陈述申辩中提出|' \
                               r'[^，。,；\n]*?陈述申辩.*?提出异议|' \
                               r'我局向你公司送达《行政处罚事先告知书》（粤保监中介〔2013〕50号）后，你公司向我局提出陈述和申辩)' \
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
                               r'经核查|' \
                               r'经我局核查|' \
                               r'我局核查结果|' \
                               r'经我局核查|' \
                               r'经审核|' \
                               r'我局核查认为|' \
                               r'三、我局核查认为|' \
                               r'我局核查意见如下|' \
                               r'我局认为|' \
                               r'而根据|' \
                               r'经核查)'
            defense_compiler = re.compile(defense_text_str, re.MULTILINE)
            defense_list = defense_compiler.findall(content_text)
            if len(defense_list) != 0:
                defense = defense_list[-1][0].strip()
                defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                       + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                          r'对[^，。,；\n]*?申辩意见(不予|予以)采纳|' \
                                                          r'.*?(申辩理由|陈述申辩|申辩的部分事实和理由).*?成立。|' \
                                                          r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                          r'申辩理由不影响.*?认定。|也不构成减轻或免于处罚的依据。|' \
                                                          r'我局行政处罚裁量已依法充分考虑你分公司违法行为情节较轻等情况。|' \
                                                          r'我局决定维持原(行政处罚决定|处罚决定|处理意见)(不变)?。|' \
                                                          r'.*?(构成|成为).*?依据。|' \
                                                          r'不影响我局事实认定与处理。|' \
                                                          r'较其他问题性质更为恶劣。|' \
                                                          r'.*?符合.*?法定.*?条件。|' \
                                                          r'综合上述情况，我局决定.*?。|' \
                                                          r'不足以改变.*?结果。|' \
                                                          r'现有检查事实清楚，证据确凿。|' \
                                                          r'对其处理意见不变。|' \
                                                          r'不影响该事实行为的存在。|' \
                                                          r'.*?免予行政处罚。|' \
                                                          r'自查自纠的情节不能认定。|' \
                                                          r'申辩理由不成立。))'
                defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                if defense_response_compiler.search(content_text):
                    defense_response = defense_response_compiler.search(content_text).group(1).strip()
                else:
                    if '未' in defense:
                        defense_response = ''
            else:
                defense_text_str = '(我局于2012年6月15日向你营销服务部送达《行政处罚事先告知书》，截至陈述申辩期满，我局未收到你营销服务部陈述和申辩。|' \
                                   '你分公司向我局提交陈述申辩报告的时间已超期，故我局对你分公司陈述申辩意见不予采纳。|' \
                                   '我局于.*?送达《行政处罚事先告知书》.*?提出陈述和申辩。)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense = defense_compiler.search(content_text).group(1).strip()
                defense_response = ''
        else:
            defense = defense_response = ''

        punishment_decision_text_str = r'(((依据|根据).*?第?.*?条?.*?(规定)?.?(我局)?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你|对你)|' \
                                       r'我局经复核认为|我局决定.*?作出(如下|以下)(行政)?处罚：|' \
                                       r'依据《中华人\n*民共和国保险法》第一百七十三条的规定，我局决定|' \
                                       r'依据《中\n*华人民共和国保险法》第一百七十一条的规定，我局决定|' \
                                       r'请在接到本处罚决定书之日|依照《保险法》第一百四十九条“违反本法规定|' \
                                       r'依据《\n中华人民共和国\n保险法》第一百四十七条第（一）项的规定，我局决定|' \
                                       r'依照《中华人民共和国保险法》第一百四十九条的规定，结合《行政处罚法》第二十七条第一款第（三）项的规定，我局决定|' \
                                       r'依据《保险营销员管理规定》\n第五十八条的规定，我局决定)' \
                                       r'([\s\S]*?))' \
                                       r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                       r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|请你在接到本处罚决定书之日|' \
                                       r'我局于.*?向你.*?送达|自接到本处罚决定书之日)'

        punishment_decision_compiler = re.compile(punishment_decision_text_str)
        punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

        punishment_basis_str_list = [
            r'([^\n。；]*?)(问题|行为|事项|情况|事实|做法)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?.*?\n?.*?((的|之|等)(相关)?规定)?',
            r'上述事实行为违反了《中华人民共和国保险法》第一百二十二条的规定。\n作为该中心支公司主要负责人，对2005年7月20日之后发生的违法行为负有直接责任',
            r'新华人寿东莞中支未严格按照报备条款承保团体保险违反了《中华人民共和国保险法》第一百三十六条第一款的规定',
            r'该支公司委托无展业证人员从事保险销售，违反了《保险营销员管理规定》第十九条第一款的规定',
            r'经查，你公司银保业务部客户经理吴锋（员工，驻点中国农业银行广州建设二马路支行）私自印制中国太平洋人寿保险股份有限公\n司“红福宝”产品相关误导性宣传单张，并供中国农业银行广州建设二马路支行内部培训时使用，违反了《保险公司管理规定》第\n六十三条第二款“保险机构不得利用广告宣传或者其他方式，对其保险条款内容、服务质量等做引人误解的宣传”的规定',
            r'上述向银行支付手续费以外的费用，导致财务业务数据不真实的行为违反了《\n中华人民共和国\n保险法》（2002年修正，下同）第一百二十二条的规定',
            r'经查，你私自印制中国太平洋人寿保险股份有限公司“红福宝”产品相关误导性宣传单张，并供中国农业银行广州建设二马路支行\n内部培训时使用，违反了《保险公司管理规定》第六十三条第二款“保险机构不得利用广告宣传或者其他方式，对其保险条款内容、\n服务质量等做引人误解的宣传”的规定',
            r'上述行为违反了《保险代理机构管理规定》第二十一条第一款的规定。\n2007年3月5日\n至2007年9月19日，你作为阳江市圣泰保险代理有限公司的法定代表人兼总经理对上述行为负有直接责任',
            r'上述事实行为违反了《中华人民共和国保险法》第一百二十二条的规定。\n你作为该中心支公司主要负责人，对\n2005年7月20日\n之后发生的违法行为负有直接责任',
            r'该非法经营保险代理业务的行为违反了《保险法》第一百三十二条“保险代理人、保险经纪人应当具备保险监督管理机构规定的资格条件，并取得保险监督管理机构颁发的保险代理业务许可证或者经纪业务许可证，向工商行政管理机关办理登记，领取营业执照，并缴存保证金或者投保职业责任保险”的规定',
            r'新捷胜汽修厂非法经营保险代理业务违反了《中华人民共和国保险法》（2002年修正，下同）第一百三十二条的规定，虽然新捷胜汽修厂变更了工商登记名称，但你中心仍应承担原合伙企业的法律责任',
            r'上述行为违反了《中华人民共和国保险法》（2002年修正，下同）第一百二十二条的规定，你作为该营业部\n分管银保事务的\n总经理助理，对上述行为负有直接责任',
            r'该非法经营保险代理业务的行为违反了《保险法》第一百三十二条“保险代理人、保险经纪人应当具备保险监督管理机构规定的资格条件，并取得保险监督管理机构颁发的保险代理业务许可证或者经纪业务许可证，向工商行政管理机关办理登记，领取营业执照，并缴存保证金或者投保职业责任保险”的规定',
            r'你分公司建立的业务档案记录不完整，存在未对投保人的姓名或者名称、保险费交付保险公司的时间、佣金收取时间、保险金或者保险赔款的代领时间和交付被保险人的时间等重要业务信息全部进行记录，违反了《保险经纪机构管理规定》第九十条：“保险经纪机构及其分支机构应当建立完整规范的业务档案，业务档案应当包括下列内容：（一）投保人的姓名或者名称… …”的规定',
            r'你中心支公司未经监管部门批准，分别于2007年12月至2008年6月和2008年4月至6月期间擅自在东莞市大岭山镇和塘厦镇设立两个营销服务网点，违反了《中华人民共和国保险法》第八十条第一款和《保险公司营销服务部管理办法》第二条第二款的规定',
            r'经查，你分公司在与深圳金伦科技有限公司进行的“KK健康无忧卡”保险卡销售业务中，在保险卡售出但还未激活（投保人、被保险人以及保险责任期间均未明确）时就在财务上确认保费收入，违反财务上“权责发生制”原则，导致财务数据不真实，违反了《中华人民共和国保险法》第一百二十二条的规定',
            r'你在未获得朱祖英等六位投保人同意的情况下代替上述六人签署降低意外险保额的《补充告知声明书》，将保额由100万/人降至50万/人，违反了《保险营销员管理规定》第三十六条第十四项“保险营销员从事保险营销活动，不得有下列行为：（十四）未经投保人或者被保险人同意，代替或者唆使他人代替投保人、被保险人签署保险单证及相关重要文件”的规定',
            r'经查，合众人寿保险股份有限公司东莞中心支公司未经监管部门批准，分别于2007年12月至2008年6月和2008年4月至6月期间擅自在东莞市大岭山镇和塘厦镇设立两个营销服务网点，违反了《中华人民共和国保险法》第八十条第一款和《保险公司营销服务部管理办法》第二条第二款的规定。你作为合众人寿保险股份有限公司广东分公司总经理助理，分管个人寿险营销工作，对上述行为负有直接责任'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                               '.(\n?依据|\n?根据|\n?鉴于|\n?上述事实|\n?依照)', re.MULTILINE)
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
            'announcementOrg': '广东银保监局',
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
            logger.info('广东保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('广东保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('广东保监局 数据解析 ' + ' -- 修改parsed完成')
