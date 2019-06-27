import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def yunnan_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '云南保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('云南保监局 ' + 'Url to parse: %s' % announcement_url)

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

        document_code_compiler = re.compile(r'(云银?保监罚.\d{4}.\d+号)')
        if document_code_compiler.search(content_text):
            document_code = document_code_compiler.search(content_text).group(1).strip()
        else:
            document_code_compiler = re.compile(r'(.\d{4}.\d+号)')
            if document_code_compiler.search(content_text):
                document_code = '云银保监罚' + document_code_compiler.search(content_text).group(1).strip()
            else:
                document_code = ''
        litigant_compiler = re.compile(r'^.*?\n([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|'
                                                               r'.*?存.*?(行为|问题)|'
                                                               r'.*?未经我局批准在瑞丽市养征稽查所旁房屋内.*?从事保险业务经营|'
                                                               r'.*?2008年9月，以向.*?，并以公司团队支付查勘费项目纳入赔案列支。)')
        litigant = litigant_compiler.search(content_text).group(1).strip()

        truth_text_str = r'((经查)' \
                         r'([\s\S]*?))' \
                         r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                         r'(我局认为，|综上，)?[^，。,；\n]*?(上述|以上|其).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                         r'我局于.*?向.*?发出“中国保监会云南监管局行政处罚事先告知书”|' \
                         r'当事人违反了《保险公司管理规定》第三十五条的规定|' \
                         r'上述“未建立业务档案”违反了《保险专业代理机构监管规定》第三十四条的规定|' \
                         r'上述当事人收到我局下发的|' \
                         r'委托不具有代理资格的人员开展业务的行为，违反了《中华人民共和国保险法》第一百一十六条第八项的规定|' \
                         r'上述行为违反了《保险法》第八十六条：|' \
                         r'上述行为违法了《保险公司中介业务违法行为处罚办法》第十八条的规定。|' \
                         r'依据《中华人民共和国保险法》（2002年修订）第一百五十条.*?的规定|' \
                         r'违反了《中华人民共和国保险法》第一百零六条“保险公司及其工作人员在保险业务活动中不得有下列行为|' \
                         r'，该行为违反了《中华人民共和国保险法》第一百二十二条“保险公司的营业报告|' \
                         r'该行为违反了.*?第.*?条.*?的规定。|' \
                         r'你.*?于.*?收到我局下发)'
        truth_compiler = re.compile(truth_text_str)
        if truth_compiler.search(content_text):
            truth = truth_compiler.search(content_text).group(1).strip()
        else:
            truth_text_str = litigant + r'([\s\S]*?)' \
                                        r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                        r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                                        r'我局于.*?向.*?发出“中国保监会云南监管局行政处罚事先告知书”|' \
                                        r'当事人违反了《保险公司管理规定》第三十五条的规定|' \
                                        r'上述“未建立业务档案”违反了《保险专业代理机构监管规定》第三十四条的规定|' \
                                        r'上述当事人收到我局下发的|' \
                                        r'委托不具有代理资格的人员开展业务的行为，违反了《中华人民共和国保险法》第一百一十六条第八项的规定|' \
                                        r'上述行为违反了《保险法》第八十六条：|' \
                                        r'针对上述违法行为)'
            truth_compiler = re.compile(truth_text_str)
            truth = truth_compiler.search(content_text).group(1).strip()

        if '申辩' in content_text:
            defense_text_str = r'((针对.*?行为.*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?陈述申辩(意见)?|' \
                               r'[^，。,；\n]*?向我局(报送|递交|提出|提交)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                               r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见)中称|[^，。,；\n]*?在听证阶段提出|' \
                               r'[^，。,；\n]*?在法定期限内(未)?提出(了)?(听证要求|陈述申辩|陈述申辩及听证要求|)|' \
                               r'你向我局提交申辩材料辨称|' \
                               r'我局于2011年12月26日向你发出“中国保监会云南监管局行政处罚事先告知书”（云保监稽［2011］41号），你于2012年3月26日提交了申辩书|' \
                               r'向我局提出了书面申辩意见|你公司在申辩材料中对上述违法事实发生的理由进行了陈述和申辩|' \
                               r'你公司于八月十八向我局提交了申辩书|你.*?针对该行政处罚事先告知书的陈诉申辩材料我局已收悉)' \
                               r'([\s\S]*?))' \
                               r'(因此，我局决定|' \
                               r'我局经?复核(认为|决定)|' \
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
                               r'你并未对违法事实及处罚依据提出异议|' \
                               r'但未提供相关证明材料|' \
                               r'但对认定的事实和适用的法律未提出异议|' \
                               r'我局认为，上述违法事实|' \
                               r'申辩书中未对我局进行行政处罚的事实及法律依据提出异议|' \
                               r'材料中对我局认定的违规事实及处罚法律依据无异议)'
            defense_compiler = re.compile(defense_text_str, re.MULTILINE)
            defense_list = defense_compiler.findall(content_text)
            if len(defense_list) != 0:
                defense = defense_list[-1][0]
                defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                       + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                          r'对[^，。,；\n]*?(申辩意见|陈述申辩)(不予|予以)采纳|因此.*?申辩理由.*?成立。|' \
                                                          r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                          r'因此我局认为申辩理由不能成立。|' \
                                                          r'因此我局决定.*?(维持|作出).*?行政处罚))'
                defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                if defense_response_compiler.search(content_text):
                    defense_response = defense_response_compiler.search(content_text).group(1).strip()
                else:
                    if '未' in defense:
                        defense_response = ''
            else:
                defense_text_str = '(你.*?于.*?收到我局下发的.*?在规定时限内未向我局进行陈述和申辩。)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense = defense_compiler.search(content_text).group(1).strip()
                defense_response = ''
        else:
            defense = defense_response = ''

        punishment_decision_text_str = r'(((依据|根据).*?第?.*?条.*?(规定)?.?(我局|云南保监局(局)?)(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你)|' \
                                       r'综上，我局责令|依据《中华人民共和国保险法》第一百五十条的规定，现对你作出|' \
                                       r'我局决定.*?作出(如下|以下)(行政)?处罚：' \
                                       r'根据《保险法》第一百七十三条：“保险公司、保险资产管理公司、保险专业代理机构、保险经纪人违反本法规定的|' \
                                       r'依据《保险营销员管理规定》第五十七条“保险公司委托未取得《资格证书》和《展业证》的人员从事保险营销活动|' \
                                       r'依据《保险营销员管理规定》第五十七条“……对该保险公司直接负责的高级管理人员和其他责任人员，给予警告)' \
                                       r'([\s\S]*?))' \
                                       r'(云南保监局|请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                       r'请(在)?接到本处罚决定书之日|二○一一年二月十八日|如不服从本处罚决定|.*?年.*?月.*?日)'

        punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
        punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

        punishment_basis_str_list = [
            r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'当事人违反了《保险公司管理规定》第三十五条的规定',
            r'上述行为违法了《保险公司中介业务违法行为处罚办法》第十八条的规定',
            r'上述“未建立业务档案”违反了《保险专业代理机构监管规定》第三十四条的规定',
            r'违反了《中华人民共和国保险法》第一百零六条“保险公司及其工作人员在保险业务活动中不得有下列行为：… …（五）故意编造未曾发生的保险事故进行虚假理赔，骗取保险金。”的规定。\n上述违规事实发生于你任职该机构理赔客服部负责人期间，你对此负有直接责任',
            r'上述电话销售人员欺骗投保人的行为违反了《中华人民共和国保险法》第一百一十六条第（一）项的规定，卢春华\n+作为现任泰康人寿保险股份有限公司云南分公司电话行销部经理，对2012年的6笔电话销售业务存在误导性陈述问题，负有直接责任。',
            r'上述行为违反了《中华人民共和国保险法》第一百二十二条的规定。\n你作为公司经理，对未入账学平险保费负领导责任，对违规提取的佣金及使用负主要责任和领导责任。',
            r'上述行为违反了《中华人民共和国保险法》第一百二十二条“保险公司的营业报告、财务会计报告、精算报告及其他有关报表、文件和资料必须如实记录保险业务事项、不得有虚假记载、误导性陈述和重大遗漏。”的规定。\n+作为天安保险股份有限公司普洱中心支公司的主要负责人，你对上述行为负有直接责任',
            r'其行为违反了《中华人民共和国保险法》第一百零六条“保险公司及其工作人员在保险业务活动中不得有下列行为：… …（五）故意编造未曾发生的保险事故进行虚假理赔，骗取保险金。”及第一百二十四条“保险公司应当妥善保管有关业务经营活动的完整账簿、原始凭证及有关资料。前款规定的账簿、原始凭证及有关资料的保管期限，自保险合同终止之日起计算，不得少于十年。”的规定。\n+作为该机构的主要负责人，你对以上违规行为负有直接领导责任',
            r'上述行为违反了《保险法》（2002年修订）第一百二十二条：“保险公司的营业报告、财务会计报告、精算报告及其他有关报表、文件和资料必须如实记录保险业务事项，不得由虚假记载、误导性陈述和重大遗漏”的规定。\n+作为中国平安财产保险股份有限公司云南分公司财务部的时任主要负责人，你对上述行为负有直接责任'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'[。\n;]' + '(' + punishment_basis_str + ')' +
                                               r'.(\n?依据|\n?根据|\n?鉴于|'
                                               r'\n?作为中国平安财产保险股份有限公司云南分公司个人客户部的主要负责人|'
                                               r'\n?作为该机构主要负责人，你对以上违规行为负有直接领导责任)', re.MULTILINE)
        punishment_basis_list = punishment_basis_compiler.findall(content_text)
        punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

        publish_date_text = re.search(
            punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
            replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')
        if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
            publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
            m = re.match("([0-9零一二两三四五六七八九十〇○ＯOΟ]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
            real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
        else:
            publish_date_text = table_content.find_all('tr')[1].text
            publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
            real_publish_date = publish_date.split('-')[0] + '年' + str(int(publish_date.split('-')[1])) + '月' + str(
                int(publish_date.split('-')[2])) + '日'

        result_map = {
            'announcementTitle': title,
            'announcementOrg': '云南银保监局',
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
            logger.info('云南保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('云南保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('云南保监局 数据解析 ' + ' -- 修改parsed完成')
