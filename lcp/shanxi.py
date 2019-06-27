import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def shanxi_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '山西保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('山西保监局 ' + 'Url to parse: %s' % announcement_url)

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

        if re.search(r'((晋银?保监罚字|晋银保监筹罚字).\d{4}.\d+号)\n', content_text):
            text_document_code = re.search(r'((晋银?保监罚字|晋银保监筹罚字).\d{4}.\d+号)\n', content_text).group(1)
            litigant_compiler = re.compile(text_document_code.replace(r'[', r'\[').replace(r']', r'\]') +
                                           r'\n([\s\S]*?)\n'
                                           + r'(经查|'
                                             r'依据.*?有关规定|\d{4}年.*?[月日]，我(机关|局)对.*?进行(了)?(现场检查|信访检查|.*?专项检查)|'
                                             r'\d{4}年.*?[月日]，中发现保监会、财政部联合检查组对.*进行了现场检查|'
                                             r'我(机关|局)对.*?进行(了)?(现场检查|信访检查|.*?专项检查)|在你任职.*?期间|'
                                             r'2015年，三合保险代理公司向我局提交了虚假行政许可申请资料。|'
                                             r'我局(检查组)?于?.*?对.*?进行了现场检查[。，]|我局检查组于?.*?对.*?进行了调查[。，]|'
                                             r'山西鑫晟保险代理有限公司未按规定期限向我局申请延续《经营保险代理业务许可证》)')
            litigant = litigant_compiler.search(content_text).group(1).strip()
            document_code = text_document_code.strip()
        else:
            if re.search(r'((晋银?保监罚字|晋银保监筹罚字).\d{4}.\d+号)', title):
                document_code = re.search(r'((晋银?保监罚字|晋银保监筹罚字).\d{4}.\d+号)', title).group(1).strip()
            else:
                document_code = ''
            litigant_compiler = re.compile(r'^([\s\S]*?)\n'
                                           + r'(我局检查组于?.*?对.*?进行了现场检查。|我局检查组于?.*?对.*?进行了调查，|'
                                             r'山西鑫晟保险代理有限公司未按规定期限向我局申请延续《经营保险代理业务许可证》|'
                                             r'经查|山西中铁十二局保险代理公司的《经营保险代理业务许可证》于2016年2月8日到期|'
                                             r'我局在日常监管中|我(机关|局)对.*?进行(了)?(现场检查|信访检查|.*?专项检查)|'
                                             r'我局(检查组)?于?.*?对.*?(进行|开展|实施)了?.*?(专项检查|现场检查|暗访)(中)?(发现)?.*?[。，]|'
                                             r'我局在非现场检查中发现你公司存在以下违法违规行为：|根据举报|我局在对你公司的非现场监管中|经检查|'
                                             r'在你任职.*?期间|依据.*?有关规定|'
                                             r'你公司存在.*?的违规行为|你公司未经我局批准|在2008年清理整顿保险兼业代理市场工作中)')
            litigant = litigant_compiler.search(content_text).group(1).strip()

        truth_text_str = r'(经查，|经查,|经检查，|经查实，|检查发现，|现场检查，发现|抽查，|经抽查，|经查.*?存在以下问题：|经审核)' \
                         r'([\s\S]*?)' \
                         r'(上述(违法)?事实(，)?有[\s\S]*?等证据(在案)?(证明|佐证)(在案)?(，|,)足以认定。|' \
                         r'上述事实，有.*等证据证明。|上述违法事实有.*?等证据在案证明，足以认定|' \
                         r'该违法事实，有.*?等证据在案证明，足以认定|' \
                         r'\n.*?等证据材料可证明上述事实。|' \
                         r'该公司上述行为违反了《保险法》第.*?条|' \
                         r'综上，决定给予你公司.*?的行政处罚|' \
                         r'(上述|以上)(违法)?(事实(行为)?|行为)(分别)?违反了.*?第.*?条(的)?(规定)?|' \
                         r'依据《保险营销员管理规定》第.*条规定|' \
                         r'我局认为，.*?的规定|上述事实行为违反了\n《保险法》（\n2002\n年）第.*?条\n的规定|' \
                         r'上述事实行为违反了《中华人民共和国保险法》（\n2002\n年修正）第一百二十二条规定|' \
                         r'以上行为违反了《保险法》第.*?条.*?的规定|' \
                         r'你营销服务部的上述行为，造成了财务数据不真实|' \
                         r'根据《保险营销员管理规定》第五十六条“保险公司组织、参与、协助保险营销员考试作弊的|' \
                         r'违反了《保险公司管理规定》第.*?条|' \
                         r'违反了《保险营销员管理规定》第.*?条|' \
                         r'违反了《保险法》第.*?条|' \
                         r'上述事实行为违反了《保险公司管理规定》第.*?条|' \
                         r'依据《保险营销员管理规定》第.*?条|' \
                         r'依据《保险代理机构管理规定》第.*?条|' \
                         r'上述事实违反了《保险公司管理规定》（保监会令［2004］3号）第.*条|' \
                         r'阳光财险山西省分公司的上述事实有该公司以及相关人员出具的相关说明、现场检查事实确认书、相关记账凭证复印件等证据在案佐证，足以认定。|' \
                         r'你公司提供虚假材料的行为，违反了《保险专业代理机构监管规定》第二十四条)'
        truth_compiler = re.compile(truth_text_str)
        if truth_compiler.search(content_text):
            truth = truth_compiler.search(content_text).group(2).strip()
        else:
            truth_text_str = r'(经查，|经查,|经查实，)' \
                             r'([\s\S]*?)' \
                             r'[^，。,；\n]*?(上述(违法)?事实(，|,)?.*?(有)?.*?等证据(在案)?(证明|佐证)(在案)?(，|,)足以认定。|' \
                             r'上述事实，有.*等证据证明。|上述违法事实有.*?等证据在案证明，足以认定|' \
                             r'该违法事实，有.*?等证据在案证明，足以认定|' \
                             r'该公司上述行为违反了《保险法》第.*?条|' \
                             r'综上，决定给予你公司.*?的行政处罚|' \
                             r'(上述|以上)(违法)?(事实(行为)?|行为)(分别)?违反了.*?第.*?条(的)?(规定)?|' \
                             r'依据《保险营销员管理规定》第.*条规定|' \
                             r'我局认为，.*?的规定|上述事实行为违反了\n《保险法》（\n2002\n年）第.*?条\n的规定|' \
                             r'你时任泰康人寿朔州中支的总经理，并负责财务管理，对上述行为负有直接责任。|' \
                             r'你作为.*负有直接责任。|' \
                             r'应当对上述行为负责。|' \
                             r'保险代理机构任用高级管理人员，其任职资格应当报经中国保监会核准。”的规定。|' \
                             r'该行为违反了《保险代理\n?机构管理规定》第.*?条|' \
                             r'你公司.*?的行为，违反了《保险专业代理机构监管规定》第.*?条|' \
                             r'(以上行为)?造成了.*?，违反了《保险公司管理规定》第.*?条|' \
                             r'以上行为造成了.*?，违反了《保险代理机构管理规定》第.*?条|' \
                             r'(以上行为)?造成了.*?，违反了《保险代理机构管理规定》第.*?条|' \
                             r'违反了《.*?》第.*?条|' \
                             r'以上违规行为有相关资料佐证。)'
            truth_compiler = re.compile(truth_text_str)
            if truth_compiler.search(content_text):
                truth = truth_compiler.search(content_text).group(2).strip()
            else:
                truth_text_str = r'(' + litigant + r'\n)' \
                                                   r'([\s\S]*?)' \
                                                   r'[^，。,；\n]*?(上述(违法)?事实' \
                                                   r'(，|,)?.*?(有)?.*?\n?.*?等证据(在\n?案)?\n?(证明|佐证)(在案)?(，|,)足以认定。|' \
                                                   r'上述事实，有.*等证据证明。|上述违法事实有.*?等证据在案证明，足以认定|' \
                                                   r'该违法事实，有.*?等证据在案证明，足以认定|你作为.*负有直接责任。|' \
                                                   r'违反了《中华人民共和国保险法》第八十条|' \
                                                   r'违反了《关于调整保险业务监管费收费标准和收费办法的通知》|' \
                                                   r'违反了《中华人民共和国保险法》第.*?条|' \
                                                   r'你作为.*?负有直接责任)'
                truth_compiler = re.compile(truth_text_str)
                truth = truth_compiler.search(content_text).group(2).strip()

        if '申辩' in content_text:
            defense_text_str = r'((针对.*?行为.*?申辩意见|当事人[^，。,；\n]*?未提出陈述申辩意见|' \
                               r'[^，。,；\n]*?向我局(报送|递交)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                               r'[^，。,；\n]*?在(申辩材料|陈述申辩)中称：)' \
                               r'([\s\S]*?))' \
                               r'(因此，我局决定|' \
                               r'我局经复核认为|' \
                               r'本案现已审理终结|' \
                               r'我局经复查[^，。,；\n]*?情况|' \
                               r'我局[^，。,；\n]*?认真复核|' \
                               r'经研究，对[^，。,；\n]*?予以采纳。|' \
                               r'我局认为.*?申辩理由|' \
                               r'依据.*?我局认为.*?的申辩理由|' \
                               r'经研究，我局认为.*?申辩意见|' \
                               r'我局认为.*?的行为)'
            defense_compiler = re.compile(defense_text_str, re.MULTILINE)
            defense_list = defense_compiler.findall(content_text)
            if len(defense_list) != 0:
                defense = defense_list[-1][0]
                defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                       + r'((.*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                     r'对[^，。,；\n]*?申辩意见(不予|予以)采纳。|因此.*?申辩理由.*?成立。|' \
                                                     r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                     r'因此不予采纳申辩意见。))'
                defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                defense_response = defense_response_compiler.search(content_text).group(1).strip()
        else:
            defense = defense_response = ''

        punishment_decision_text_str = r'(((依据|根据).*?第.*?条.*?(规定)?.?(我局)?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你)|' \
                                       r'我局决定.*?作出(如下|以下)(行政)?处罚：|' \
                                       r'依据《保险法》第一百七十九条“违反法律、行政法规的规定，情节严重的，国务院保险监督管理机构可以禁止有关责任人员\n一定期限直至终身进入保险业。” 的规定|' \
                                       r'依据《保险专业代理机构监管规定》第七十六条[\s\S]*?的规定，我局决定|' \
                                       r'依据该规定，我局决定[\s\S]*?\n|' \
                                       r'依据.*?第.*?条[\s\S]*?的规定，我局决定[\s\S]*?\n|' \
                                       r'依据该规定，决定给予你公司作出警告，并罚款5000元的行政处罚。)' \
                                       r'([\s\S]*?))' \
                                       r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                       r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|当事人如对本处罚决定不服|' \
                                       r'请及时联系我局办公室确认缴纳罚款事宜)'
        punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
        punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

        punishment_basis_str_list = [
            r'([^\n。；]*?)(问题|行为|事项|情况|事实|数据不真实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'经查实[\s\S]*?违反[\s\S]*?规定(.*?负有直接责任)?',
            r'你公司拒不缴存保证金，且情节严重，违反了《中华人民共和国保险法》第一百三十二条的规定',
            r'在2008年清理整顿保险兼业代理市场工作中，我局于2007年12月27日和2008年5月23日两次在《山西日报》发布要求缴清监管费的公告后，'
            r'下列机构拒不缴纳监管费，违反了《关于调整保险业务监管费收费标准和收费办法的通知》（保监发[2006]13号）“对从事保险兼业代理的机构，'
            r'按每年每家机构500元定额收取保险业务监管费。”'
            r'和《中华人民共和国保险法》第一百零九条“保险监督管理机构有权检查保险公司的业务状况、财务状况及资金运用状况，'
            r'有权要求保险公司在规定的期限内提供有关的书面报告和资料。保险公司依法接受监督检查。”的规定'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                               r'.(\n*?依据|\n*?根据|\n*?鉴于|\n*?上述事实|\n?按照)', re.MULTILINE)
        punishment_basis_list = punishment_basis_compiler.findall(content_text)
        punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

        publish_date_text = re.search(
            punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
            replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace(r'\n', '')
        if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
            publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1]
            m = re.match("([0-9零一二两三四五六七八九十〇○Ｏ]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
            real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
        else:
            publish_date_text = table_content.find_all('tr')[1].text.strip()
            publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
            real_publish_date = publish_date.split('-')[0] + '年' + str(int(publish_date.split('-')[1])) + '月' + str(
                int(publish_date.split('-')[2])) + '日'

        result_map = {
            'announcementTitle': title,
            'announcementOrg': '山西银保监局',
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
            logger.info('山西保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('山西保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('山西保监局 数据解析 ' + ' -- 修改parsed完成')
