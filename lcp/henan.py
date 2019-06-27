import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def henan_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '河南保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('河南保监局 ' + 'Url to parse: %s' % announcement_url)

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
        title = table_content.find_all('tr')[0].text.strip()

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

        document_code_compiler = re.compile(r'(([豫宛]保监罚|豫银保监保罚决字).\n?\d{4}\n?.\n?.?\d+\n?.?号)')
        if document_code_compiler.search(content_text):
            document_code = document_code_compiler.search(content_text).group(1).strip()
            litigant_compiler = re.compile(
                document_code.replace(r'[', r'\[').replace(r']', r'\]') + r'\n([\s\S]*?)\n' +
                r'(经查|经检查|依据.*?的有关规定|'
                r'你于[\s\S]*?担任.*?期间|'
                r'你公司[\s\S]*?期间)')
            litigant = litigant_compiler.search(content_text).group(1).strip()
        else:
            if document_code_compiler.search(title):
                document_code = document_code_compiler.search(title).group(1).strip()
            else:
                document_code = ''
            litigant_compiler = re.compile(r'^([\s\S]*?)(经查|经检查|依据.*?的有关规定)')
            litigant = litigant_compiler.search(content_text).group(1).strip()
        truth_text_str = r'((经查|经检查，|经查实，|检查发现，|现场检查，发现|经查明，|经查明：|负有直接责任：|存在以下违规事实：|' \
                         r'你公司于2007年2月4日至2007年2月6日期间|你公司于2006年11月期间|' \
                         r'\n.*?存在.*?的违法行为)' \
                         r'([\s\S]*?))' \
                         r'([^，。,；\n]*?(上述|以上)(违法)?(事实|行为)(,|，)?(有|由)?[^。；\n]*?等证据(在案)?(证明|予以证实)(,|，|。)(足以认定。)?|' \
                         r'上述违法事实，有\n.*?\n等证据在案证明，足以认定。|' \
                         r'上述(事实)?行为违反了\n?.*?\n?第.*?条\n?(的|之)规定|' \
                         r'上述行为违反了\n《中华人民共和国保险法》第一百三十四条\n和\n《保险营销员管理规定》第四十三条\n的规定|' \
                         r'依据《保险法》第一百四十五条、第一百五十条的规定|违反了《保险法》第一百零七条的规定。)'
        truth_compiler = re.compile(truth_text_str)
        truth = truth_compiler.search(content_text).group(1).strip()
        if '申辩' in content_text:
            defense_text_str = r'((针对.*?行为.*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?陈述申辩(意见)?|' \
                               r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                               r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见)中称|[^，。,；\n]*?在听证阶段提出|' \
                               r'[^，。,；\n]*?在法定期限内(未)?提出(了)?(听证要求|陈述申辩|陈述申辩及听证要求|听证申请及陈述申辩意见)|' \
                               r'根据《中华人民共和国行政处罚法》第三十一条和三十二条规定.*?向我局提交书面的陈述书和申辩书。)' \
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
                               r'经我局向南阳理工学院成人教育学院调查|' \
                               r'我局认为你公司的陈述申辩未对违法事实、处罚理由及依据提出异议|' \
                               r'我局认为你作为人保财险新乡市分公司理赔中心主任|' \
                               r'我局认为你负责人保财险新乡分公司全系统的理赔管理工作|' \
                               r'逾期视为放弃陈述和申辩。|' \
                               r'依据现场检查及听证情况|' \
                               r'我局对陈述申辩意见进行复核认为)'
            defense_compiler = re.compile(defense_text_str, re.MULTILINE)
            defense_list = defense_compiler.findall(content_text)
            if len(defense_list) != 0:
                defense = defense_list[-1][0].strip()
                defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                       + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                         r'对[^，。,；\n]*?申辩(意见|理由)(不予|予以)采纳|' \
                                                         r'因此.*?(申辩理由|陈述申辩|申辩).*?成立|' \
                                                         r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                         r'逾期视为放弃陈述和申辩。))'
                defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                if defense_response_compiler.search(content_text):
                    defense_response = defense_response_compiler.search(content_text).group(1).strip()
                else:
                    if '未' in defense:
                        defense_response = ''
            else:
                defense_text_str = r'(\n你[^，。,；\n]*?在规定期限内未提出陈述申辩(和听证要求|意见)?。\n)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense = defense_compiler.search(content_text).group(1).strip()
                defense_response = ''
        else:
            defense = defense_response = ''
        punishment_decision_text_str = r'(((依据|根据)[^。；]*?第[^。；]*?条[^；。]*?(决定|责令|给予|于.*?向.*?发出|拟对你|对|规定应予)|' \
                                       r'我局决定.*?作出(如下|以下)(行政)?处罚：|' \
                                       r'依据《保险法》第一百六十五的规定，我局决定)' \
                                       r'([\s\S]*?))' \
                                       r'(当事人应当在接到本处罚决定书之日|当事人如对本处罚决定不服|本处罚决定书自送达之日起执行|' \
                                       r'[^，。,；\n]*?如不服本处罚决定|请你.*?在接到本处罚决定书之日|如不服本处罚决定|请你公司在接到本处罚决定书之日|' \
                                       r'请.*?在接到本处罚决定(书)?之日|请在接到本处罚决定书之日|如你公司对我局认定的违规事实、处罚理由及依据有异议|' \
                                       r'[^，。,；\n]*?应在接到本处罚决定之日)'
        punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
        punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()
        punishment_basis_str_list = [
            r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'美诚保代任用不具备高级管理人员资格的人员履行高级管理人员职责，违反了《保险法》第一百二十一条的规定',
            r'永安产险河南分公司任用不具备高级管理人员资格的人员履行高级管理人员职责，违反了《保险法》第八十一条的规定',
            r'.*?认为，\n?.*?\n?行为\n?违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'我局认为，\n你公司上述违法违规\n行为违反了\n《保险法》第\n一百一十六条和第八十六条\n的规定',
            r'我局认为，你营销服务部\n上述违法违规行为\n违反了\n《保险法》第八十六条\n的规定',
            r'我局认为，你公司\n上述违法违规行为\n违反了\n《人身保险新型产品信息披露管理办法》第十条\n的规定',
            r'上述行为违反了\n《保险法》（2002年修订）\n第一百二十二条、一百零六条、一百三十四条\n的规定',
            r'上述行为违反了\n《中华人民共和国保险法》第一百三十四条\n和\n《保险营销员管理规定》第四十三条\n的规定',
            r'我机关认为，你公司\n不如实记录保险业务事项的行为\n违反了\n《保险法》（2002年修正）第一百二十二条、《保险法》第八十六条\n的规定；'
            r'\n委托未取得合法资格的人员从事保险销售活动的 行为\n违反了\n《保险法》第一百一十六条、第一百三十条\n的规定；\n妨碍依法监督检查的行为\n违反了\n《保险法》第一百五十六条\n的规定'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                               '.(\n?依据|\n?根据|\n?鉴于)', re.MULTILINE)
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
            'announcementOrg': '河南银保监局',
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
            logger.info('河南保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('河南保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('河南保监局 数据解析 ' + ' -- 修改parsed完成')
