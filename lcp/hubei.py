import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def hubei_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '湖北保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('湖北保监局 ' + 'Url to parse: %s' % announcement_url)

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

        document_code_compiler = re.compile(r'((鄂保监罚|鄂银保监罚决字).\d{4}.\d+号)')
        if document_code_compiler.search(content_text):
            document_code = document_code_compiler.search(content_text).group(1).strip()
            if document_code + '\n行政处罚决定书' in content_text:
                litigant_compiler = re.compile(
                    document_code.replace(r'[', r'\[').replace(r']', r'\]') + r'\n行政处罚决定书\n([\s\S]*?)\n' +
                    r'(经查|经检查|依据.*?的有关规定|\d+年\d+月\d+日|根据我局2008年整顿|根据举报|.*?对.*?(进行了?|开展).*?((现场|投诉|举报|专项|临时)检查|检查清理))')
            else:
                litigant_compiler = re.compile(
                    document_code.replace(r'[', r'\[').replace(r']', r'\]') + r'\n([\s\S]*?)\n' +
                    r'(经查|经检查|依据.*?的有关规定|\d+年\d+月\d+日|根据我局2008年整顿|根据举报|.*?对.*?(进行了?|开展).*?((现场|投诉|举报|专项|临时)检查|检查清理))')
            litigant = litigant_compiler.search(content_text).group(1).strip()
        else:
            if document_code_compiler.search(announcement_title):
                document_code = document_code_compiler.search(title).group(1).strip()
                litigant_compiler = re.compile(
                    r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|2008年5月29日|2008年6月3日|根据我局2008年整顿|根据举报|'
                                       r'.*?对.*?(进行了?|开展).*?(现场|投诉|举报|专项|临时)检查)')
                litigant = litigant_compiler.search(content_text).group(1).strip()
            else:
                document_code_compiler = re.compile(r'(.\d{4}.\d+号)')
                if document_code_compiler.search(announcement_title):
                    document_code = '鄂银保监罚决字' + document_code_compiler.search(announcement_title).group(1).strip()
                    litigant_compiler = re.compile(
                        r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|2008年5月29日|2008年6月3日|根据我局2008年整顿|根据举报|'
                                           r'.*?对.*?(进行了?|开展).*?(现场|投诉|举报|专项|临时)检查)')
                    litigant = litigant_compiler.search(content_text).group(1).strip()
                else:
                    document_code_compiler = re.compile(r'((鄂保监罚|鄂银保监罚决字).\d{4}.\d+)')
                    if document_code_compiler.search(content_text):
                        document_code = document_code_compiler.search(content_text).group(1).strip()
                        litigant_compiler = re.compile(
                            document_code.replace(r'[', r'\[').replace(r']', r'\]') + r'\n([\s\S]*?)\n' +
                            r'(经查|经检查|依据.*?的有关规定|2008年5月29日|2008年6月3日|'
                            r'根据我局2008年整顿|根据举报|.*?对.*?(进行了?|开展).*?((现场|投诉|举报|专项|临时)检查|检查清理))')
                        litigant = litigant_compiler.search(content_text).group(1).strip()
                        document_code = document_code + '号'
                    else:
                        document_code = ''
                        litigant_compiler = re.compile(r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|'
                                                                          r'.*?对.*?(进行了?|开展).*?(现场|投诉|举报|专项|临时)检查)')
                        litigant = litigant_compiler.search(content_text).group(1).strip()

        litigant = litigant.replace('行政处罚决定书', '').strip()

        truth_text_str = r'((经查|现场检查，发现)' \
                         r'([\s\S]*?))' \
                         r'([^，。,；]*?(上述|以上)(违规|违法)?(事实|行为)[^。；\n]*?有.*?等证据(在案)?证明|' \
                         r'[^，。,；]*?(上述|以上)(违规|违法)?(事实|行为)[^。；\n]*?违反了.*?第.*?条.*?((之|的|等)规定)?|' \
                         r'[^，。,；\n]*?违反.*?第.*?条.*?规定|' \
                         r'以上行为违反了《保险法》第一百二十二的规定。)'
        truth_compiler = re.compile(truth_text_str)
        if truth_compiler.search(content_text):
            truth = truth_compiler.search(content_text).group(1).strip()
        else:
            truth_text_str = litigant + r'([\s\S]*?)' \
                                        r'([^，。,；]*?(上述|以上)(违规|违法)?(事实|行为)[^。；\n]*?有.*?等证据(在案)?证明|' \
                                        r'[^，。,；]*?(上述|以上)(违规|违法)?(事实|行为)[^。；\n]*?违反了.*?第.*?条.*?((之|的|等)规定)?|' \
                                        r'[^，。,；\n]*?违反.*?第.*?条.*?规定|' \
                                        r'以上行为违反了《保险法》第一百二十二的规定。)'
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
                                                          r'对[^，。,；\n]*?申辩意见(不予|予以)采纳。|因此.*?申辩理由.*?成立。|' \
                                                          r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?))'
                defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                if defense_response_compiler.search(content_text):
                    defense_response = defense_response_compiler.search(content_text).group(1).strip()
                else:
                    if '未' in defense:
                        defense_response = ''
        else:
            defense = defense_response = ''

        punishment_decision_text_str = r'(((依据|根据).*?第?.*?条.*?(决定|责令|给予|于.*?向.*?发出|对|拟对你)|' \
                                       r'我局决定.*?作出(如下|以下)(行政)?处罚：|' \
                                       r'我局决定对你给予|' \
                                       r'我局对.*?陈述申辩意见不予采纳，决定作出如下处罚)' \
                                       r'([\s\S]*?))' \
                                       r'(.*?应(当)?在接到本处罚决定书之日|当事人如对本处罚决定不服|本处罚决定书自送达之日起执行|' \
                                       r'你公司如不服本处罚决定|请你.*?在接到本处罚决定书之日|如不服本处罚决定|请你公司在接到本处罚决定书之日|' \
                                       r'请.*?在接到本处罚决定书之日|你.*?必须立即采取措施对上述违法行为予以改正|你.*?在接到本处罚决定书15日内|' \
                                       r'如你对本处罚决定不服|.*?应在接到本处罚决定书)'
        punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
        punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

        punishment_basis_str_list = [
            r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'经查，你支公司2008年对承保的非车险业务违规批单退费，违反了《保险法》第一百二十二条之规定',
            r'上述行为违反了《保险法》一百三十一条之规定',
            r'经查，你分公司2008年将对银行工作人员的奖励费用在“手续费及佣金支出”科目列支，违反了《保险法》第一百二十二条之规定'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                               '.(\n?依据|\n?根据|\n?鉴于|\n?我局决定对你给予)', re.MULTILINE)
        punishment_basis_list = punishment_basis_compiler.findall(content_text)
        punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

        publish_date_text = re.search(
            punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
            replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace(r'\n', '')
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
            'announcementOrg': '湖北银保监局',
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
            logger.info('湖北保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('湖北保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('湖北保监局 数据解析 ' + ' -- 修改parsed完成')
