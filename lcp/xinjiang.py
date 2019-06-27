import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def xinjiang_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '新疆保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('新疆保监局 ' + 'Url to parse: %s' % announcement_url)

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

        document_code_compiler = re.compile(r'((新保监[罚中]|新银保监罚决字).*?\d{4}.*?\d+.*?号)')
        if document_code_compiler.search(content_text):
            document_code = document_code_compiler.search(content_text).group(1).strip()
            litigant_compiler = re.compile(document_code.replace(r'[', r'\[').replace(r']', r'\]')
                                           + r'\n([\s\S]*?)\n' +
                                           r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|'
                                           r'一、你在担任.*?期间|'
                                           r'我局在.*?现场检查过程中，发现|'
                                           r'近期，我局对.*?进行了查处。|'
                                           r'新疆中亚保险代理有限公司和新疆德昌保险代理有限公司未按规定缴存营业保证金，违反了《保险代理机构管理规定》第二十一条和第二十二条之规定)')
            litigant = litigant_compiler.search(content_text).group(1).strip()
        else:
            if document_code_compiler.search(title):
                document_code = document_code_compiler.search(title).group(1).strip()
                litigant_compiler = re.compile(r'^([\s\S]*?)\n' +
                                               r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|'
                                               r'一、你在担任.*?期间|'
                                               r'我局在.*?现场检查过程中，发现|'
                                               r'近期，我局对.*?进行了查处。|'
                                               r'新疆中亚保险代理有限公司和新疆德昌保险代理有限公司未按规定缴存营业保证金，违反了《保险代理机构管理规定》第二十一条和第二十二条之规定)')
                litigant = litigant_compiler.search(content_text).group(1).strip()
            else:
                document_code = ''
                litigant_compiler = re.compile(r'关于对(.*?)(实施行政处罚|违规截留客户保费的行为实施行政处罚的通报)')
                litigant = litigant_compiler.search(title).group(1).strip()

        truth_text_str = r'((经查|一、(^二)|二、|三、|四、|五、)' \
                         r'([\s\S]*?))' \
                         r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                         r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                         r'刘孝虎上述行为违反了《保险营销员管理规定》第三十六条第十八款之规定|' \
                         r'依据 《保险代理机构管理规定》第一百三十八条之规定及你单位实际情况|' \
                         r'，违反了《保险代理机构管理规定》第二十一条和第二十二条之规定|' \
                         r'，这违反了《保险经纪机构管理规定》第六十九条之规定|' \
                         r'，违反了《保险代理机构管理规定》第六十三条之规定)'
        truth_compiler = re.compile(truth_text_str)
        truth_list = truth_compiler.findall(content_text)
        if len(truth_list) > 0:
            truth = '\n'.join([kk[0].strip() for kk in truth_list])
        else:
            truth_text_str = litigant + r'([\s\S]*?)' \
                                        r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                        r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?)'
            truth_compiler = re.compile(truth_text_str)
            if truth_compiler.search(content_text):
                truth = truth_compiler.search(content_text).group(1).strip()
            else:
                truth_text_str = r'(各保险公司、保险中介公司、保险行业协会：)' + r'([\s\S]*?)' \
                                                             r'(依据)'
                truth_compiler = re.compile(truth_text_str)
                truth = truth_compiler.search(content_text).group(2).strip()

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
                                       + r'((.*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                     r'对[^，。,；\n]*?申辩意见(不予|予以)采纳|因此.*?申辩理由.*?成立。|' \
                                                     r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?))'
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

        punishment_decision_text_str = r'(((依据|根据).*?第?.*?条.*?(规定)?.?我局(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你)|' \
                                       r'我局经复核认为|我局决定.*?作出(如下|以下)(行政)?处罚：|' \
                                       r'(依据|根据).*?第?.*?条.*?(规定)?.?(决定)?(给予|对你|对其))' \
                                       r'([\s\S]*?))' \
                                       r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                       r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|[^。；\n]*?在本处罚决定书送达之日起15日|' \
                                       r'[^。；\n]*?在接到本处罚决定书之日|二、|三、|各保险公司应引以为戒，切实加强保险营销员队伍管控|' \
                                       r'各保险中介公司要进一步增强依法合规经营意识|各公司接此通知后|各保险中介公司要吸取教训，引以为戒)'

        punishment_decision_compiler = re.compile(punishment_decision_text_str)
        punishment_decision_list = punishment_decision_compiler.findall(content_text)
        if len(punishment_decision_list) > 0:
            punishment_decision = '\n'.join([kk[0].strip() for kk in punishment_decision_list])

        punishment_basis_str_list = [
            r'([^\n。；\s]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'经查，你单位自2006年2月起未使用独立的代收保险费账户，违反了《保险代理机构管理规定》第九十一条之规定',
            r'新疆中亚保险代理有限公司和新疆德昌保险代理有限公司未按规定缴存营业保证金，违反了《保险代理机构管理规定》第二十一条和第二十二条之规定',
            r'乌鲁木齐吉尔盛保险代理有限公司未经批准，擅自任用高级管理人员，违反了《保险代理机构管理规定》第六十三条之规定'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                               '.(\n?依据|\n?(我局)?根据|\n?鉴于)', re.MULTILINE)
        punishment_basis_list = punishment_basis_compiler.findall(content_text)
        punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

        publish_date_text = re.search(punishment_decision_list[-1][0] + r'([\s\S]*?)$', content_text).group(
            1).replace('\n', '')
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
            'announcementOrg': '新疆银保监局',
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
            logger.info('新疆保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('新疆保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('新疆保监局 数据解析 ' + ' -- 修改parsed完成')
