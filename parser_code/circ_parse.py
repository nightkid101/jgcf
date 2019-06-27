import re

from pymongo import MongoClient
from init import logger_init, config_init
from bs4 import BeautifulSoup as bs
from utility import get_year, cn2dig, request_site_page, get_content_text
from oss_utils import oss_add_file, init_ali_oss

from lcp import tianjin, hebei, shanxi, heilongjiang, shanghai, jiangsu, zhejiang, henan, shandong, chongqing, hubei, \
    hunan, sichuan, yunnan, qinghai, ningxia, xinjiang, xiamen, suzhou, shantou, neimenggu, jilin, anhui, fujian, \
    jiangxi, guangxi, hainan, shaanxi, tangshan, wenzhou, qingdao, ningbo, dalian, gansu, yantai, guizhou, shenzhen, \
    guangdong, beijing, xizang, liaoning

ali_bucket = init_ali_oss()
logger = logger_init('保监机构 数据解析')
config = config_init()
if config['mongodb']['dev_mongo'] == '1':
    db = MongoClient(config['mongodb']['ali_mongodb_url'], username=config['mongodb']['ali_mongodb_username'],
                     password=config['mongodb']['ali_mongodb_password'],
                     port=int(config['mongodb']['ali_mongodb_port']))[config['mongodb']['ali_mongodb_name']]
else:
    db = MongoClient(
        host=config['mongodb']['mongodb_host'],
        port=int(config['mongodb']['mongodb_port']),
        username=None if config['mongodb']['mongodb_username'] == '' else config['mongodb']['mongodb_username'],
        password=None if config['mongodb']['mongodb_password'] == '' else config['mongodb']['mongodb_password'])[
        config['mongodb']['mongodb_db_name']]

city_map = {
    'tianjin': '天津监管局',
    'hebei': '河北监管局',
    'shanxi': '山西监管局',
    'heilongjiang': '黑龙江监管局',
    'shanghai': '上海监管局',
    'jiangsu': '江苏监管局',
    'zhejiang': '浙江监管局',
    'shandong': '山东监管局',
    'henan': '河南监管局',
    'hubei': '湖北监管局',
    'hunan': '湖南监管局',
    'chongqing': '重庆监管局',
    'yunnan': '云南监管局',
    'qinghai': '青海监管局',
    'ningxia': '宁夏监管局',
    'xinjiang': '新疆监管局',
    'xiamen': '厦门监管局',
    'suzhou': '苏州监管分局',
    'shantou': '汕头监管分局',
    'neimenggu': '内蒙古监管局',
    'jilin': '吉林监管局',
    'anhui': '安徽监管局',
    'fujian': '福建监管局',
    'guangxi': '广西监管局',
    'hainan': '海南监管局',
    'shaanxi': '陕西监管局',
    'tangshan': '唐山监管分局',
    'wenzhou': '温州监管分局',
    'qingdao': '青岛监管局',
    'ningbo': '宁波监管局',
    'dalian': '大连监管局',
    'gansu': '甘肃监管局',
    'yantai': '烟台监管分局',
    'guizhou': '贵州监管局',
    'guangdong': '广东监管局',
    'liaoning': '辽宁监管局',
    'jiangxi': '江西监管局',
    'beijing': '北京监管局',
    'xizang': '西藏监管局',  # 没数据
    'shenzhen': '深圳监管局',
    'sichuan': '四川监管局',  # 有表格 表格未解析

}


# 保监会 解析
def parse_circ():
    for each_circ_data in db.circ_data.find({'origin': '保监会', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if '派出机构行政处罚情况' in announcement_title or '行政处罚实施情况' in announcement_title \
                or '保监会一季度处罚234家机构122人' in announcement_title \
                or '派出机构实施行政处罚情况' in announcement_title:
            logger.warning(announcement_url + ' ' + announcement_title + ' 与监管处罚无关')
            db.circ_data.update_one({'_id': each_circ_data['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('保监会' + ' 无关数据' + ' -- 修改status完成')
            continue

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('Url to parse: ' + announcement_url)

        r = request_site_page(announcement_url)
        if r is None:
            logger.error('网页请求错误 %s' % announcement_url)
            return
        content_soup = bs(r.text.encode(r.encoding).decode('utf-8'), 'lxml') if r else bs('', 'lxml')

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

        if each_circ_data['type'] == '行政处罚决定':
            table_content = content_soup.find(id='tab_content')
            if not table_content:
                logger.error('网页请求错误 %s' % announcement_url)
                continue
            content_text = get_content_text(table_content.find_all('tr')[3])
            title = table_content.find_all('tr')[0].text.strip()

            if re.search(r'^(.*)\n+(当事人|当 事 人|.*?：)', content_text):
                document_code = re.search(r'^(.*)\n+(当事人|当 事 人|.*?：)', content_text).group(1).strip()
                litigant = re.search(
                    document_code +
                    r'\n([\s\S]*?)\n(.*依据|依据|根据|经查|\d{4}年\d{1,2}月\d{1,2}日\n?至\n?(\d{4}年)?\d{1,2}月\d{1,2}日|'
                    r'\d{4}年\d{1,2}月\d{1,2}日\n|\d{4}年\d{1,2}月\n?至\n?\d{1,2}月|'
                    r'近期，我会对.*上述当事人在规定期限内没有提出陈述申辩意见。本案现已审理终结。|'
                    r'\d{4}[\s\S]*?(，)?我会.*?进行了(专项)?现场检查|我会于\d{4}年.*?对.*?进行了现场检查|'
                    r'经查，(\d{4}年.*，)?.*存在(以下|下列)(违法)?行为：|'
                    r'嘉禾人寿在2008年第4季度和2009年第1季度偿付能力严重不足的情况下)',
                    content_text).group(1).strip()
            else:
                document_code = re.search(r'(银保监罚决字〔\d{4}〕\d+号)', title).group(1).strip()
                litigant = re.search(r'^([\s\S]*?)\n(.*依据|依据|根据|经查|近期，我会对.*上述当事人在规定期限内没有提出陈述申辩意见。本案现已审理终结。)',
                                     content_text).group(1).strip()
            truth_text_str = r'((经查，|经查,|经检查，|经查实，|检查发现，|现场检查，发现|抽查，|经抽查，|经查.*?存在以下问题：)' \
                             r'([\s\S]*?))' \
                             r'((我局认为，)?(上述|以上).*?(事实|行为|事实).*?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                             r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                             r'当事人在申辩材料中称|' \
                             r'(\n|。)(鉴于|根据|依据))'
            truth_compiler = re.compile(truth_text_str)
            if truth_compiler.search(content_text):
                truth = truth_compiler.search(content_text).group(1).strip()
            else:
                truth_text_str = litigant + r'([\s\S]*?)' \
                                            r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                            r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                                            r'(\n|。)依据|' \
                                            r'你在申辩材料中称)'
                truth_compiler = re.compile(truth_text_str)
                truth = truth_compiler.search(content_text).group(1).strip()

            if '申辩' in content_text:
                defense_text_str = r'((针对.*?行为.*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?陈述申辩(意见)?|' \
                                   r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                                   r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见)中称|[^，。,；\n]*?在听证阶段提出|' \
                                   r'[^，。,；\n]*?在法定期限内(未)?提出(了)?(听证要求|陈述申辩|陈述申辩及听证要求)|' \
                                   r'[^，。,；\n]*?向我会提出听证申请|' \
                                   r'针对上述违法行为，太平财险四川分公司和张玮提出了听证申请|' \
                                   r'针对上述行为，平安财险四川分公司提出了听证申请|' \
                                   r'[^，。,；\n]*?未提出听证申请和陈述申辩意见|' \
                                   r'[^，。,；\n]*?规定期限内没有提出书面陈述申辩意见|' \
                                   r'[^，。,；\n]*?申辩提出|' \
                                   r'[^，。,；\n]*?提出陈述申辩|' \
                                   r'[^，。,；\n]*?辩称|' \
                                   r'[^，。,；\n]*?申辩中称|' \
                                   r'中再集团提出2007年度非寿险业务准备金提取与管理行为已经超过两年处罚时效)' \
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
                                   r'针对蒋新伟、原宇玲提出的听证申辩意见，我会经复核认为|' \
                                   r'针对上述当事人的听证申辩意见，我会经复核认为|' \
                                   r'我会对陈述申辩意见进行了复核|' \
                                   r'我会依法公开举行了听证会|' \
                                   r'针对.*?申辩意见，我会经复核认为|' \
                                   r'我会经复核认为|' \
                                   r'针对.*?申辩意见，我会认为|' \
                                   r'我会对.*?申辩意见进行了认真复核|' \
                                   r'对于当事人的申辩|' \
                                   r'对此，我会认为|' \
                                   r'经研究，我会认为|' \
                                   r'经查，我会认为|' \
                                   r'我会认为|' \
                                   r'针对.*?申辩意见，我机关认为|' \
                                   r'我机关认为|' \
                                   r'经查|' \
                                   r'我会认为：解决员工福利的理由未能改变虚列费用的违法事实)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense_list = defense_compiler.findall(content_text)
                if len(defense_list) != 0:
                    defense = defense_list[-1][0].strip()
                    defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                           + r'(([\s\S]*?)' + \
                                           r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                           r'对[^，。,；\n]*?申辩意见(不予|予以)采纳|因此.*?申辩理由.*?成立。|' \
                                           r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                           r'意见不予采纳|我会已经予以考量。|因此对公司的违规行为负有一定直接责任|' \
                                           r'对公司的违法行为负有直接管理责任|' \
                                           r'应当依照《保险法》的相关规定予以处罚|' \
                                           r'申辩理由不成立。|' \
                                           r'理由不予采纳。))'
                    defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                    if defense_response_compiler.search(content_text):
                        defense_response = defense_response_compiler.search(content_text).group(1).strip()
                    else:
                        if '未' in defense:
                            defense_response = ''
                else:
                    defense_text_str = '([^。；\n]*?向.*?公告送达了《行政处罚事先告知书》.*?提出陈述申辩。|' \
                                       '我局依法于2012年5月25日对你公司送达了《行政处罚事先告知书》，你公司在规定的时间内未提出陈述和申辩意见，也未要求举行听证。)'
                    defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                    defense = defense_compiler.search(content_text).group(1).strip()
                    defense_response = ''
            else:
                defense = defense_response = ''

            punishment_decision_text_str = r'(((依据|根据|依照).*?第.*?条.*?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你)|' \
                                           r'综上，我会(决定)?作出如下处罚|决定作出如下处罚|上述问题表明.*提出如下监管要求)' \
                                           r'([\s\S]*?))' \
                                           r'(请在本处罚决定书送达之日|[^，。,；\n]*?应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                           r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|[^，。,；\n]*?如对本处罚决定不服|' \
                                           r'\n\d{4}年.*月.*日$)'

            punishment_decision_compiler = re.compile(punishment_decision_text_str)
            punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

            punishment_basis_str_list = [
                r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            ]
            punishment_basis_str = '|'.join(punishment_basis_str_list)
            punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                   '.(\n?依据|\n?根据|\n?鉴于)', re.MULTILINE)
            punishment_basis_list = punishment_basis_compiler.findall(content_text)
            punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

            publish_date_text = re.search(
                punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
                replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace(r'\n', '')
            if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
                m = re.match("([0-9零一二两三四五六七八九十〇○ＯOΟО]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
                real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
            else:
                publish_date_text = table_content.find('tr')[1].text
                publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
                real_publish_date = publish_date.split('-')[0] + '年' + str(int(publish_date.split('-')[1])) + '月' + str(
                    int(publish_date.split('-')[2])) + '日'

            result_map = {
                'announcementTitle': title,
                'announcementOrg': '银保监会',
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
        else:
            table_content = content_soup.find(id='tab_content')
            content_text = get_content_text(table_content.find_all('tr')[3])
            title = table_content.find_all('tr')[0].text.strip()
            document_code = re.search(r'^([\s\S]*?)\n', content_text).group(1).strip()
            if re.search(document_code + '\n+(.*)[：:]', content_text):
                litigant = re.search(document_code + '\n+(.*)[：:]', content_text).group(1)
            else:
                document_code = re.search(r'(监管函〔\d{4}〕\d+号)', title).group(1)
                litigant = re.search(r'^([\s\S]*?)[：:]\n', content_text).group(1)

            if re.search('关于解除.*?监管措施的通知', announcement_title):
                announcement_type = '解除监管措施'
                truth = re.search('((根据|鉴于).*?)依据有关监管规定', content_text).group(1).strip()
                defense = defense_response = punishment_basis = ''
                publish_date = re.findall(r'(\d{4}.\d+.\d+.)', content_text)[-1]
                real_publish_date = format(publish_date)
                regulatory_requirements = re.search(r'(依据有关监管规定，我会决定[\s\S]*?)\n中国保监会', content_text).group(1).strip()
            else:
                announcement_type = '监管措施'
                truth_text_str = r'(你公司报送的2017年2季度偿付能力报告显示，|2017年2季度，你|抽查审核发现，你|现场评估，发现你|' \
                                 r'评估工作，发现你|现场评估，查实你|经查，你公司|工作部署，|专项检查，发现你|现场检查，发现你|经核查发现，你|' \
                                 r'近期，你公司|现场调查，发现你|经核查发现，|现就检查发现的主要问题（|经审核发现，|经查，在你|现场检查中，发现你|' \
                                 r'根据我会监管信息，截至2016年4月30日，你|鉴于你|信访投诉，反映你|但根据我会在现场检查中已查实的情况，你|你公司|' \
                                 r'经查,你公司存在以下问题|日前,我会核查发现|\n.*?开展了现场检查，)' \
                                 r'([\s\S]*?)' \
                                 r'((按照有关规定，|经研究.|依据有关规定，|依据相关规定，|根据有关规定，|依据.*的有关规定，|根据.*规定，|' \
                                 r'根据《保险公司偿付能力监管规则第10号：风险综合评级（分类监管）》第二十七条和第二十九条的相关规定，经中国保监会偿付能力监管委员会第三十九次工作会议研究决定，|' \
                                 r'依照《保险法》第八十六条、第一百六十九条，|），|根据查实情况，|' \
                                 r'为加强对保险产品的管理，保护投保人和被保险人的合法权益，依据.*规定，|' \
                                 r'为加强对保险产品的管理，保护投保人和被保险人的合法权益，)?' \
                                 r'(现)?(对你公司)?(提出|采取)(以|如)下监管(要求|措施)(：|:)|' \
                                 r'上述问题表明.*?提出如下监管要求)'
                truth_text_compiler = re.compile(truth_text_str)
                if truth_text_compiler.search(content_text):
                    truth = truth_text_compiler.search(content_text).group(2).strip()
                else:
                    truth = re.search(r'一、检查发现的问题([\s\S]*)二、监管要求', content_text).group(1).strip()

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
                        defense = defense_list[-1][0].strip()
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

                if re.search(r'(现)?(对你公司)?(提出|采取)[以如]下监管(要求|措施)[：:]([\s\S]*?)(中国保监会|.*年.*月.*日$)', content_text):
                    regulatory_requirements = re.search(
                        r'(现)?(对你公司)?(提出|采取)[以如]下监管(要求|措施)[：:]([\s\S]*?)(中国保监会|.*年.*月.*日$)',
                        content_text).group(5).strip()
                else:
                    if re.search(r'二、监管要求([\s\S]*?)中国保监会', content_text):
                        regulatory_requirements = re.search(r'二、监管要求([\s\S]*?)中国保监会', content_text).group(1).strip()
                    else:
                        regulatory_requirements = re.search(r'(你公司应将整改(方案及)?落实情况书面(报告|上报)我会。我会将视你公司整改情况,采取后续监管措施。)',
                                                            content_text).group(1).strip()

                punishment_basis_str_list = [
                    r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
                ]
                punishment_basis_str = '|'.join(punishment_basis_str_list)
                punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                       '.(\n?依据|\n?根据|\n?鉴于)', re.MULTILINE)
                punishment_basis_list = punishment_basis_compiler.findall(content_text)
                punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

                publish_date_text = re.search(
                    regulatory_requirements.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']',
                                                                                                                   r'\]').
                    replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')
                if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                    publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
                    m = re.match("([0-9零一二两三四五六七八九十〇○ＯOΟО]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?",
                                 publish_date)
                    real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(
                        cn2dig(m.group(3))) + '日'
                else:
                    publish_date_text = table_content.find('tr')[1].text
                    publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
                    real_publish_date = publish_date.split('-')[0] + '年' + str(int(publish_date.split('-')[1])) + '月' + \
                                        str(int(publish_date.split('-')[2])) + '日'

            result_map = {
                'announcementTitle': title,
                'announcementOrg': '银保监会',
                'announcementDate': real_publish_date,
                'announcementCode': document_code,
                'facts': truth,
                'defenseOpinion': defense,
                'defenseResponse': defense_response,
                'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                'punishmentBasement': punishment_basis,
                'punishmentDecision': regulatory_requirements,
                'type': announcement_type,
                'oss_file_id': file_id,
                'status': 'not checked'
            }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('保监会 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('保监会 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('保监会 数据解析 ' + ' -- 修改parsed完成')


# 地方保监局 解析
def local_parse_circ():
    tianjin.tianjin_circ(db, logger)
    hebei.hebei_circ(db, logger)
    shanxi.shanxi_circ(db, logger)
    heilongjiang.heilongjiang_circ(db, logger)
    shanghai.shanghai_circ(db, logger)
    jiangsu.jiangsu_circ(db, logger)
    zhejiang.zhejiang_circ(db, logger)
    henan.henan_circ(db, logger)
    shandong.shandong_circ(db, logger)
    chongqing.chongqing_circ(db, logger)
    hubei.hubei_circ(db, logger)
    hunan.hunan_circ(db, logger)
    sichuan.sichuan_circ(db, logger)
    yunnan.yunnan_circ(db, logger)
    qinghai.qinghai_circ(db, logger)
    ningxia.ningxia_circ(db, logger)
    xinjiang.xinjiang_circ(db, logger)
    xiamen.xiamen_circ(db, logger)
    suzhou.suzhou_circ(db, logger)
    shantou.shantou_circ(db, logger)
    neimenggu.neimenggu_circ(db, logger)
    jilin.jilin_circ(db, logger)
    anhui.anhui_circ(db, logger)
    fujian.fujian_circ(db, logger)
    jiangxi.jiangxi_circ(db, logger)
    guangxi.guangxi_circ(db, logger)
    hainan.hainan_circ(db, logger)
    shaanxi.shaanxi_circ(db, logger)
    tangshan.tangshan_circ(db, logger)
    wenzhou.wenzhou_circ(db, logger)
    qingdao.qingdao_circ(db, logger)
    ningbo.ningbo_circ(db, logger)
    dalian.dalian_circ(db, logger)
    gansu.gansu_circ(db, logger)
    yantai.yantai_circ(db, logger)
    guizhou.guizhou_circ(db, logger)
    shenzhen.shenzhen_circ(db, logger)
    guangdong.guangdong_circ(db, logger)
    beijing.beijing_circ(db, logger)
    xizang.xizang_circ(db, logger)
    liaoning.liaoning_circ(db, logger)


def parse():
    # 保监会
    parse_circ()

    # 地方监管局
    local_parse_circ()


if __name__ == "__main__":
    parse()
