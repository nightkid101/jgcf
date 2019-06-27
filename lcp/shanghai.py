import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def shanghai_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '上海保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('上海保监局 ' + 'Url to parse: %s' % announcement_url)

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

        document_code = re.search(r'(沪银?保监罚.\d{4}.\d+号).*?$', title).group(1).strip()

        if re.search(r'^(沪银?保监罚.*)\n', content_text):
            text_document_code = re.search(r'(沪银?保监罚.*)\n', content_text).group(1)
            litigant_compiler = re.compile(text_document_code.replace(r'[', r'\[').replace(r']', r'\]') +
                                           r'\n([\s\S]*?)\n'
                                           + r'(经查|依据.*?有关规定|\d{4}年.*?[月日]，我(机关|局)对.*?进行(了)?(现场检查|信访检查|.*?专项检查)|'
                                             r'\d{4}年.*?[月日]，中发现保监会、财政部联合检查组对.*进行了现场检查|'
                                             r'我(机关|局)对.*?进行(了)?(现场检查|信访检查|.*?专项检查)|在你任职.*?期间|'
                                             r'2015年，三合保险代理公司向我局提交了虚假行政许可申请资料。|'
                                             r'我局(检查组)?于?.*?对.*?进行了现场检查[。，]|我局检查组于?.*?对.*?进行了调查[。，]|'
                                             r'山西鑫晟保险代理有限公司未按规定期限向我局申请延续《经营保险代理业务许可证》)')
            litigant = litigant_compiler.search(content_text).group(1).strip()
        else:
            litigant_compiler = re.compile(r'^([\s\S]*?)\n'
                                           + r'(依据.*?的有关规定|.*?违法事实和证据|.*违规事实和证据|.*违法事实及证据|'
                                             r'2009年6月至11月间，你.*代理销售保险产品|'
                                             r'你公司于.*?在.*?保险(代理|经纪)人职业责任保险|'
                                             r'经查|你公司在.*中，存在.*?情况。|在你担任.*?期间|你公司在.*?业务过程中|'
                                             r'你公司.*?安排进出口货运险业务时|经检查并在现场检查取证记录.*?签章确认|'
                                             r'.*?你.*?担任.*?期间|你公司于.*?期间|'
                                             r'你公司.*?为|你公司.*?过程中)')
            litigant = litigant_compiler.search(content_text).group(1).strip()

        truth_text_str = r'(经查，|经查,|经查实，|检查发现，|违法事实和证据|违规事实和证据|违法事实及证据|' \
                         r'经检查并在现场检查取证记录04号中经你公司签章确认，)' \
                         r'([\s\S]*?)' \
                         r'((上述|以上)(违法)?(事实|行为).*?证据(材料)?(在案|予以)?(证明|佐证)?(在案)?((，|,)足以认定。)?|' \
                         r'上述.*?事实*等证据证明|上述违法事实有.*?等证据在案证明，足以认定|.*行政处罚的依据(和|及)决定|.*处罚依据及处罚决定|' \
                         r'依据.*?第.*?条规定，我局拟对你公司处1万元罚款的行政处罚。|' \
                         r'上述行为分别违反了《中华人民共和国保险法》一百二十二条和一百零六条的相关规定)'
        truth_compiler = re.compile(truth_text_str)
        if truth_compiler.search(content_text):
            truth = truth_compiler.search(content_text).group(2).strip()
        else:
            truth_compiler = re.compile(litigant + r'([\s\S]*?)' + r'(根据《中华人民共和国保险法》第一百六十条规定，决定给予你支行如下行政处罚：|'
                                                                   r'你.*?上述行为违反了.*?第.*?条的相关规定。|'
                                                                   r'依据.*?第.*?条的?规定，现给予你.*?的行政处罚。|'
                                                                   r'依据.*?第.*?条规定，我局拟给予你.*?的行政处罚。|'
                                                                   r'依据.*?第.*?条规定，现责令你公司改正，给予你.*?的行政处罚。|'
                                                                   r'上述事实有(以下|如下)证据证明)')
            truth = truth_compiler.search(content_text).group(1).strip()

        if '申辩' in content_text:
            defense_text_str = r'((针对.*?行为.*?申辩意见|(你[^，。,；\n]*?未[^，。,；\n]*?(申请)?听证(申请)?，)?[^，。,；\n]*?未?提出陈述申辩意见|' \
                               r'[^，。,；\n]*?向我局(报送|递交)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                               r'[^，。,；\n]*?在(申辩材料|陈述申辩)中称：|我局已于.*?向.*?送达了.*?在规定期限内未(再)?提出申辩意见。|' \
                               r'我局已于.*?向.*?送达了|我局已于.*?向.*?送达了.*?在规定期限内.*?未提出申辩意见。)' \
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
                               r'我局经审核后|' \
                               r'我局不予采纳|三、(行政)?处罚的履行方式和期限|经审核|' \
                               r'根据申辩意见和崇明县人民法院刑事判决书|经复核)'
            defense_compiler = re.compile(defense_text_str, re.MULTILINE)
            defense_list = defense_compiler.findall(content_text)
            if len(defense_list) != 0:
                defense = defense_list[-1][0].strip()
                defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') + \
                                       r'((.*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                   r'对[^，。,；\n]*?申辩意见(不予|予以)采纳。|因此.*?申辩理由.*?成立。|' \
                                                   r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                   r'因此.*?我局.*?不予采纳。|我局调整了原定的处罚决定。))'
                defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                if defense_response_compiler.search(content_text):
                    defense_response = defense_response_compiler.search(content_text).group(1).strip()
                else:
                    if '未' in defense:
                        defense_response = ''
        else:
            defense = defense_response = ''

        punishment_decision_text_str = r'((综上，我局(决定)?.*作出如下处罚：|我局决定.*作出如下处罚：|根据.*?第.*?条的?规定，决定|根据.*?第.*?条的?相关规定，决定|' \
                                       r'依据.*?第.*?条的?规定，现|依据.*?第.*?条的?规定，我局)' \
                                       r'([\s\S]*?))' \
                                       r'(当事人应当在接到本处罚决定书之日|当事人如对本处罚决定不服|本处罚决定书自送达之日起执行|' \
                                       r'你公司如不服本处罚决定|请你.*?在接到本处罚决定书之日|如不服本处罚决定|请你公司在接到本处罚决定书之日|' \
                                       r'请.*?在接到本处罚决定书之日|你.*?应当在接到本处罚决定书之日|\n.*?处罚的履行方式和期限|' \
                                       r'\n.*?应在收到本决定书之日|我局已于.*?送达了)'
        punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
        if punishment_decision_compiler.search(content_text):
            punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()
        else:
            punishment_decision_text_str = r'(行政处罚的依据和决定|行政处罚的依据及决定|处罚依据及处罚决定)' \
                                           r'([\s\S]*?)' \
                                           r'(当事人应当在接到本处罚决定书之日|当事人如对本处罚决定不服|本处罚决定书自送达之日起执行|' \
                                           r'你公司如不服本处罚决定|请你.*?在接到本处罚决定书之日|如不服本处罚决定|请你公司在接到本处罚决定书之日|' \
                                           r'请.*?在接到本处罚决定书之日|我局已于.*?送达了|\n.*?应在收到本决定书之日|' \
                                           r'三、不服本行政处罚决定、申请行政复议或者提起行政诉讼的途径和期限|三、行政处罚的履行方式和期限|' \
                                           r'三、处罚的履行方式和期限 )'
            punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
            punishment_decision = punishment_decision_compiler.search(content_text).group(2).strip()

        punishment_basis_str_list = [
            r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)负有直接责任',
            r'你公司聘任不具有任职资格的人员，违反了《中华人民共和国保险法》（2015年修正）第一百二十一条的规定',
            r'华康上分变更营业场所未按规定报告，违反了中国保监会《保险专业代理机构监管规定》（2015年修订）第十四条的规定',
            r'你公司未按规定妥善保管业务档案，违反了中国保监会《保险专业代理机构监管规定》（2015年修订）第五十七条的规定',
            r'你公司2008年1-9月为保险公司代开保险中介服务统一发票累计80.25万元，占总业务收入的14.3%，违反了《保险代理机构管理规定》第一百三十六条的规定',
            r'你公司2007年度从事保险代理业务过程中，未建立规范完整的业务档案并对保单、投保单等单证进行复印留存，违反了《保险代理机构管理规定》第九十六条的规定',
            r'你公司通过要求保险公司开具费率为0.3％的保单，实际按照0.1％与投保人结算保费的方式给予投保人保险合同以外的利益，违法了《保险代理机构管理规定》第一百零二条的规定',
            r'该公司通过要求保险公司开具费率为0.3％的保单，实际按照0.1％与投保人结算保费的方式给予投保人保险合同以外的利益，违反了《保险代理机构管理规定》第一百零二条的规定',
            r'你公司于2007年10月至2007年12月期间，私自印刷瑞福德健康保险股份有限公司10天期的公交意外险保单41500张，并分散到各网点销售，违反了《中华人民共和国保险法》第一百四十条的规定',
            r'你于2007年10月至2007年12月在华顺保险代理有限公司担任总经理期间，私自印刷瑞福德健康保险股份有限公司10天期的公交意外险保单41500张，'
            r'并分散到各网点销售，违反了《保险代理机构管理规定》第一百零一条的规定',
            r'海发代理违反了《保险法》第一百二十一条、《保险专业代理机构监管规定》第三十三条、《保险法》第一百三十一条的规定',
            r'2008年度你公司共以上述名义向投保人支付.*?违反了《保险代理机构管理规定》第一百零二条的规定',
            r'你公司在2008年度为平安财产保险股份有限公司上海分公司代理学平险业务过程中，私自印刷保险凭证，签发所谓保险协议，私自扩展保险责任期限，侵占、截留保险赔款，违反了《中华人民共和国保险法》第一百三十一条的规定',
            r'你公司2008年.*?违反了《保险经纪机构管理规定》第一百二十八条规定',
            r'上述行为分别违反了《中华人民共和国保险法》一百二十二条和一百零六条的相关规定',
            r'你公司违反了《保险法》第一百二十一条、《保险专业代理机构监管规定》第三十三条、《保险法》第一百三十一条的规定，'
            r'根据《保险法》第一百六十九条 、《保险专业代理机构监管规定》第八十八条、《保险法》第一百六十六条的规定'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile('[。\n；]' + '(' + punishment_basis_str + ')' +
                                               r'.(\n?依据|\n?根据|\n?鉴于|\n?应?按照|\n?决定吊销你公司的)', re.MULTILINE)
        punishment_basis_list = punishment_basis_compiler.findall(content_text)
        punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

        publish_date_text = re.search(
            punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
            replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')
        if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
            publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1]
            m = re.match("([0-9零一二两三四五六七八九十〇○Ｏ]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
            real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
        else:
            publish_date_text = table_content.find_all('tr')[1].text
            publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
            real_publish_date = publish_date.split('-')[0] + '年' + str(int(publish_date.split('-')[1])) + '月' + str(
                int(publish_date.split('-')[2])) + '日'

        result_map = {
            'announcementTitle': title,
            'announcementOrg': '上海银保监局',
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
            logger.info('上海保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('上海保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('上海保监局 数据解析 ' + ' -- 修改parsed完成')
