import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def tianjin_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '天津保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('天津保监局 ' + 'Url to parse: %s' % announcement_url)

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

        document_code = re.search(r'^([\s\S]*?)\n', content_text).group(1)
        if re.match(r'.*保监.*\d{4}.*\d+号', document_code):
            litigant_compiler = re.compile(
                document_code.replace(r'[', r'\[').replace(r']', r'\]')
                + r'\n'
                  r'([\s\S]*?)\n' + r'(依据.*(的)?有关规定|经查[，。]|\d{4}年(\d+月)?，你公司|检查发现你公司|你公司于|'
                                    r'\d{4}年\d+月，你作为.*?(工作人员|营销员|经理)|经对你公司.*?检查发现，|'
                                    r'\d{4}年\d+月，我局检查发现你单位|我局在对.*?现场检查中发现|检查发现，你公司|'
                                    r'\d{4}年\d+月至\d+月(期)?间，|\d{4}年\d+月至\d+月，经对你.*现场检查发现|'
                                    r'\d{4}年\d+月至\d{4}年\d+月(期)?间，|抽查发现你公司|'
                                    r'你任.*期间，经营存在下列违法行为：|你任.*期间，该公司存在下列违法行为：|'
                                    r'你任.*期间，存在.*?问题|'
                                    r'2010年 12月，生命人寿保险股份有限公司|2009年4月，在未取得我局批准的情况下，)')
            litigant = litigant_compiler.search(content_text).group(1).strip()
        else:
            document_code = re.search(r'.(.*?保监.*?\d{4}.*?\d+号).', title).group(1).strip()
            litigant = re.search(r'^([\s\S]*?)'
                                 + '(依据.*(的)?有关规定|经查，|检查发现你公司)', content_text).group(1).strip()

        truth_text_str = r'(经查，.*?存在以下违法行为：\n|经查，|经查，.*存在下列违法违规行为：\n|' \
                         r'经抽查发现，|经查。你公司存在下列违法行为：|' \
                         r'经查，.*存在下列违法行为：\n?)' \
                         r'([\s\S]*?)' \
                         r'(上述(违法)?事实，有.*?等证据(在案)?证明(在案)?(，|,)足以认定。|上述事实，有.*等证据证明。|上述违法事实有.*?等证据在案证明，足以认定|' \
                         r'该违法事实，有.*?等证据在案证明，足以认定|' \
                         r'该公司上述行为违反了《保险法》第一百一十六条：“保险公司及其工作人员在保险业务活动中不得有下列行为：|' \
                         r'综上，决定给予你公司.*?的行政处罚|' \
                         r'(上述|以上)(违法)?(事实(行为)?|行为)(，)?违反了《中华人民共和国保险法》（2002年修正）第.*?条(的)?规定|' \
                         r'(上述|以上)(违法)?(事实(行为)?|行为)(，)?违反了《中华人民共和国保险法》第.*?条(的)?规定|' \
                         r'上述行为违反了《保险营销员管理规定》第.*?条|' \
                         r'上述行为违反了中国保监会《人身保险新型产品信息披露管理办法》第八条的规定|' \
                         r'上述事实行为违反了《保险代理机构管理规定》（保监会令\[2004\]14号）第二条的规定|' \
                         r'上述行为违反了中国保监会《保险营销员管理规定》第三十六条|' \
                         r'上述(事实(行为)?|行为)违反了《保险兼业代理管理暂行办法》第.*?条(的)?规定|' \
                         r'上述(事实)?行为(分别)?违反了《保险代理机构管理规定》第.*?条(的)?(规定)?|' \
                         r'上述(事实)?行为(分别)?违反了《保险经纪机构管理规定》第.*?条(的)?规定|' \
                         r'上述(事实(行为)?|行为)违反了(中国保监会)?《保险营销员管理规定》第.*?条(的)?规定|' \
                         r'依据《保险营销员管理规定》第.*条规定|' \
                         r'上述事实行为违反了《机动车交通事故责任强制保险条例》第.*条规定)'
        truth_compiler = re.compile(truth_text_str)
        if truth_compiler.search(content_text):
            truth = truth_compiler.search(content_text).group(2).strip()
        else:
            truth_text_str = r'(' + litigant + r'\n)' \
                                               r'([\s\S]*?)' \
                                               r'(上述(违法)?事实，有.*?等证据(在案)?证明(在案)?(，|,)足以认定。|上述事实，有.*等证据证明|' \
                                               r'上述违法事实有.*?等证据在案证明，足以认定。|该违法事实，有.*?等证据在案证明，足以认定|' \
                                               r'该公司上述行为违反了《保险法》第一百一十六条：“保险公司及其工作人员在保险业务活动中不得有下列行为：|' \
                                               r'综上，决定给予你公司.*?的行政处罚|' \
                                               r'(上述|以上)(违法)?(事实(行为)?|行为)(，)?违反了《中华人民共和国保险法》（2002年修正）第.*?条(的)?规定|' \
                                               r'(上述|以上)(违法)?(事实(行为)?|行为)(，)?违反了《中华人民共和国保险法》第.*?条(的)?规定|' \
                                               r'上述行为违反了《保险营销员管理规定》第.*?条|' \
                                               r'上述行为违反了中国保监会《人身保险新型产品信息披露管理办法》第八条的规定|' \
                                               r'上述事实行为违反了《保险代理机构管理规定》（保监会令\[2004\]14号）第二条的规定|' \
                                               r'上述行为违反了中国保监会《保险营销员管理规定》第三十六条|' \
                                               r'上述(事实(行为)?|行为)行为违反了《保险兼业代理管理暂行办法》第.*?条(的)?规定|' \
                                               r'上述(事实)?行为(分别)?违反了《保险代理机构管理规定》第.*?条(的)?(规定)?|' \
                                               r'上述(事实)?行为(分别)?违反了《保险经纪机构管理规定》第.*?条(的)?规定|' \
                                               r'上述(事实(行为)?|行为)违反了(中国保监会)?《保险营销员管理规定》第.*?条(的)?规定|' \
                                               r'依据《保险营销员管理规定》第.*条规定|' \
                                               r'上述事实行为违反了《机动车交通事故责任强制保险条例》第.*条规定)'
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
                               r'我局对陈述申辩意见进行了复核。)'
            defense_compiler = re.compile(defense_text_str, re.MULTILINE)
            defense_list = defense_compiler.findall(content_text)
            if len(defense_list) != 0:
                defense = defense_list[-1][0]
                defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                       + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                          r'对[^，。,；\n]*?申辩意见(不予|予以)采纳。|因此.*?申辩理由.*?成立。|' \
                                                          r'我局(经复核)?认为.*?申辩(理由|意见|事由).*?符合.*?的条件.(予以采纳。)?))'
                defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                defense_response = defense_response_compiler.search(content_text).group(1).strip()
        else:
            defense = defense_response = ''

        punishment_decision_text_str = r'(((依据|根据|依照).*?第?.*?条.*?(规定)?.?(我局)?(决定|责令|给予|于.*?向.*?发出|' \
                                       r'对.*?作出|拟对你|对.*?做出|对.*?予以)|' \
                                       r'我局(决定)?.*?(作|做)出(如下|以下)(行政)?处罚：|' \
                                       r'我局认为：\n.*?行为|' \
                                       r'综上，我局决定|' \
                                       r'我局决定：\n.*?根据|' \
                                       r'上述事实行为违反了《保险代理机构管理规定》第二十一条、二十二条的规定，决定给予你公司责令改正、责令停止接受新业务的行政处罚。)' \
                                       r'([\s\S]*?))' \
                                       r'(当事人应当在接到本处罚决定书之日|当事人如对本处罚决定不服|本处罚决定书自送达之日起执行|' \
                                       r'你公司如不服本处罚决定|请你.*?在接到本处罚决定书之日|如不服本处罚决定|请你公司在接到本处罚决定书之日|' \
                                       r'请.*?在接到本处罚决定书之日|如对本处罚决定不服)'
        punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
        punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

        punishment_basis_str_list = [
            r'([^\n。；：]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)(违反|属).*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'该公司上述行为违反了《保险法》第一百一十六条：“保险公司及其工作人员在保险业务活动中不得有下列行为：（一）欺骗投保人、被保险人或者受益人……”',
            r'经查，你任国泰人寿保险有限责任公司天津分公司总经理期间.*?违反.*?条',
            r'2009年2月至6月通过费用报销套取现金.*?违反了.*?条规定',
            r'一、货物运输预约保险业务管理失控，业务、财务基础数据不真实，违反《中华人民共和国保险法》第八十六条；\n'
            r'二、虚假批单退费、虚列车辆使用费用，违反《中华人民共和国保险法》第八十六条；\n二、委托未取得合法代理资格的保险机构从事保险营销活动，违反《保险公司中介业务违法行为处罚办法》第十七条'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'[。\n；：]' + '(' + punishment_basis_str + ')' +
                                               r'.(\n?依据|\n?根据|\n?鉴于|\n.*?上述违法事实|\n二、|'
                                               r'本案现已审理终结。|\n?我局认为|\n?决定给予你公司责令改正|\n?依照)',
                                               re.MULTILINE)
        punishment_basis_list = punishment_basis_compiler.findall(content_text)
        punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

        publish_date_text = re.search(
            punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
            replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')
        if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
            publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1]
            m = re.match("([0-9零一二两三四五六七八九十〇○]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
            real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
        else:
            publish_date_text = table_content.find_all('tr')[1].text
            publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
            real_publish_date = publish_date.split('-')[0] + '年' + str(int(publish_date.split('-')[1])) + '月' + str(
                int(publish_date.split('-')[2])) + '日'

        result_map = {
            'announcementTitle': title,
            'announcementOrg': '天津银保监局',
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
            logger.info('天津保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('天津保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('天津保监局 数据解析 ' + ' -- 修改parsed完成')
