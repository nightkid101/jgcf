import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def beijing_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '北京保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('北京保监局 ' + 'Url to parse: %s' % announcement_url)

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

        document_code_compiler = re.compile(r'(京银?保监罚(字)?.\d{4}.\d+号)')
        if '行政处罚信息' in title or '行政处罚披露' in title:
            litigant = re.search(r'行政处罚(信息披露|披露|信息)(.*?)$', title).group(2).strip()
            if document_code_compiler.search(content_text):
                document_code = document_code_compiler.search(content_text).group(1).strip()
                publish_date_text = re.search(r'^.*?于(.*?)作出.*?处罚决定', content_text).group(1).strip()
                if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                    publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
                    m = re.match("([0-9零一二两三四五六七八九十〇○ＯOΟО]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?",
                                 publish_date)
                    real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(
                        cn2dig(m.group(3))) + '日'
                else:
                    publish_date_text = table_content.find_all('tr')[1].text
                    publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
                    real_publish_date = publish_date.split('-')[0] + '年' + str(
                        int(publish_date.split('-')[1])) + '月' + str(
                        int(publish_date.split('-')[2])) + '日'
            else:
                document_code = ''
                publish_date_text = table_content.find_all('tr')[1].text
                publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
                real_publish_date = publish_date.split('-')[0] + '年' + str(
                    int(publish_date.split('-')[1])) + '月' + str(
                    int(publish_date.split('-')[2])) + '日'
        else:
            if document_code_compiler.search(content_text):
                document_code = document_code_compiler.search(content_text).group(1).strip()
                litigant_compiler = re.compile(document_code.replace(r'[', r'\[').replace(r']', r'\]') +
                                               r'\n([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|'
                                                                   r'.*?现场检查|我局查实|.*?(审核|调查过程中).*?发现|'
                                                                   r'近期.*?存在违规行为|'
                                                                   r'.*?进行了.*?专项检查|'
                                                                   r'.*?我局依法.*?进行(现场)?(调查|检查)|'
                                                                   r'.*?有效期至|'
                                                                   r'.*?我局(收到|提交).*?申请材料|'
                                                                   r'.*?存在.*?问题|'
                                                                   r'你们于\n2008年3月31日\n向我局提交.*?申请材料|'
                                                                   r'我局于|'
                                                                   r'.{4}年.*?月|'
                                                                   r'2007\n?年1月1\n?日|'
                                                                   r'我局对你|'
                                                                   r'我局依法对)')
                litigant = litigant_compiler.search(content_text).group(1).strip()
            else:
                if document_code_compiler.search(title):
                    document_code = document_code_compiler.search(title).group(1).strip()
                else:
                    document_code = ''
                litigant_compiler = re.compile(r'^([\s\S]*?)\n' + r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|'
                                                                  r'.*?现场检查|我局查实|.*?(审核|调查过程中).*?发现|'
                                                                  r'近期.*?存在违规行为|'
                                                                  r'.*?进行了.*?专项检查|'
                                                                  r'.*?我局依法.*?进行(现场)?(调查|检查)|'
                                                                  r'.*?有效期至|'
                                                                  r'.*?我局(收到|提交).*?申请材料|'
                                                                  r'.*?存在.*?问题|'
                                                                  r'你们于\n2008年3月31日\n向我局提交.*?申请材料|'
                                                                  r'我局于|'
                                                                  r'.{4}年.*?月|'
                                                                  r'2007\n?年1月1\n?日|'
                                                                  r'我局对你|'
                                                                  r'我局依法对)')
                litigant = litigant_compiler.search(content_text).group(1).strip()

            punishment_decision_text_str = r'(((依据|根据)[^。；\n]*?第[^。；\n]*?条[^。；\n]*?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你|对你处以)|' \
                                           r'我局经复核认为|我局决定.*?作出(如下|以下)(行政)?处罚：|' \
                                           r'依据\n《保险公司管理规定》\n第九十九条之规定，决定给予|据此，我局作出如下行政处罚|' \
                                           r'依据《保险经纪机构管理规定》（保监会令\[2004\]15号）133条的规定，我局决定|' \
                                           r'依据《保险法》143条规定，我局决定)' \
                                           r'([\s\S]*?))' \
                                           r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如.*?不服本处罚决定|' \
                                           r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|$|' \
                                           r'我局.*?送达|.*?应在收到本处罚决定书之日)'

            punishment_decision_compiler = re.compile(punishment_decision_text_str)
            punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

            publish_date_text = re.search(
                punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
                replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')
            if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
                m = re.match("([0-9零一二两三四五六七八九十〇○ＯOΟО]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?",
                             publish_date)
                real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(
                    cn2dig(m.group(3))) + '日'
            else:
                publish_date_text = table_content.find_all('tr')[1].text
                publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
                real_publish_date = publish_date.split('-')[0] + '年' + str(
                    int(publish_date.split('-')[1])) + '月' + str(
                    int(publish_date.split('-')[2])) + '日'

        truth_text_str = r'((经查)' \
                         r'([\s\S]*?))' \
                         r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                         r'(,|，)?(依据|根据).*?第.*?条|' \
                         r'我局[\s\S]*?送达|' \
                         r'以上问题有谈话笔录等材料为证。)'
        truth_compiler = re.compile(truth_text_str)
        if truth_compiler.search(content_text):
            truth = truth_compiler.search(content_text).group(1).strip()
        else:
            truth_text_str = litigant + r'([\s\S]*?)' \
                                        r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                        r'(我局认为，|综上，)?[^，。,；\n]*?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                                        r'(,|，|。|\n)(依据|根据).*?第.*?条|' \
                                        r'以上问题有谈话笔录等材料为证。)'
            truth_compiler = re.compile(truth_text_str)
            if truth_compiler.search(content_text):
                truth = truth_compiler.search(content_text).group(1).strip()
            else:
                truth = ''

        if '申辩' in content_text:
            defense_text_str = r'((针对.*?行为.*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?陈述申辩(意见)?|' \
                               r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                               r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见)中称|[^，。,；\n]*?在听证阶段提出|' \
                               r'[^，。,；\n]*?在法定期限内(未)?提出(了)?(听证要求|陈述申辩|陈述申辩及听证要求)|' \
                               r'[^，。,；\n]*?(提交|提出)了(陈述)?申辩意见|' \
                               r'我局向.*?送达.*?提出|我局向你公司送达《行政处罚事先告知书》后，你公司对处罚事实提出了异议。|' \
                               r'[^，。,；\n]*?在陈述、申辩中(认为|表示)|' \
                               r'[^，。,；\n]*?在陈述、申辩中对违法事实的认定未提出异议|' \
                               r'在规定期限内.*?提出陈述和申辩|' \
                               r'[^，。,；\n]*?在陈述、申辩中未对我局认定的违规事实提出异议|' \
                               r'你公司对编制虚假资料等行为进行了申辩。|' \
                               r'[^，。,；\n]*?在陈述和申辩中未对我局认定的违规事实提出异议|' \
                               r'[^，。,；\n]*?在陈述和申辩中认为|' \
                               r'[^，。,；\n]*?在陈述和申辩中未对我局认定的违法违规事实提出异议|' \
                               r'[^，。,；\n]*?的陈述、申辩未对我局拟做出行政处罚的事实、理由及依据提出异议)' \
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
                               r'我局审核后认为|' \
                               r'我局复核后|' \
                               r'经研究，我局认为|' \
                               r'经核查|' \
                               r'对此，我局认为|' \
                               r'经复查|' \
                               r'经审查|' \
                               r'经复核|' \
                               r'经审核|' \
                               r'经核查|' \
                               r'经研究)'
            defense_compiler = re.compile(defense_text_str, re.MULTILINE)
            defense_list = defense_compiler.findall(content_text)
            if len(defense_list) != 0:
                defense = defense_list[-1][0].strip()
                defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                       + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                          r'对[^，。,；\n]*?申辩意见(不予|予以|不再)采纳|因此.*?申辩理由.*?成立。|' \
                                                          r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                          r'但陈述意见中部分客观情况可以作为情节予以考虑。|' \
                                                          r'因此，不再采纳你的申辩意见。|' \
                                                          r'因此不再重复考虑。|不予采纳。|' \
                                                          r'因此，我局不予采纳你的陈述、申辩意见。))'
                defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                if defense_response_compiler.search(content_text):
                    defense_response = defense_response_compiler.search(content_text).group(1).strip()
                else:
                    if '未' in defense:
                        defense_response = ''
            else:
                defense_text_str = '(在规定期限内.*?未行使陈述权和申辩权，也未要求举行听证。|' \
                                   '在规定期限内.*?未进行陈述和申辩，也未申请听证。|' \
                                   '在规定期限内.*?未行使陈述权和申辩权。|' \
                                   '在规定期限内.*?未行使陈述权和申辩权,也未要求举行听证。|' \
                                   '在规定期限内.*?未提出陈述和申辩，也未要求举行听证。|' \
                                   '你公司在陈述、申辩中对我局认定的违规事实表示认可，未对处罚依据提出异议。|' \
                                   '在规定期限内.*?未进行陈述和申辩。|' \
                                   '在规定期限内.*?未陈述和申辩，也未申请听证。|' \
                                   '在规定期限内.*?未陈述和申辩。|' \
                                   '你于2010年12月9日提交了陈述和申辩书，又于2010年12月27日撤销了该陈述和申辩书，放弃行使陈述权和申辩权。)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense = defense_compiler.search(content_text).group(1).strip()
                defense_response = ''
        else:
            defense = defense_response = ''

        punishment_decision_text_str = r'(((依据|根据)[^。；\n]*?第[^。；\n]*?条[^。；\n]*?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你|对你处以)|' \
                                       r'我局经复核认为|我局决定.*?作出(如下|以下)(行政)?处罚：|' \
                                       r'依据\n《保险公司管理规定》\n第九十九条之规定，决定给予|据此，我局作出如下行政处罚|' \
                                       r'依据《保险经纪机构管理规定》（保监会令\[2004\]15号）133条的规定，我局决定|' \
                                       r'依据《保险法》143条规定，我局决定)' \
                                       r'([\s\S]*?))' \
                                       r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如.*?不服本处罚决定|' \
                                       r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|$|' \
                                       r'我局.*?送达|.*?应在收到本处罚决定书之日)'

        punishment_decision_compiler = re.compile(punishment_decision_text_str)
        punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

        punishment_basis_str_list = [
            r'([^\n。]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反([^\n。\s]*?)第([^\n。\s]*?)条([\s\S]*?)',
            r'[^\n。；]*?违反了[^\n。；]*?第[^\n。；]*?条[^\n。；]*?'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'(。|\n|；|^)' + '(' + punishment_basis_str + ')' +
                                               r'.(\n?依据|\n?根据|\n?鉴于|\n?我局[\s\S]*?送达)', re.MULTILINE)
        punishment_basis_list = punishment_basis_compiler.findall(content_text)
        punishment_basis = '；'.join([kk[1].strip() for kk in punishment_basis_list])

        result_map = {
            'announcementTitle': title,
            'announcementOrg': '北京银保监局',
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
            logger.info('北京保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('北京保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('北京保监局 数据解析 ' + ' -- 修改parsed完成')
