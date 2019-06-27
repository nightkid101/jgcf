import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def hebei_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '河北保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('河北保监局 ' + 'Url to parse: %s' % announcement_url)

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

        document_code_compiler = re.compile(r'((保监冀罚|冀保监罚|冀银保监筹保罚决字)\s*?\n?.\n?\s*?\d{4}\n?.\n?\d+\n?\s*?号)')
        if document_code_compiler.search(content_text):
            document_code = document_code_compiler.search(content_text).group(1).strip()
            litigant_compiler = re.compile(
                document_code.replace(r'[', r'\[').replace(r']', r'\]') + r'\n([\s\S]*?)\n'
                + r'(经查|经检查|依据.*?有关规定|抽查|经抽查|.*?(现场检查|信访检查|专项检查))')
            litigant = litigant_compiler.search(content_text).group(1).strip()
        else:
            if document_code_compiler.search(title):
                document_code = document_code_compiler.search(title).group(1).strip()
            else:
                document_code = ''
            litigant_compiler = re.compile(r'^(中国银保监会河北监管局筹备组\n行政处罚决定书|河北保监局行政处罚决定书|中国保监会河北监管局行政处罚决定书)?([\s\S]*?)\n' +
                                           r'(经查|经检查|依据.*?有关规定|抽查|经抽查|.*?(现场检查|信访检查|专项检查))')
            litigant = litigant_compiler.search(content_text).group(2).strip()

        litigant = litigant.replace('中国保监会河北监管局行政处罚决定书', '').strip()

        truth_text_str = r'(经查，.*?存在以下违法行为：\n|经查，.*存在下列违法违规行为：\n|经抽查发现，|经查。你公司存在下列违法行为：|' \
                         r'经查，.*存在下列违法行为：\n?|经查，.*存在下列违规行为：\n?|经查,你.*?存在下列违法行为:\n?|经查，你存在下列违法\n行为：\n?|' \
                         r'经查，你公司违法违规行为)' \
                         r'([\s\S]*?)' \
                         r'(上述(违法)?事实，有.*?等证据(在案)?证明(在案)?(，|,)足以认定。|上述事实，有.*等证据证明。|上述违法事实有.*?等证据在案证明，足以认定|' \
                         r'该违法事实，有.*?等证据在案证明，足以认定|' \
                         r'该公司上述行为违反了《保险法》第.*?条|' \
                         r'综上，决定给予你公司.*?的行政处罚|' \
                         r'(上述|以上)(违法)?(事实(行为)?|行为)(分别)?违反了.*?第.*?条(的)?(规定)?|' \
                         r'依据《保险营销员管理规定》第.*条规定|' \
                         r'我局认为，.*?的规定|上述事实行为违反了\n《保险法》（\n2002\n年）第.*?条\n的规定|' \
                         r'上述事实行为违反了《中华人民共和国保险法》（\n2002\n年修正）第一百二十二条规定|' \
                         r'违反了《保险法》第一百二十二条、一百零七条规定)'
        truth_compiler = re.compile(truth_text_str)
        if truth_compiler.search(content_text):
            truth = truth_compiler.search(content_text).group(2).strip()
        else:
            truth_text_str = r'(经查，|经查,)' \
                             r'([\s\S]*?)' \
                             r'(上述(违法)?事实，有.*?等证据(在案)?证明(在案)?(，|,)足以认定。|上述事实，有.*等证据证明。|上述违法事实有.*?等证据在案证明，足以认定|' \
                             r'该违法事实，有.*?等证据在案证明，足以认定|' \
                             r'该公司上述行为违反了《保险法》第.*?条|' \
                             r'综上，决定给予你公司.*?的行政处罚|' \
                             r'(上述|以上)(违法)?(事实(行为)?|行为)(分别)?违反了.*?第.*?条(的)?(规定)?|' \
                             r'依据《保险营销员管理规定》第.*条规定|' \
                             r'我局认为，.*?的规定|上述事实行为违反了\n《保险法》（\n2002\n年）第.*?条\n的规定)'
            truth_compiler = re.compile(truth_text_str)
            if truth_compiler.search(content_text):
                truth = truth_compiler.search(content_text).group(2).strip()
            else:
                truth_text_str = r'(' + litigant + r'\n)' \
                                                   r'([\s\S]*?)' \
                                                   r'(上述(违法)?事实，有.*?等证据(在案)?证明(在案)?(，|,)足以认定。|' \
                                                   r'上述事实，有.*等证据证明。|上述违法事实有.*?等证据在案证明，足以认定|' \
                                                   r'该违法事实，有.*?等证据在案证明，足以认定|' \
                                                   r'该公司上述行为违反了《保险法》第.*?条|' \
                                                   r'综上，决定给予你公司.*?的行政处罚|' \
                                                   r'(上述|以上)(违法)?(事实(行为)?|行为)(分别)?违反了.*?第.*?条(的)?(规定)?|' \
                                                   r'依据《保险营销员管理规定》第.*条规定|' \
                                                   r'我局认为，.*?的规定|上述事实行为违反了\n《保险法》（\n2002\n年）第.*?条\n的规定)'
                truth_compiler = re.compile(truth_text_str)
                truth = truth_compiler.search(content_text).group(2).strip()

        if '申辩' in content_text:
            defense_text_str = r'((针对.*?行为.*?申辩意见|当事人[^，。,；\n]*?未提出陈述申辩意见|' \
                               r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                               r'[^，。,；\n]*?在(申辩材料|陈述申辩)中称：|当事人提出陈述申辩|' \
                               r'[^，。,；\n]*?提出[^，。,；\n]*?陈述申辩意见)' \
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
                               r'我局对.*?审核|' \
                               r'我局审核后认为)'
            defense_compiler = re.compile(defense_text_str, re.MULTILINE)
            defense_list = defense_compiler.findall(content_text)
            if len(defense_list) != 0:
                defense = defense_list[-1][0]
                defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                       + r'(([\s\S]*)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                         r'对[^，。,；\n]*?申辩意见(不予|予以|不)采纳|因此.*?申辩理由.*?成立。|' \
                                                         r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                         r'综上，我局对.*?陈述申辩意见不予采纳|决定对上述三人的陈述申辩意见不予采纳))'
                defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                defense_response = defense_response_compiler.search(content_text).group(1).strip()
            else:
                defense = re.search(r'(当事人盛衡公估提出陈述申辩,我局对陈述申辩意见进行了审核,并依法重新制发了《行政处罚事先告知书》。法定期限内，当事人未再提出陈述申辩。)',
                                    content_text).group(1).strip()
                defense_response = '本案现已审理终结。'
        else:
            defense = defense_response = ''

        punishment_decision_text_str = r'(((依据|根据)[^。；]*?第[^。；]*?条[^。；]*?(规定)?.?(我局)?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你)|' \
                                       r'(综上，)?我局决定.*?作出(如下|以下)(行政)?处罚|' \
                                       r'依据《中华人民共和\n国保险法》（2009年修订）第一百七十二条的规定，责令改正，决定给予)' \
                                       r'([\s\S]*?))' \
                                       r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                       r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|当事人如对本处罚决定不服|' \
                                       r'.*?如不服本处罚决定|请在接到本处罚决定书之日|.*?应在收到本处罚决定书之日)'

        punishment_decision_compiler = re.compile(punishment_decision_text_str)
        punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

        punishment_basis_str_list = [
            r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'永安财险石家庄中支达成赔偿协议后支付赔款时间超过10日，违反了《中华人民共和国保险法》（2015年修正）第二十三条和第一百一十六条第（五）项的规定，且情节严重',
            r'永安财险石家庄中支列支劳务费贴补代理公司手续费，违反了《中华人民共和国保险法》（2015年修正）第八十六条的规定',
            r'华夏人寿河北分公司对保险产品的不确定利益承诺保证收益，违反了《中华人民共和国保险法》（2015年修正）第一百一十六条第（一）项的规定',
            r'农行中长街分理处代理销售的部分保险业务客户信息不真实、不完整，违反了《中华人民共和国保险法》（2015年修正）第八十六条和第一百三十二条的规定',
            r'信泰人寿邯郸中支理赔业务部分数据不真实，违反了《中华人民共和国保险法》（2015年修正）第八十六条的规定，且情节严重',
            r'信泰人寿邯郸中支财务、业务系统数据不真实，违反了《中华人民共和国保险法》（2015年修正）第八十六条的规定',
            r'信泰人寿邯郸中支部分业务客户信息不真实，违反了《中华人民共和国保险法》（2015年修正）第八十六条的规定',
            r'信泰人寿邯郸中支未按监管规定完成犹豫期内新单回访，违反了《人身保险新型产品信息披露管理办法》第十条的规定',
            r'上述事实行为违反了[\s\S]*?规定(.*?负有直接责任)?',
            r'我机关认为[\s\S]*(行为|代理保险业务)[\s\S]*?违\n?反[\s\S]*?规定',
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile('[。\n；]' + '(' + punishment_basis_str + ')' +
                                               r'.(\n?依据|\n?根据|\n?鉴于)', re.MULTILINE)
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
            'announcementOrg': '河北银保监局',
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
            logger.info('河北保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('河北保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('河北保监局 数据解析 ' + ' -- 修改parsed完成')
