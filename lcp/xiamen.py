import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def xiamen_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '厦门保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('厦门保监局 ' + 'Url to parse: %s' % announcement_url)

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

        document_code_compiler = re.compile(r'((厦保监[罚处]|厦银保监(（筹）)?罚决字).\d{4}.\d+号)')
        if document_code_compiler.search(content_text):
            document_code = document_code_compiler.search(content_text).group(1).strip()
            litigant_compiler = re.compile(document_code.replace(r'[', r'\[').replace(r']', r'\]')
                                           + r'\n([\s\S]*?)\n' +
                                           r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|'
                                           r'近期，我局对.*一案进行了调查、审理|'
                                           r'你在.*?期间|'
                                           r'我局于.*?对.*?进行了现场检查，查明|'
                                           r'你公司在2009年协助天安保险股份有限公司厦门分公司思明营销服务部虚构保险中介业务套取费用|'
                                           r'你公司在\n2006\n年7-12月业务经营过程中，违反了《保险法》第一百二十二条的规定)')
            litigant = litigant_compiler.search(content_text).group(1).replace('行政处罚决定书', '').strip()
        else:
            if document_code_compiler.search(title):
                document_code = document_code_compiler.search(title).group(1).strip()
            else:
                document_code = ''
            litigant_compiler = re.compile(r'^([\s\S]*?)\n' +
                                           r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|'
                                           r'近期，我局对.*一案进行了调查、审理|'
                                           r'你在.*?期间|我局于.*?对.*?进行了现场检查，查明|'
                                           r'你公司在2009年协助天安保险股份有限公司厦门分公司思明营销服务部虚构保险中介业务套取费用|'
                                           r'你公司在\n2006\n年7-12月业务经营过程中，违反了《保险法》第一百二十二条的规定|'
                                           r'我局在对[\s\S]*?现场检查|'
                                           r'你公司在参与中国农业银行厦门市分行“\n2007\n年度保险项目招标”过程中|'
                                           r'根据中国保监会《关于做好\n2006\n年全国财产保险专项现场检查工作的通知》|'
                                           r'你公司于\n2006\n年8月11日|'
                                           r'你于\n2006\n年8月12日上午\n在厦门广播电视大学|'
                                           r'你公司未经批准在厦设立出单点,违反了《保险法》第八十条的规定|'
                                           r'你公司于2005年7月至2005年11月期间未经批准在厦门市金榜路凯旋广场六楼设立出单点，违反了《保险法》第八十条的规定)')
            litigant = litigant_compiler.search(content_text).group(1).replace('行政处罚听证权利告知书', '').replace(
                '行政处罚事先告知书', '').replace('行政处罚决定书', '').replace('中国保监会厦门监管局', '').replace(
                '中国保监会厦门监管局行政处罚事先告知书', '').strip()

        truth_text_str = r'(经查，|经查,|经检查，|经查实，|检查发现，|现场检查，发现|抽查，|经抽查，|经查.*?存在以下问题：|现场检查，查明|' \
                         r'现场检查中发现，)' \
                         r'([\s\S]*?)' \
                         r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                         r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                         r'以上事实，有现场检查事实确认书、电销中心旧址和新\n址租赁合同、电销业务清单、中国保监会厦门监管局现场勘验笔录、调查笔录等证据在案证明，足以认定。|' \
                         r'根据《保险销售从业人员监管办法》第三十四条规定|综上，我局决定|' \
                         r'，该行为违反了《保险营销员管理规定》第五十二条的规定|' \
                         r'你的行为违反了《保险营销员管理规定》第三十六条的规定|' \
                         r'，违反了《保险法》第一百二十二条、《保险公司管理规定》第九十五条的规定|' \
                         r'，违反了《保险法》第一百二十二条的规定|' \
                         r'依据《保险代理机构管理规定》第一百三十六条规定|' \
                         r'，依据《保险统计管理暂行规定》第三十八条规定|' \
                         r'依据《保险统计管理暂行规定》第三十八条规定，我局拟对你公司作出警告的行政处罚。|' \
                         r'，依据《保险法》第一百四十五条规定)'
        truth_compiler = re.compile(truth_text_str)
        if truth_compiler.search(content_text):
            truth = truth_compiler.search(content_text).group(2).strip()
        else:
            truth_text_str = litigant + r'([\s\S]*?)' \
                                        r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                        r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                                        r'，违反了《人身保险新型产品信息披露管理办法》第.*?条的规定|' \
                                        r'根据《保险营销员管理规定》、《厦门市保险代理人资格考试违规处理办法》|' \
                                        r'根据.*?第.*?条.*?规定|' \
                                        r'依据《保险法》第一百四十七条的规定，我局拟对你公司作出罚款10万元的行政处罚|' \
                                        r'依据《保险法》第一百四十七条的规定，我局拟责令你公司改正上述违规行为，处以罚款10万元的行政处罚。|' \
                                        r'依据《保险代理机构管理规定》第一百三十六条规定|' \
                                        r'，依据《保险法》第一百三十九条及《保险公司管理规定》第九十九条规定|' \
                                        r'依据《保险法》第一百三十九条、《保险公司管理规定》第九十九条规定|' \
                                        r'，综合考虑公司积极配合检查并主动进行整改|' \
                                        r'依据《保险法》第一百三十九条、《中华人民共和国行政处罚法》第二十七条的规定|' \
                                        r'，鉴于情节较轻，依据《保险法》第一百三十九条第二款及《保险公司管理规定》第九十九条规定|' \
                                        r'依据《保险公司管理规定》第九十九条规定，我局拟对你公司作出警告的行政处罚|' \
                                        r'依据《保险营销员管理规定》第七章第五十二条规定，拟对你予以警告处分，并处叁仟元罚款)'
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
                defense_text_str = r'(可在收到本告知书之日起10日内到我局（厦门市鹭江道98号建行大厦39层）进行陈述和申辩。逾期视为放弃陈述权和申辩权。|' \
                                   r'可以在收到本告知书之日起10日内向我局或中国保监会提出书面的陈述、申辩意见。逾期视为放弃陈述或申辩的权利。)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense = defense_compiler.search(content_text).group(1).strip()
                defense_response = ''
        else:
            defense = defense_response = ''

        punishment_decision_text_str = r'(((依据|根据).*?第?.*?条.*?(规定)?.?(我局)?(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你)|' \
                                       r'我局经复核认为|我局决定.*?作出(如下|以下)(行政)?处罚：|综上，我局做出如下处罚：|' \
                                       r'根据《保险营销员管理规定》、《厦门市保险代理人资格考试违规处理办法》|我局决定)' \
                                       r'([\s\S]*?))' \
                                       r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                       r'请(在)?接到本处罚决定书之日|如不服从本处罚决定|\n.{4}年.*?月.*?日|联系部门：厦门保监局|' \
                                       r'逾期视为放弃陈述权和申辩权)'

        punishment_decision_compiler = re.compile(punishment_decision_text_str)
        punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

        punishment_basis_str_list = [
            r'([^\n。；\s]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'中国人寿财产保险股份有限公司厦门市分公司上述行文违反了《中华人民共和国保险法》（2015年修正）第八十六条',
            r'鉴于你作为该公司的主要负责人，对上述违规行为负有责任',
            r'经查，你公司存在财务数据不真实，违反了《保险法》第一百二十二条的规定',
            r'经查，你公司违反了国家税务总局、中国保险监督管理委员会《关于规范保险中介服务发票有关问题的通知》（国税发\[\n2004\n\]51号）中第二条的规定',
            r'你公司在\n2006\n年7-12月业务经营过程中，违反了《保险法》第一百二十二条的规定',
            r'我局在对你公司的保险中介业务现场检查中发现，你公司\n违反了《保险代理机构管理规定》第三十九条、第五十六条及国家税务总局、中国保险监督管理委员会《关于规范保险中介服务发票有关问题的通知》（国税发\[\n2004\n\]51号）中第二条的规定',
            r'经查，你公司\n违反了《保险法》第一百零六条规定',
            r'你公司在参与中国农业银行厦门市分行“\n2007\n年度保险项目招标”过程中，违反了《保险法》第一百零六条的规定',
            r'经查，你公司\n违反了《保险法》第一百零六条的规定',
            r'经查，你公司未按照相关规定对公司员工王少伟发放《执业证书》，违反了《保险代理机构管理规定》第五十六条的规定',
            r'我局在对你公司的保险中介业务现场检查中发现，你公司\n存在在营业场所外另设代理网点开展代理业务等\n问题，违反了《保险兼业代理管理暂行办法》第二十一条的规定',
            r'我局在对厦门祥安保险代理有限公司的保险中介业务现场检查中发现，违反了《保险代理机构管理规定》第九十一条的规定',
            r'我局在对你公司的保险中介业务现场检查中发现，你公司违反了《保险代理机构管理规定》第九十一条的规定',
            r'经查，你公司\n违反了《保险法》第一百二十二条和《保险统计管理暂行规定》第二十四条、第二十五条的规定',
            r'我局在对中国人民财产保险股份有限公司厦门市分公司的保险统计现场检查中发现，你公司在\n2005\n年的业务经营过程中，违反了《保险法》第一百二十二条和《保险统计管理暂行规定》第二十四条、第二十五条的规定',
            r'我局在对中国人民财产保险股份有限公司厦门市分公司的保险统计现场检查中发现，'
            r'你公司在\n2005\n年全年和\n2006\n年1季度的业务经营过程中，违反了《保险法》第一百二十二条和《保险统计管理暂行规定》第二十四条、第二十五条的规定',
            r'经查，你公司违反了《保险法》第一百零六条、第一百二十二条、《保险公司管理规定》第九十五条的规定',
            r'你公司于\n2006\n年8月11日\n在翔安区公务用车统一保险招标竞争性谈判项目中，违反了《保险法》第一百零六条、第一百二十二条，《保险公司管理规定》第九十五条的规定',
            r'你于\n2006\n年8月12日上午\n在厦门广播电视大学举行的保险代理从业人员基本资格电子化考试中代替陈亚勉参加考试，该行为违反了《保险营销员管理规定》',
            r'经查，你公司存在下列未经批准设立出单点并在出单点接受投保、开展业务、打印保单等变相设立分支机构的行为，\n违反了《保险法》第八十条的规定',
            r'你公司未经批准在厦设立出单点,违反了《保险法》第八十条的规定',
            r'你公司于2005年7月至2005年11月期间未经批准在厦门市金榜路凯旋广场六楼设立出单点，违反了《保险法》第八十条的规定',
            r'经查，你在担任厦门祥安保险代理有限公司总经理期间，该公司违反了《保险代理机构管理规定》第九十一条的规定',
            r'经查，你公司\n违反了《保险法》第一百零六条的规定，综合考虑公司积极配合检查并主动进行整改'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                               '.(\n?依据|\n?根据|\n?鉴于|\n?我局决定)', re.MULTILINE)
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
            'announcementOrg': '厦门银保监局',
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
            logger.info('厦门保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('厦门保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('厦门保监局 数据解析 ' + ' -- 修改parsed完成')
