import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def jiangsu_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '江苏保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('江苏保监局 ' + 'Url to parse: %s' % announcement_url)

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
        content_text = get_content_text(table_content.find_all('tr')[3]).replace('（2015\n年修订）', '（2015年修订）')
        if content_text == '':
            continue
        title = table_content.find_all('tr')[0].text.strip()

        document_code_compiler = re.compile(r'((苏保监罚|苏银保监罚决字).\n?\d{4}\n?.\n?\d+\n?号)')
        if document_code_compiler.search(content_text):
            document_code = document_code_compiler.search(content_text).group(1).strip()
        else:
            document_code = ''

        litigant_compiler = re.compile(document_code.replace(r'[', r'\[').replace(r']', r'\]')
                                       + r'\n([\s\S]*?)\n' + r'(依据有关法律规定|依据.*?的有关规定|经查，|经查,|经检查|'
                                                             r'2015年5月，大童保险销售服务有限公司南通营业部|'
                                                             r'根据《农业保险条例》第十五条第三款的规定|'
                                                             r'你公司存在给予投保人保险合同约定以外其他利益的行为|'
                                                             r'2011年，你公司赔案号为605012011320000000259等89笔赔案|'
                                                             r'2009\n年\n2\n月至\n2010\n年\n8\n月期间|'
                                                             r'2007\n年\n12\n月\n19\n至\n20\n日|2007\n年\n11\n月\n13\n日|'
                                                             r'2009年11月26-27日|2008年6月6日—8月6日|2008年7月22—23日|'
                                                             r'2007年.*?(以来|期间)|2008年4月江苏保监局对你公司现场检查|'
                                                             r'在你任.*?期间|2007年，你单位采取批单注销保单和批减保额、保费的方式|'
                                                             r'2007\n年\n4\n月以来|2007\n年\n6\n月\n20\n日|'
                                                             r'2006年4月17日)')
        litigant = litigant_compiler.search(content_text).group(1).strip()

        truth_text_str = r'(经查，|经查,|经检查，|经查实，|检查发现，|现场检查，发现|公司存在以下问题：|存在以下违规问题：|2007年.*?以来|2007年.*?期间|' \
                         r'南京市秦淮区中山东路300号2幢1601室|主要负责人：孙伟群|主要负责人：韩安萍|主要负责人：陆刚|地址：泰州市江洲南路117号|' \
                         r'单位地址：泰州市江洲南路117号|你单位在经营中存在以下违法行为：|我局发现|本案现已审理终结。)' \
                         r'([\s\S]*?)' \
                         r'(上述事实(,|，)?有.*?等证据证明(,|，|。)(足以认定。)?|' \
                         r'上述行为违反了《保险代理机构管理规定》第六十三条的规定|' \
                         r'[^，。,；\n]*?上述.*?(违法|事实)?(行为|问题).*?违反了.*?第.*?条.*?的?规定|上述保单出具批单、批注或补充协议属于该公司权限，该公司应对上述行为负责。|' \
                         r'[^，。,；\n]*?上述违法行为(,|，)我局决定对.*?作出以下行政处罚：|' \
                         r'针对你在未取得执业证书的情况从事保险销售的行为|' \
                         r'2013年8月至11月，江苏太平先后与江苏中诚保险销售有限公司和江苏华邦保险销售有限公司签订代理协议|' \
                         r'针对.*?上述违法行为.{1}我局决定对.*?作出(如下|下列)(行政)?处罚：|' \
                         r'你公司的上述行为《中华人民共和国保险法》（以下简称《保险法》）第一百三十一条第（八）项的规定|' \
                         r'根据《保险销售从业人员监管办法》第三十一条的规定|上述行为构成了.*?第.*?条规定的违规行为|' \
                         r'此行为构成了《保险营销员管理规定》第五十二条规定的违规行为|该行为违反了《保险公司管理规定》第六十二条和第六十三条的规定|' \
                         r'依据.*?第.*?规定|[^，。,；\n]*?(行为|问题)[^。；\n]*?违反了.*?第[\s\S]*?条.*?的?规定)'
        truth_compiler = re.compile(truth_text_str)
        truth = truth_compiler.search(content_text).group(2).strip()

        if '申辩' in content_text:
            defense_text_str = r'((针对.*?行为.*?申辩意见|当事人[^，。,；\n]*?未?提出陈述申辩(意见)?|' \
                               r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                               r'[^，。,；\n]*?在(申辩材料|陈述申辩)中称：|' \
                               r'你公司可在收到本告知书之日起10日内到我局进行陈述和申辩。|' \
                               r'你未?提出陈述申辩)' \
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
                               r'我局对陈述申辩意见进行了复核|' \
                               r'我局经过?复核认为|' \
                               r'逾期视为放弃陈述权和申辩权。|' \
                               r'经对你公司陈述申辩意见进行复核，我局认为|' \
                               r'经复核你的陈述申辩意见和案卷材料，我局认为|' \
                               r'我局认为,对你的调查笔录|' \
                               r'我局复核认为)'
            defense_compiler = re.compile(defense_text_str, re.MULTILINE)
            defense_list = defense_compiler.findall(content_text)
            if len(defense_list) != 0:
                defense = defense_list[-1][0].strip()
                defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                       + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                          r'对[^，。,；\n]*?申辩意见(不予|予以)采纳|因此.*?申辩理由.*?成立。|' \
                                                          r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                          r'不予采纳。))'
                defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                if defense_response_compiler.search(content_text):
                    defense_response = defense_response_compiler.search(content_text).group(1).strip()
                else:
                    if '未' in defense or '你公司可在收到本告知书之日起10日内到我局进行陈述和申辩。' in defense:
                        defense_response = ''
        else:
            defense = defense_response = ''

        punishment_decision_text_str = r'(((依据|根据)[^。；\n]*?第[^。；\n]*?条[^。；\n]*?(规定)?.?(我局)?' \
                                       r'(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你)|' \
                                       r'我局决定.*?作出(如下|以下)(行政)?处罚：|' \
                                       r'根据《保险法》第一百七十一的规定，我局决定|依据《保险公司管理规定》第六十九的规定,我局责令|' \
                                       r'根据《中华人民共和国行政许可法》第七十八的规定，我局决定|' \
                                       r'根据《中华人民共和国保险法》一百七十三条的规定,我局决定|' \
                                       r'根据《中华人民共和国保险法》第一百六十二条、第一百七十二条和《中华人民共和国行政处罚法》第二十七条的规定\n,\n我局责令|' \
                                       r'根据《中华人民共和国保险法》第一百六十二的规定，我局责令|' \
                                       r'根据有关证据及听证会情况，我局决定如下：|' \
                                       r'根据该条(规定)?，我局|按照《行政处罚法》第二十七条的规定依法减轻行政处罚，我局决定|' \
                                       r'根据《中华人民共和国保险法》一百五十条之规定，我局决定|' \
                                       r'依据《保险代理机构管理规定》第\n129\n条、第\n132\n条、第\n138\n条、第\n140\n条、第\n142\n条和第\n143\n条的规定)' \
                                       r'([\s\S]*?))' \
                                       r'(当事人应当在接到本处罚决定书之日|当事人如对本处罚决定不服|本处罚决定书自送达之日起执行|你公司如不服本处罚决定|' \
                                       r'请在接到本处罚决定书之日|如不服本处罚决定|因你公司现已搬离南京市鼓楼区管家桥85号华荣大厦410室)'
        punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
        punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

        punishment_basis_str_list = [
            r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)(违反|构成).*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'你公司的上述行为《中华人民共和国保险法》（以下简称《保险法》）第一百三十一条第（八）项的规定',
            r'上述行为违反了《中华人民共和国保险法》一百三十六条的规定',
            r'上述行为构成了《保险专业代理机构监管规定》第八十七条规定的违规行为。你作为该公司的董事长兼总经理，对上述违规行为直接负责',
            r'上述行为构成了《保险专业代理机构监管规定》第八十七条规定的违规行为',
            r'此行为构成了《保险营销员管理规定》第五十二条规定的违规行为',
            r'上述事实行为违反了《保险代理机构管理规定》第\n21\n条、第\n41\n条、第\n56\n条、'
            r'第\n91\n条、第\n96\n条、第\n101\n条、第\n104\n条、第\n107\n条和第\n108\n条的规定'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile('[。\n；]' + '(' + punishment_basis_str + ')' +
                                               r'.(\n?(应当?)?依据|\n?(应当)?根据|\n?鉴于|\n?应你的要求|\n?.*?陈述申辩)', re.MULTILINE)
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
            'announcementOrg': '江苏银保监局',
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
            logger.info('江苏保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('江苏保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('江苏保监局 数据解析 ' + ' -- 修改parsed完成')
