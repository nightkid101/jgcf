import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def heilongjiang_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '黑龙江保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('黑龙江保监局 ' + 'Url to parse: %s' % announcement_url)

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
        document_code = re.search(r'((黑银?保监罚字?|黑银保监（筹）保罚字|佳银?保监罚字).\d{4}.\d+号)', title).group(1).strip()

        if re.search(r'((黑银?保监罚字?|黑银保监（筹）保罚字|佳银?保监罚字).\d{4}.\d+号)\n', content_text):
            text_document_code = re.search(r'((黑银?保监罚字?|黑银保监（筹）保罚字|佳银?保监罚字).\d{4}.\d+号)\n',
                                           content_text).group(1)
            litigant_compiler = re.compile(text_document_code.replace(r'[', r'\[').replace(r']', r'\]')
                                           + r'\n?([\s\S]*?)\n'
                                           + r'(依据.*?有关规定|.*?(黑龙江保监局)?(检查组)?.*?对?.*?(现场)?检查|'
                                             r'一、违法事实及证据|一、违法事实和证据|经查|你.*?于.*?违反.*?规定)')
            litigant = litigant_compiler.search(content_text).group(1).strip()
        else:
            litigant_compiler = re.compile(
                r'^([\s\S]*?)\n' + r'(依据.*?有关规定|.*?，(黑龙江保监局)?(检查组)?.*?对.*?(现场)?检查|一、违法事实及证据|一、违法事实和证据|经查|'
                                   r'.*?我局.*?对.*?进行(检查|调查)|阳光财产保险股份有限公司伊春中心支公司于2008年经营交强险业务过程中)')
            litigant = litigant_compiler.search(content_text).group(1).strip()

        litigant = litigant.replace('行政处罚决定书', '').strip()

        truth_text_str = r'((经查|二、|三、|（二）|（三）|黑龙江保监局经检查发现下列事实|检查发现下列事实|经检查发现下列事实|经检查发现|' \
                         r'(黑龙江保监局|我局).*?对.*?进行(现场)?检查，发现.*?存在.*?行为|黑龙江保监局经现场检查，发现如下事实|' \
                         r'黑龙江保监局.*?对.*?检查，发现.*?以下问题|' \
                         r'我局对阳光财险黑龙江省分公司和阳光财险哈尔滨营业部进行了现场检查)' \
                         r'([\s\S]*?))' \
                         r'((上述|该|以上)(违法|违法违规)?(事实|行为).*?(有)?.*?等证据(在案)?(证明|佐证)(在案)?(.*?足以认定)?|' \
                         r'上述第一项行为违反了《中华人民共和国保险法》（2009）第一百一十六条第（六）项的规定|' \
                         r'当事人邹基德，对虚列业务及管理费负有直接责任)'
        truth_compiler = re.compile(truth_text_str)
        truth_list = truth_compiler.findall(content_text)
        if len(truth_list) > 0:
            truth = '\n'.join([each_truth[0] for each_truth in truth_list]).strip()
        else:
            truth_compiler = re.compile(r'(违法事实和证据|违法事实及证据)' +
                                        r'([\s\S]*?)' +
                                        r'((上述|该|以上)(违法|违法违规)?(事实|行为).*?(有)?.*?等证据(在案)?(证明|佐证)(在案)?(.*?足以认定)?)')
            if truth_compiler.search(content_text):
                truth = truth_compiler.search(content_text).group(2).strip()
            else:
                truth_compiler = re.compile(
                    litigant.replace(r'(', r'\(').replace(r')', r'\)') +
                    r'([\s\S]*?)' + r'(依据《中华人民共和国保险法》第一百四十五条第（八）项的规定|你是负有直接责任的高级管理人员。|'
                                    r'(上述|该|以上)(违法)?(事实|行为).*?(有)?.*?等证据(在案)?(证明|佐证)(在案)?(.*?足以认定)?|'
                                    r'(依据|根据).*?第.*?条.*?规定|'
                                    r'上述事实(行为)?.*?违反了.*?第?.*?条)')
                truth = truth_compiler.search(content_text).group(1).strip()

        if '申辩' in content_text:
            defense_text_str = r'((针对.*?行为.*?申辩意见|[^，。,；\n]*?未[^，。,；\n]*?提出陈述申辩(意见)?|' \
                               r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                               r'[^，。,；\n]*?(申辩材料|陈述申辩|申辩)中?称|' \
                               r'[^，。,；\n]*?陈述申辩.*?(要求|请求)|' \
                               r'[^，。,；\n]*?提出陈述申辩|' \
                               r'人保寿险鹤岗中心支公司对行政处罚认定的事实无异议，但请求减轻行政处罚。|' \
                               r'阳光财险黑龙江省分公司在陈述申辩材料中请求不吊销阳光财险哈尔滨营业部经营保险业务许可证|' \
                               r'当事人提出了听证及陈述申辩意见)' \
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
                               r'黑龙江保监局经复核认为|' \
                               r'黑龙江保监局经审理(后)?认为|' \
                               r'黑龙江保监局认为|' \
                               r'黑龙江保监局经过调查核实后认为|' \
                               r'我局认为，阳光财险黑龙江省分公司在陈述申辩材料中提出的理由与违法行为无关，不予采纳。|' \
                               r'我局依法举行了听证并对陈述申辩意见进行了复核)'
            defense_compiler = re.compile(defense_text_str, re.MULTILINE)
            defense_list = defense_compiler.findall(content_text)
            if len(defense_list) != 0:
                defense = defense_list[-1][0].strip()
                defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                       + r'(([\s\S]*)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                         r'对[^，。,；\n]*?申辩意见(不予|予以)采纳。|因此.*?申辩理由.*?成立。|' \
                                                         r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                         r'决定对.*?罚款数额适当下调。|' \
                                                         r'综上，我局.*?陈述申辩意见不予采纳|' \
                                                         r'黑龙江保监局对.*?申辩意见不予采纳。|' \
                                                         r'综上，黑龙江保监局.*?(陈述)?申辩(理由|内容)?不予采纳。|' \
                                                         r'综上，黑龙江保监局采纳.*?陈述申辩意见。|' \
                                                         r'综上，对.*?上述(陈述)?申辩理由不予采纳。|' \
                                                         r'五是人保财险黑龙江省分公司在检查前主动自查整改并报告，具有主动减轻违法行为危害后果的情节。|' \
                                                         r'组织落实自查整改并报告，具有主动减轻违法行为危害后果的情节。|' \
                                                         r'黑龙江保监局对.*?陈述申辩(理由)?不予采纳。|' \
                                                         r'黑龙江保监局对大地财险大庆中心支公司陈述申辩予以采纳，酌情给予从轻处罚。|' \
                                                         r'其辩称家庭原因和悔改表现不是法定从轻、减轻理由，不予采信。|' \
                                                         r'故对其第二项陈述申辩内容不予采纳|' \
                                                         r'故对其陈述申辩不予采纳。|' \
                                                         r'我局认为，阳光财险黑龙江省分公司在陈述申辩材料中提出的理由与违法行为无关，不予采纳。))'
                defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                defense_response = defense_response_compiler.search(content_text).group(1).strip()
        else:
            defense = defense_response = ''

        punishment_decision_text_str = r'(((依据|根据)[^。；\n]*?第?[^。；\n]*?条[^。；\n]*?(规定)?.?(我局|黑龙江保监局)?' \
                                       r'(决定|责令|给予|于.*?向.*?发出|对.*?作出|拟对你|依法撤销|对.*?给予)|' \
                                       r'(综上，)?我局.*?决定.*?作出(如下|以下)(行政)?处罚：)' \
                                       r'([\s\S]*?))' \
                                       r'(当事人应当在接到本处罚决定书之日|当事人.*?如对本处罚决定不服|本处罚决定书自送达之日起执行|' \
                                       r'你公司如不服本处罚决定|请你.*?在接到本处罚决定书之日|如不服本处罚决定|请你公司在接到本处罚决定书之日|' \
                                       r'请.*?在接到本处罚决定书之日|\n.*?履行方式及期限|.*?在收到本处罚决定书之日|请在收到本处罚决定书之日)'
        punishment_decision_compiler = re.compile(punishment_decision_text_str, re.MULTILINE)
        punishment_decision_list = punishment_decision_compiler.findall(content_text)
        punishment_decision = '\n'.join(
            [each_punishment_decision[0] for each_punishment_decision in punishment_decision_list]).strip()

        punishment_basis_str_list = [
            r'([^\n。；]*?)(问题|行为|事项|情况|事实|中华联黑龙江分公司虚列增值服务费|人保财险道外支公司未按照规定使用车险条款费率|'
            r'人保财险南岗支公司2015年虚列宣传费)([^\n。；\s]*?)违反.*?\n?.*?第.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'上述第一项行为违反《中华人民共和国保险法》第一百二十四条的规定。'
            r'上述第二项行为违反《中华人民共和国保险法》第八十六条、第一百三十三条的规定。'
            r'上述第三项行为违反《保险专业代理机构监管规定》（2013）第六十二条的规定。'
            r'上述第四项行为违反《保险专业代理机构监管规定》（2013）第三十二条的规定',
            r'你单位于2009年违反交强险费率浮动规定，违反了《中华人民共和国保险法》第一百零七条的规定',
            r'上述事实行为违反了《保险法》一百零七条、一百二十二条',
            r'上述事实行为违反了《保险法》一百二十二条',
            r'上述事实行为违反了《保险法》一百零七条',
            r'上述事实行为违反了《保险法》第八十条、一百零六条',
            r'上述事实行为违反了《保险法》一百零七条、一百零九条、一百二十二条',
            r'1、2008年未经批准在海伦设立黑龙江润融保险代理有限公司分支机构并担任主要负责人，经营保险代理业务，违反了《保险代理机构管理规定》第二条规定',
            r'1、黑龙江润融保险代理有限公司2008年未经批准在海伦设立分支机构，经营保险代理业务，违反了《保险代理机构管理规定》第二条规定',
            r'1、你公司2008年未经批准在海伦设立分支机构，经营保险代理业务，违反了《保险代理机构管理规定》第二条规定',
            r'上述事实行为违反了《机动车交通事故责任强制保险条例》的规定',
            r'新华人寿宁安支公司对业务员使用违规产品计划书销售保险产品未能实施有效管控，违反《保险公司管理规定》第五十五条的规定'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile('[。\n；]' + '(' + punishment_basis_str + ')' +
                                               r'.(\n*?依据|\n*?根据|\n*?鉴于|\n*?[^\n。；]*?陈述申辩)', re.MULTILINE)
        punishment_basis_list = punishment_basis_compiler.findall(content_text)
        punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list]).strip()

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
            'announcementOrg': '黑龙江银保监局',
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
            logger.info('黑龙江保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('黑龙江保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('黑龙江保监局 数据解析 ' + ' -- 修改parsed完成')
