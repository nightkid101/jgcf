import re

from utility import cn2dig, get_year, request_site_page, get_content_text
from bs4 import BeautifulSoup as bs
from oss_utils import oss_add_file, init_ali_oss

ali_bucket = init_ali_oss()


def jilin_circ(db, logger):
    for each_circ_data in db.circ_data.find({'origin': '吉林保监局', 'status': {'$nin': ['ignored']}}):
        announcement_url = each_circ_data['url']
        announcement_title = each_circ_data['title']

        if db.circ_data.find(
                {'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
            {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url,
             'parsed': True}).count() == 1:
            continue

        logger.info('吉林保监局 ' + 'Url to parse: %s' % announcement_url)

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

        document_code_compiler = re.compile(r'(吉银?保监[罚处].*?\d{4}.*?\d+.*?号)')
        if document_code_compiler.search(content_text):
            document_code = document_code_compiler.search(content_text).group(1).strip()
            litigant_compiler = re.compile(document_code.replace(r'[', r'\[').replace(r']', r'\]') +
                                           r'\n([\s\S]*?)\n' +
                                           r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|'
                                           r'2015年2月11日，吉林保监局向新华人寿保险股份有限公司长春中心支公司下发监管函|'
                                           r'2015年,新华人寿保险股份有限公司吉林中心支公司存在代抄录风险提示语句的违法行为。|'
                                           r'.*?对.*?现场检查|.*?存在.*?行为|'
                                           r'2008年7-8月，你公司所属南关营销服务部二部在客户答谢会上给予投保人蚕丝被、夏凉被等现场签单奖励|'
                                           r'你公司自2007年8月至检查日止从未组织过业务员参加代理人资格考试，截止检查日公司有营销员55人|'
                                           r'你在任阳光财险长春市二道区支公司负责人期间|'
                                           r'2007年6月，你公司承保车号为吉G41805、吉G42589、吉G42518等三辆车的交强险业务|'
                                           r'2007年6月13日，你公司所属洮南营销服务部承保了车号为吉G31290车的交强险业务|'
                                           r'你公司于2007年6月25日承保车牌号码为吉A7A263捷达轿车的商业车险业务|'
                                           r'2006年9月至2007年2月间，你公司虚列营业费用226,310.67元|'
                                           r'2006年11月至2007年2月间，你公司虚列营业费用257,484.49元)')
            litigant = litigant_compiler.search(content_text).group(1).replace('中国保监会行政处罚决定书', '').strip()
        else:
            if document_code_compiler.search(title):
                document_code = document_code_compiler.search(title).group(1).strip()
            else:
                document_code = ''
            litigant_compiler = re.compile(r'^([\s\S]*?)\n' +
                                           r'(经查|经检查|依据.*?的有关规定|抽查|经抽查|'
                                           r'2015年2月11日，吉林保监局向新华人寿保险股份有限公司长春中心支公司下发监管函|'
                                           r'2015年,新华人寿保险股份有限公司吉林中心支公司存在代抄录风险提示语句的违法行为。|'
                                           r'.*?对.*?现场检查|.*?存在.*?行为|'
                                           r'2008年7-8月，你公司所属南关营销服务部二部在客户答谢会上给予投保人蚕丝被、夏凉被等现场签单奖励|'
                                           r'你公司自2007年8月至检查日止从未组织过业务员参加代理人资格考试，截止检查日公司有营销员55人|'
                                           r'你在任阳光财险长春市二道区支公司负责人期间|'
                                           r'2007年6月，你公司承保车号为吉G41805、吉G42589、吉G42518等三辆车的交强险业务|'
                                           r'2007年6月13日，你公司所属洮南营销服务部承保了车号为吉G31290车的交强险业务|'
                                           r'你公司于2007年6月25日承保车牌号码为吉A7A263捷达轿车的商业车险业务|'
                                           r'2006年9月至2007年2月间，你公司虚列营业费用226,310.67元|'
                                           r'2006年11月至2007年2月间，你公司虚列营业费用257,484.49元|'
                                           r'你公司于\n2007年5月30日\n承保车牌号吉 BA6292 捷达车的商业车险业务|'
                                           r'2007年4月29日\n，你公司承保吉AH5950号家庭自用车的交强险业务|'
                                           r'2007年6月1日，人保财险长春市南关支公司委托吉林省宏大保险代理公司|'
                                           r'你在担任新华人寿保险股份有限公司延边中心支公司副总经理（主持工作）期间|'
                                           r'你公司未经我局批准，擅自在安图县二道镇、松江镇、两江镇设立营业网点|'
                                           r'2007年4月9日，你公司朝阳营销服务部（现安华农业保险股份有限公司长春市朝阳支公司）承保吉A19714号车的商业车险业务|'
                                           r'2005年7月至2006年6月，你公司共10次以现金方式向保险兼业代理机构长春通立汽车服务有限\n?公司支付代理手续费|'
                                           r'你公司于\n2006年4月21日\n承保了车牌号为吉A27130的车险业务|'
                                           r'2005年12月末，安华农业保险股份有限公司四平中心支公司委托时任安华农业'
                                           r'保险股份有限公司辽源营销服务部经理王全勇在东丰县筹建东丰营销服务部|'
                                           r'2005年12月末，你受安华农业保险股份有限公司四平中心支公司委托在东丰县筹建东丰营销服务部)')
            litigant = litigant_compiler.search(content_text).group(1).replace('中国保监会行政处罚决定书', '').strip()

        truth_text_str = r'(经查，|经查,|经检查，|经查实，|检查发现，|现场检查，发现|抽查，|经抽查，|经查.*?存在以下问题：)' \
                         r'([\s\S]*?)' \
                         r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等(证据|书证)(在案)?(证明|为证)(,|，|。)(足以认定。)?|' \
                         r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                         r'你公司未及时履行赔偿保险金义务、制作“鸳鸯保单”、异地开展车险业务|' \
                         r'人民广场支公司采用账外经营的方式，办理商业车险业务，导致公司业务财务数据不真实|' \
                         r'你部以上行为违反了《保险法》第一百二十二条的规定|' \
                         r'人保财险德惠支公司上述行为违反了《保险法》第二十四条的规定。|' \
                         r'你公司违规减收保费，给予被保险人保费优惠的行为|' \
                         r'平安财险长春市经济技术开发区支公司给予被保险人额外利益的行为|' \
                         r'上述\n行为违反了《保险法》第一百二十二条的规定。|' \
                         r'人保财险磐石支公司采用制作“鸳鸯单”，保费收入不入账的行为|' \
                         r'人保财险长春市高新支公司扩展责任条款未向保险监管部门报备的行为|' \
                         r'人保财险通化市分公司第一营业部异地承保车险业务，且未与有业务往来的代理公司签订代理合同的行为|' \
                         r'你公司通过改变车辆使用性质变相降低费率的行为|' \
                         r'绿园支公司虚列应收保费的行为违反了《保险法》第一百二十二条的规定|' \
                         r'你公司虚列应收保费的行为违反了《保险法》第一百二十二条的规定|' \
                         r'你公    司向未取得代理资格的代理人支付商业车险和交强险手续费的行为|' \
                         r'安邦财险吉林分公司营业部向未取得保险代理资格的代理人支付商业车险和交强险手续费的行为|' \
                         r'上述事实行为违反了《中华人民共和国保险法\n?》第八十条之规定|' \
                         r'你公司未经保险监管部门批准擅自设立分支机构开办保险业务的行为违反了《保险法》第八十条的规定)'
        truth_compiler = re.compile(truth_text_str)
        if truth_compiler.search(content_text):
            truth = truth_compiler.search(content_text).group(2).strip()
        else:
            truth_text_str = litigant + r'([\s\S]*?)' \
                                        r'((我局认为，)?(上述|以上).*?(事实|行为|事实)(,|，)?有.*?等证据(在案)?证明(,|，|。)(足以认定。)?|' \
                                        r'(我局认为，|综上，)?(上述|以上).*?(行为|问题|事实).*?违反.*?第.*条.*?(的规定)?|' \
                                        r'上述委托未取得《保险代理从业人员资格证书》人员开展保险业务的行为，违反了《保险营销员管理规定》第\n四十三条的规定|' \
                                        r'你公司所属分支机构未及时履行赔偿保险金义务的行为违反了《保险法》第二十四条的规定|' \
                                        r'你公司采用系统外出单方式承保车险业务的行为|' \
                                        r'你公司采用制作“鸳鸯”单的方式给予被保险人优惠的行为)'
            truth_compiler = re.compile(truth_text_str)
            truth = truth_compiler.search(content_text).group(1).strip()

        if '申辩' in content_text:
            defense_text_str = r'((针对.*?(行为|问题).*?申辩意见|(当事人)?[^，。,；\n]*?(未)?提出(了)?陈述申辩(意见)?|' \
                               r'[^，。,；\n]*?向我局(报送|递交|提出)[^，。,；\n]*?|本案在审理过程中.*?提出陈述申辩|' \
                               r'[^，。,；\n]*?在(申辩材料|陈述申辩|陈述申辩意见|申辩意见)中称|[^，。,；\n]*?在听证阶段提出|' \
                               r'[^，。,；\n]*?在(法定|规定)期限内(未)?(提出|提交)(了)?(听证要求|陈述申辩|陈述申辩及听证要求|陈述、申辩材料)|' \
                               r'[^，。,；\n]*?申辩称)' \
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
                               r'经查，我局认为|经复核|鉴于你公司|' \
                               r'依据现场检查及听证情况|' \
                               r'经我局复核|我局经过复核认为|当事人并未对我局认定的违法事实、处罚理由及依据提出异议|' \
                               r'复核认为)'
            defense_compiler = re.compile(defense_text_str, re.MULTILINE)
            defense_list = defense_compiler.findall(content_text)
            if len(defense_list) != 0:
                defense = defense_list[-1][0].strip()
                defense_response_str = defense.replace(r'[', r'\[').replace(r']', r'\]') \
                                       + r'(([\s\S]*?)' + r'(本案现已审理终结。|不符合.*?情形。|根据.*?依法可从轻或者减轻行政处罚。|' \
                                                          r'对[^，。,；\n]*?申辩意见(不予|予以)采纳|因此.*?申辩理由.*?成立。|' \
                                                          r'我局认为.*?申辩(理由|意见).*?符合.*?第.*?条.*?的条件.(予以采纳。)?|' \
                                                          r'有关依法从轻或减轻行政处罚的规定。|在规定期限内，你公司未行使陈述权和申辩权。|' \
                                                          r'应对该支公司的违法违规问题承担责任。))'
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

        punishment_decision_text_str = r'(((依\n?据|根据)[^。；]*?第[^。；]*?条[^。；]*?(规定)?.?(我局)?(决定|责令|给予|于.*?向.*?发出|对.*?\n?作出|拟对你|拟给予|现对你处以)|' \
                                       r'我局经复核认为|我局决定[^。；\n]*?作出(如下|以下)(行政)?处罚：|综上，决定作出如下处罚：|' \
                                       r'我局决定[^。；\n]*?|依据该条规定，现给予)' \
                                       r'([\s\S]*?))' \
                                       r'(请在本处罚决定书送达之日|当事人应当在接到本处罚决定书之日|如不服本处罚决定|' \
                                       r'请.*?接到本处罚决定书之日|如不服从本处罚决定|二〇〇八年九月十八日|请你公司及时向总公司汇报处罚结论|' \
                                       r'.*?应当在接到本处罚决定书之日)'

        punishment_decision_compiler = re.compile(punishment_decision_text_str)
        punishment_decision = punishment_decision_compiler.search(content_text).group(1).strip()

        punishment_basis_str_list = [
            r'([^\n。；]*?)(问题|行为|事项|情况|事实)([^\n。；\s]*?)违反.*?\n?.*?第?.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            r'上述行为违反了《保险法》（2015年修正）（以下同）八十六条之规定',
            r'我局认为，上述行为致使公司个险期缴保费数据在较短时间内出现波动，影响了公司财务、业务数据的真实性，'
            r'其行为违反了《中华人民共和国保险法》第八十六条的规定。\n曹磊时任该公司主要负责人，应对上述违法行为负直接责任',
            r'人民广场支公司采用账外经营的方式，办理商业车险业务，导致公司业务财务数据不真实，'
            r'违反了《保险法》第一百二十二条的规定，你作为该支公司经理对以上违法行为负有直接领导责任。根据《保险法》第一百五十条的规定',
            r'我局认为，上述行为违反了《保险法》第一百一十六条第十三项及《关于加强和完善保险营销员管理工作有关事项的通知》'
            r'（保监发[2009]98号）第三条、第六条的规定。\n当事人翟廷杰应对上述违规招聘营销员的行为负直接责任'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                               '.(\n?依\n?据|\n?根据|\n?鉴于|\n?我局决定|\n?翟廷杰在申辩材料中称|\n?中国人寿松原分公司在申辩材料中称)',
                                               re.MULTILINE)
        punishment_basis_list = punishment_basis_compiler.findall(content_text)
        punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

        publish_date_text = re.search(
            punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
            replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')
        if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
            publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '')
            m = re.match("([0-9零一二两三四五六七八九十〇○ＯOΟО]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
            real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
        else:
            publish_date_text = table_content.find_all('tr')[1].text
            publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
            real_publish_date = publish_date.split('-')[0] + '年' + str(int(publish_date.split('-')[1])) + '月' + str(
                int(publish_date.split('-')[2])) + '日'

        result_map = {
            'announcementTitle': title,
            'announcementOrg': '吉林银保监局',
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
            logger.info('吉林保监局 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('吉林保监局 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('吉林保监局 数据解析 ' + ' -- 修改parsed完成')
