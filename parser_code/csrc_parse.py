import re

from pymongo import MongoClient
from init import logger_init, config_init
from utility import request_site_page, get_content_text, format_date
from bs4 import BeautifulSoup as bs
from oss_utils import init_ali_oss, oss_add_file

logger = logger_init('证监会 数据解析')
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

ali_bucket = init_ali_oss()


# 证监会解析
def parse_csrc(url, doc_type, data_id, org):
    logger.info(doc_type)
    logger.info('url to parse ' + url)
    r = request_site_page(url)
    if r is None:
        logger.error('网页请求错误')
        return
    content_soup = bs(r.text.encode(r.encoding).decode('utf-8'), 'lxml')

    if doc_type != '要闻':
        content_text = get_content_text(content_soup.find(class_='mainContainer'))
        head_info = content_soup.find(class_='headInfo')
        title = head_info.find_all('tr')[4].text.split(':')[1].strip()

        if db.parsed_data.find({'origin_url': url, 'oss_file_origin_url': url}).count() == 0:
            oss_file_map = {
                'origin_url': url,
                'oss_file_origin_url': url,
                'origin_url_id': data_id,
                'oss_file_type': 'html',
                'oss_file_name': title,
                'oss_file_content': r.text.encode(r.encoding).decode('utf-8'),
                'parsed': False
            }
            response = db.parsed_data.insert_one(oss_file_map)
            file_id = response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + title + '.html',
                         r.text.encode(r.encoding).decode('utf-8'))
            db.csrc_data.update_one({'_id': data_id}, {'$set': {'status': 'parsed'}})
        else:
            db.csrc_data.update_one({'_id': data_id}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': url, 'oss_file_origin_url': url})['_id']

        if doc_type == '行政处罚决定':
            real_publish_date = format_date(head_info.find_all('tr')[2].find_all('td')[2].text.split(':')[1].strip())
            document_code = re.search(r'\n((证监罚字)?.\d{4}.\d+号)\n', content_text).group(1).strip()
            litigant_compiler = re.compile(document_code.replace(r'[', r'\[').replace(r']', r'\]')
                                           + r'\n([\s\S]*?)'
                                             r'(依据[\s\S]*?规定|经查明|经查|.*?对.*?进行.*?调查|'
                                             r'张家界旅游开发股份有限公司（以下简称张家界公司）有关人员信息披露违法案|'
                                             r'泰阳证券有限责任公司（以下简称“泰阳证券”）证券违法案|'
                                             r'民安证券挪用客户交易结算资金一案，我会于2005年6月7日决定立案调查|'
                                             r'长沙市商业银行非法划扣泰阳证券有限责任公司（以下简称泰阳证券）客户交易结算资金一案|'
                                             r'上海大众公用事业（集团）股份有限公司\(以下简称大众公用\)证券违法案|'
                                             r'广东科龙电器股份有限公司（以下简称科龙公司）|'
                                             r'武汉证券有限责任公司（以下简称武汉证券）责任人员违法案|'
                                             r'日前，深大通违法一案, 已由我会调查、审理终结，并依法向当事人履行了事先告知程序。|'
                                             r'四通集团高科技股份有限公司（以下简称四通高科）信息披露违法案|'
                                             r'日前|.*?一案,现已|.*?已由我会调查审理终结|.*?立案调查|.*?一案)')
            if litigant_compiler.search(content_text):
                litigant = litigant_compiler.search(content_text).group(1).strip()
            else:
                litigant = re.search(r'^.*?\n\n\n\n.*?\n([\s\S]*?)依据.*?规定', content_text).group(1).strip()
            if litigant == '':
                litigant = re.search(r'关于(.*?)违反.*?处罚决定', title).group(1).strip()

            truth_text_str = r'((.*?经查|二、|三、|四、|（二）|五、|司法机关认定.*?存在以下违法事实：|现查明|一、(违规|违法| 违规)(事实|行为))' \
                             r'[\s\S]*?)' \
                             r'((上述|以上)(违法(违规)?)?(事实|情况|行为)[\s\S]*?等?[^。，\n]*?(证据|佐证|谈话笔录等|作证)|' \
                             r'陈述等证据证明，事实清楚，证据充分，足以认定。|' \
                             r'以上事实，有相关公告、《评估报告》、相关协议、评估工作底稿、汽车厂商出具的相关说明、保千里电子业务员出具的相关说明、相关人员询问笔录、支付凭证等证据证明，足以认定。|' \
                             r'虞凌云的上述行为，违反了《证券法》第七十三条、第七十六条第一款的规定，构成第二百零二条所述内幕交易行为。|' \
                             r'我会认为，刘长鸿、冯文渊使用其控制的账户组，操纵“南通锻压”、“北京旅游”等两只股票|' \
                             r'苏彩龙在知悉内幕信息且该内幕信息未公开的情况下，建议梁准买入“和佳股份”，而梁准在明知苏彩龙和佳股份董秘身份的情况下，' \
                             r'在内幕信息公开前、与苏彩龙联络之后交易了“和佳股份”。|' \
                             r'朱继华上述交易“德豪润达”股票的行为违反了《证券法》第七十三条、七十六条的规定，构成《证券法》第二百零二条所述“内幕交易”的情形。|' \
                             r'张彦上述交易“德豪润达”股票的行为违反了《证券法》第七十三条、七十六条的规定|' \
                             r'吴建敏使用“苏某某”账户交易红太阳股票的行为违反了《证券法》第四十五条第二款“为上市公司出具审计报告|' \
                             r'徐东波的上述行为违反了《证券法》第七十六条的规定，构成《证券法》第二百零二条所述“内幕交易”的情形。|' \
                             r'李国东的上述行为违反了《证券法》第七十七条的规定，构成《证券法》第二百零三条所述“操纵证券市场”的情形。|' \
                             r'舒文胜、朱项平身为从业人员期间买卖股票的行为违反了《证券法》第四十三条的规定|' \
                             r'《补充协议》中有天目药业应收账款清收等内容，属于《上市公司信息披露管理办法》第三十条第二款第（三）项“公司订立重要合同|' \
                             r'我会认为，李德胜、丁彦森出租个人业务资格的行为违背了其作为执业人员应负的诚信义务|' \
                             r'国能集团\n2005\n年年度报告、\n2006\n年年度报告|' \
                             r'方向光电未按规定披露信息、所披露的信息存在虚假记载和重大遗漏的行为|' \
                             r'万基集团利用他人账户买卖证券的行为，违反了《证券法》第八十条的规定|' \
                             r'按照《证券法》第六十七条关于“发生可能对上市公司股票交易价格产生较大影响的重大事件|' \
                             r'韩偲铭编造、传播虚假信息的行为|' \
                             r'当事人劲嘉股份及其代理人在提交的书面申辩材料中及在听证会上提出|' \
                             r'在听证会上，海纳通投资有限公司崔某某作为证人提供证言称|' \
                             r'根据当事人违法行为的事实、性质、情节与社会危害程度，依据《证券法》第一百九十三条的规定，我会决定|' \
                             r'叶志刚在陈述、申辩意见中提出|' \
                             r'岳远斌提出了自己不知悉、未利用内幕信息的辩解|' \
                             r'对于上述违法情况，对照|' \
                             r'林文清未履行相关披露义务的行为|' \
                             r'经过调查与审理，我会认定|' \
                             r'我会认为，华寅所及其注册会计师刘文俊|' \
                             r'我会认为，“中农化重组大成股份”、“中农资重组大成股份”这两个事项所涉及的情况|' \
                             r'我会认为，永华所及其执业人员，未能勤勉尽责|' \
                             r'我会认为，南京中北2003年、2004年的年报披露行为|' \
                             r'综合上述情况，审理认为，佘鑫麒身为上市公司董事、总经理，无视法律的多项禁止性规定|' \
                             r'我会认为，精工科技研制成功结晶炉一事，是对其股票交易价格有显著影响的重要信息|' \
                             r'我会认为，ST黄海申请免除亚洲开发银行青岛轮胎开发项目贷款中青岛市财政局代公司偿还的人民币债务|' \
                             r'我会认为，捷利股份收购辽宁中期这一事项|' \
                             r'根据《暂行条例》第五十九条和第六十条的规定，我会决定|' \
                             r'广东证券及其相关人员的上述行为，违反了原《证券法》第二十四条关于“证券公司承销证券|' \
                             r'富成证券的上述行为违反了原《证券法》第一百三十条|' \
                             r'中科健的上述行为违反了《中华人民共和国证券法》|' \
                             r'根据《证券法》第二百零一条“证券公司在证券交易中有严重违法行为|' \
                             r'对上述行为负有直接责任的是在天一科技2003年审计报告上签字的注册会计师李海来、唐爱清。|' \
                             r'科大创新2002年度虚增销售收入、利润以及以账外资金处理管理费用和股票发行费用|' \
                             r'璐通期货未经客户委托和授权，代理客户进行交易的行为违反了|' \
                             r'(以上|上述)各项证据充分、确实，足以认定当事人各项(违法|违规)行为|' \
                             r'我会认为，原海南港澳国际信托公司沈阳证券营业部的上述行为违反了《中华人民共和国证券法》|' \
                             r'本会认为，华信会计师所上述行为违反了《股票发行与交易管理暂行条例》第三十五条的规定|' \
                             r'当事人以上违法行为有相关年报、中报、相关董事会决议、相关担保合同、有关信息披露文件、相关谈话笔录等主要证据在案佐证。各项证据充分、确凿，足以认定当事人上述违法行为。|' \
                             r'证实当事人(以上|上述|此项)违法行为的主要证据有|' \
                             r'以上各项证据充分、确实，足以认定当事人上述.*?违法行为。|' \
                             r'本会认为.*?上述行为违反了《证券法》第六十一条|' \
                             r'证明上述违法事实的证据材料|证实此项事实的主要证据有|' \
                             r'上述证据充分、确实，足以认定当事人各项违规事实。|' \
                             r'三九医药的上述行为，违反了《中华人民共和国证券法》（以下简称“《证券法》”）第一百七十七条的规定|' \
                             r'上述问题.*?不符合.*?第.*?条.*?规定.*?作为.*?对上述问题负有主要责任。|' \
                             r'二、处罚决定?|' \
                             r'根据当事人的违法事实、性质、情节与社会危害程度|' \
                             r'曾国波的上述行为，违反了《证券法》第七十六条的规定，|' \
                             r'以上事实，有涉案人员询问笔录，涉案人员三方存管银行账户资金往来记录，涉案账户委托下单\nIP\n、\nMAC\n记录，' \
                             r'涉案账户交易记录以及相关账户借用配资协议等证据证明，足以认定。|' \
                             r'以上违法事实，有宏磊股份《\n2012\n年半年度报告》、《\n2012\n年年度报告》，宏磊股份的会计记录，相关董事会决议，相关人员的谈话笔录等证据证明\n，足以认定。|' \
                             r'以上事实，有交易流水、当事人询问笔录、电脑\nIP\n、\nMAC\n取证信息等证据证明，足以认定。|' \
                             r'以上违法事实，有账户开户、交易、资金流水记录及相关银行凭证，委托下单\nIP\n地址，下单电脑硬盘序列号记录，相关协议，询问笔录等证据证明，足以认定。|' \
                             r'以上违法事实，有科伦药业披露的相关临时信息，科伦药业《\n2010\n年年度报告》和《\n2011\n年年度报告》\n，' \
                             r'相关工商登记资料，相关会议记录，相关董事会决议，相关会计记录，相关人员的谈话笔录等证据证明，足以认定。|' \
                             r'上述事实有重组相关协议、公告、询问笔录、证券账户开户及交易资料、资金凭证、\nMAC\n地址等证据证实，足以认定。|' \
                             r'以上违法事实，有南纺股份\n2006\n年、\n2007\n年、\n2008\n年、\n2009\n年、\n2010\n年年度报告，南纺股份的会计记录，' \
                             r'相关部门提供的书证，相关董事会决议，相关人员的谈话笔录等证据证明\n，足以认定。|' \
                             r'以上违法事实，有袁郑健交易中茵股份股票所使用的\nIP\n地址、袁郑健交易\n中茵股份股票的资金往来、相关博客文章发表的\nIP\n地址\n及阅读情况、' \
                             r'袁郑健交易中茵股份股票的数据、中茵股份股票交易价格变化情况等证据证明，足以认定。|' \
                             r'以上违法事实，有贤成矿业\n2009\n年半年度报告、\n2009\n年年度报告、\n2010\n年半年度报告、\n2010\n年年度报告、\n2011' \
                             r'\n年半年度报告、\n2011\n年年度报告、\n2012\n年半年度报告，贤成矿业临时公告，贤成矿业的会计凭证，相关银行的资金划转凭证，' \
                             r'相关担保合同，相关司法文书，相关机构的说明，相关人员的询问笔录等证据证明\n，足以认定。)'
            truth_compiler = re.compile(truth_text_str, re.MULTILINE)
            truth_list = truth_compiler.findall(content_text)
            truth = '\n'.join([kk[0] for kk in truth_list])

            punishment_decision = re.search(
                r'(((根据|属于|基于|考虑|按照|依照|鉴于).*?(依据|根据|依照|按照).*?第.*?[条之].*?(我会|中国证监会、财政部|本会)(决定|对.*?处以|作出|对.*?作出|拟决定)|'
                r'根据当事人违法行为的事实、性质、情节[和与]社会危害程度，(我会|中国证监会、财政部|本会)(决定|对.*?处以|作出|对.*?作出)|'
                r'(经研究，)?(依据|根据|依照|按照).*?第.*?[条之].*?(我会|中国证监会、财政部|本会)(决定|对.*?处以|作出|对.*?作出)|'
                r'根据当事人违法行为的事实、性质、情节[和与]社会危害程度.*?(依据|根据|依照|按照).*?第?.*?[条之].*?(我会|中国证监会、财政部|本会)(决定|对.*?处以|作出|对.*?作出)|'
                r'根据当事人违法行为的事实、性质、情节与社会危害程度，以及宏磊股份\n2013\n年上半年收回了宏磊集团占用的全部资金\n508,715,650.54\n元，'
                r'并向宏磊集团收取了资金占用费\n25,414,971.69\n元的情况，依据《证券法》第一百九十三条和《中华人民共和国行政处罚法》\n第二十七条从轻和减轻行政处罚的相关规定，我会决定：|'
                r'综上，我会决定|'
                r'鉴于上述情况，同时考虑到潘海深在交易行为发生后曾主动向上海证券交易所报告、案发后能够积极配合监管部门调查，而且内幕交易违法所得金额不大等情节，我会决定|'
                r'根据.*?条.*?规定.*?经研究决定|'
                r'根据上述法律规定关于“吊销责任人员的从业资格证书”罚则，经研究决定|'
                r'根据《证券法》第二百零一条“证券公司在证券交易中有严重违法行为，不再具备经营资格的，由证券监督管理机构取消其证券业务许可，并责令关闭”的规定，取消|'
                r'基于以上(违法)事实，本会.*?决定|据此，根据上述法律规定，经研究决定|'
                r'经研究决定，对.*?处以|根据上述法律规定，经研究，对.*?处以|'
                r'根据上述法律规定，经研究决定：责令|根据.*?第.*?条规定，对当事人作出如下处罚决定：|'
                r'根据上述法律规定，经研究决定|根据.*?第.*?条规定，对各当事人作出如下处罚决定|'
                r'根据.*?第.*?条的规定，对.*?处以|经研究决定，根据《办法》第34条的规定，撤销|'
                r'根据《股票发行与交易管理暂行条例》第七十四条第\(二\)项及《证券法》第一百七十七条的规定，决定|'
                r'根据上述法律规定，本会决定|据此，根据《股票条例》第七十一条第二款的规定，对平安乌鲁木齐营业部处以|'
                r'根据《证券法》第201条“证券公司在证券交易中有严重违法行为，不再具备经营资格的，由证券监督管理机构取消其证券业务许可，并责令关闭”的规定，取消大连证券的证券业务许可，并责令其关闭。|'
                r'根据《期货条例》第五十九条规定之规定，对当事人作出如下处罚决定|'
                r'根据《股票条例》第七十三条，对当事人作出如下处罚决定|'
                r'根据《证券法》第一百七十七条第一款之规定，对当事人作出如下决定|'
                r'二、处罚决定|二、处罚决|'
                r'基于以上事实，本会决定向)'
                r'[\s\S]*?)\n'
                r'(.*?应自收到本处罚决定书之日|(当事人)?.*?[如若](果)?对本(处罚)?决定不服)', content_text).group(1).strip()

            if '申辩' in content_text:
                defense_text_str = r'((\n[^。\n]*?(听证|申辩|辩称|提出)|应有关当事人要求，我会于2007年11月2日召开听证会，听取了当事人的陈述与申辩，|' \
                                   r'应有关当事人要求，我会于2007年3月21日召开听证会，听取了当事人的陈述与申辩，|' \
                                   r'听取了当事人的陈述和申辩意见，|应当事人的申请，召开了听证会，听取了其陈述和申辩，|' \
                                   r'应部分当事人的要求举行了听证会，听取了当事人的陈述、申辩意见，|' \
                                   r'部分当事人提交了陈述和申辩意见，我们进行了审查。应当事人宫云科的申请，我们依法举行了听证，听取了其陈述和申辩。|' \
                                   r'[^。，\n]*?(未|不)?(提出|要求|提交|进行)?陈述[^。，\n]*?申辩[^。\n]*?(未|不|并)(要求|申请)[^。\n]*?听证|' \
                                   r'当事人未要求听证，(也未|但)(提交|提出了?)(陈述)?[^。，\n]*?申辩意见。|当事人未提交陈述和申辩意见。|' \
                                   r'当事人[^。，\n]*?未(要求|申请)陈述[^。，\n]*?申辩和听证|当事人不要求陈述、申辩和听证。|' \
                                   r'[^。，\n]*?在听证及陈述申辩中提出|' \
                                   r'[^。，\n]*?未提出陈述、申辩意见。|[^。，\n]*?未(提交|提出)陈述、申辩意见及听证申请。|' \
                                   r'应当事人宁波富邦等要求，我会依法举行了听证会，听取了当事人及其代理人的陈述和申辩。|' \
                                   r'应当事人申请，我会举行了听证会，听取并复核了当事人的陈述与申辩意见，|时任独立董事夏建林在陈述申辩中提出其在审议资产置换事项的董事会上|' \
                                   r'当事人提交了陈述和申辩意见，我们进行了审查。应当事人申请，我们依法举行了听证会。|' \
                                   r'应当事人惠顺装饰的要求，我会举行了听证会，听取了惠顺装饰的陈述申辩。|' \
                                   r'并依法向各当事人事先告知了具体违法事项，听取了有关当事人的陈述申辩。|' \
                                   r'公告期满[^。\n]*?未提出陈述申辩|当事人未申请听证，也未提交陈述和申辩意见。|' \
                                   r'当事人没有提出听证要求，也没有提出陈述申辩意见。|张野在个人陈述材料中提出|' \
                                   r'当事人[^。，\n]*?提出|唐建平的代理人在听证会上提出|李际滨提出自己的交易行为没有利用内幕信息|' \
                                   r'我会应赵伟平的要求举行了听证会，听取了赵伟平的陈述和申辩。广州攀达放弃听证。|' \
                                   r'应当事人的申请，行政处罚委员会于2009年1月12日举行听证会，听取了当事人的陈述和申辩。听证会后，行政处罚委员会进行了复核。|' \
                                   r'周良超未按要求在公告发布60日内领取《行政处罚事先告知书》，逾期未领取的，视为送达，同时，视为放弃陈述申辩及听证的权利。|' \
                                   r'根据当事人的申请，我会2018年3月6日召开听证会，听证会当日，当事人无故缺席也未委托代理人出席，亦未提交陈述申辩意见。|' \
                                   r'公告期满，李晓明未在规定的时间内领取告知书，亦未提出陈述申辩或要求听证。|' \
                                   r'嘉宇实业在申辩意见中提出其在案件调查期间积极配合，主动提交相应材料，并在事后积极整改，请求减轻处罚。|' \
                                   r'听证会上，当事人对《事先告知书》认定的事实予以承认，但认为该事实不构成未勤勉尽责情形。|' \
                                   r'沃克森评估公司、李文军和黄立新在陈述申辩和听证过程中，提出了如下陈述和申辩意见：|' \
                                   r'当事人吕美庆及其代理人在陈述、申辩材料及听证中否认吕美庆操控涉案账户组交易股票操纵股价，请求对吕美庆免予处罚。|' \
                                   r'黄芝颢提出听证，2015年12月1日，我会组织听证，当事人未到场，亦未提出陈述、申辩意见。按照《中国证券监督管理委员会行政处罚听证规则》' \
                                   r'（证监法律字〔2007〕8号）第九条第二款规定，当事人未按期参加听证的，视为放弃听证权利。|' \
                                   r'当事人在《事先告知书回执》中要求陈述申辩和举行听证会，2017年2月28日，当事人向我会递交《放弃听证及申辩意见》等材料，' \
                                   r'表示放弃听证。截至3月22日，其陆续向我会递交了《呈递证监会材料》《鲜言关于放弃行政诉讼权利声明书》《法律自首申请书》及《积极交纳罚款的承诺书》等材料。)' \
                                   r'([\s\S]*?))' \
                                   r'((经复核，)?(本案)?现已调查、审理(并复核)?终结。|' \
                                   r'\n[^。\n]*?我会.*?(认为|认定)|' \
                                   r'\n经复核|\n经核实|经复核，我会认为|经复核，根据|经核实|' \
                                   r'对于上述申辩意见，我会在审理与复核过程中已经充分考虑。|' \
                                   r'我会认为，评估师在实际执业过程中|根据现有证据，我会对[^。\n]*?申辩意见不予采纳。|' \
                                   r'我会复核认为，本案客观证据|' \
                                   r'我会认定王明华知悉内幕信息|' \
                                   r'因此，我会对于天澄门在绿大地欺诈发行上市时未勤勉尽责|' \
                                   r'我会认为，\nA381XXX355\n等\n19\n个证券账户网上委托交易的地址具有关联性|' \
                                   r'我会认为，由于利安达对华阳科技|' \
                                   r'我会认为，2011年5月4日|' \
                                   r'经我会核查|' \
                                   r'我会认为，袁郑健采取连续交易|' \
                                   r'我会认为，霍峰时任|' \
                                   r'我会认为，根据《证券法》的规定|' \
                                   r'我会认为，投资者从事证券|' \
                                   r'我会认为，根据|' \
                                   r'我会认为，《证券法》设定|' \
                                   r'我会认为，第一|' \
                                   r'我会认为，安泰期货在听证中提供的证据材料|' \
                                   r'基于前述查明的情况|' \
                                   r'我会认为，瞿湘的辩解|' \
                                   r'经查明，大业期货存在以下违法行为：|' \
                                   r'经查明，格林期货存在以下违法行为：|' \
                                   r'经审理|' \
                                   r'经查明，张家界公司信息披露存在如下违法行为|' \
                                   r'我会复核意见如下)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense_list = [kk for kk in defense_compiler.findall(content_text) if '我会同时认为' not in kk[0]]
                if len(defense_list) != 0:
                    defense = defense_list[-1][0].strip()
                    defense_response_str = defense.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                               .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                               .replace(r'+', r'\+') + \
                                           r'([\s\S]*?)' \
                                           + r'((经复核，)?本案现已调查、审理(并复核)?终结。|' \
                                           + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                               .replace(r'.', r'\.') \
                                               .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                               .replace(r'+', r'\+') \
                                           + ')'

                    defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                    if defense_response_compiler.search(content_text):
                        defense_response = defense_response_compiler.search(content_text).group(1).strip()
                    else:
                        defense_response = ''
                else:
                    defense = re.search(r'(上述当事人均未向我会提交陈述申辩意见或者要求听证。|'
                                        r'2006年9月21日召开听证会，听取了当事人的陈述申辩。|'
                                        r'2006年7月26日召开听证会，听取了当事人的陈述申辩。|'
                                        r'我会于2006年10月20日召开听证会，听取了当事人的陈述与申辩。|'
                                        r'于2006年4月29日召开了听证会，听取了当事人的陈述申辩。|'
                                        r'当事人没有陈述申辩，也没有要求听证。|'
                                        r'我会于2005年10月9日召开听证会，听取了当事人的陈述与申辩。|'
                                        r'应相关当事人要求，召开了听证会，听取了当事人的陈述与申辩。|'
                                        r'应.*?当事人.*?要求，我会于.*?听证.*?听取了当事人.*?的陈述与申辩。|'
                                        r'根据当事人的申请，召开了听证会，听取了其陈述和申辩。'
                                        r'我会依法向当事人告知了作出行政处罚的事实、理由及依据，并对当事人的陈述、申辩意见进行了复核。|'
                                        r'根据当事人陈宗海和马贤明的申请，召开了听证会，听取了其陈述和申辩。|'
                                        r'当事人未提出陈述申辩意见和听证要求。|'
                                        r'应当事人要求，我会于2004年10月22日依法举行听证，听取了当事人及其代理人的陈述与申辩。|'
                                        r'根据当事人.*?申请，召开了听证会，听取了其陈述和申辩。|'
                                        r'当事人.*?要求听证，我会.*?听证会，听取了.*?申辩和陈述意见。|'
                                        r'应当事人的要求举行了听证会，听取了当事人的陈述和申辩意见。|'
                                        r'根据.*?申请召开了听证会，听取了.*?陈述和申辩。|'
                                        r'举行了听证会，听取了当事人的陈述申辩。|'
                                        r'举行了听证会,听取了当事人的陈述申辩。|'
                                        r'其中当事人周家银要求听证，我部于2004年5月18日组织了对周家银的听证会，听取了周家银的申辩和陈述意见。|'
                                        r'依法履行了事先告知程序，听取了当事人的陈述和申辩。|'
                                        r'本会已调查终结，并听取了当事人的陈述与申辩。|'
                                        r'当事人未要求举行听证，也未进行陈述与申辩。|'
                                        r'现已调查终结，并于2004年3月4日依法举行听证，听取了有关当事人及其代理人的陈述及申辩。|'
                                        r'经研究，我会认为，宁城老窖提出的申辩意见事实和理由不成立，不予采纳。|'
                                        r'各当事人均未要求听证。当事人冯勇和梁伟斌向我会提出了书面陈述、申辩意见，但申辩理由不充分，本会不予采纳。|'
                                        r'于2003年10月24日依法举行听证，听取了当事人及其代理人的陈述和申辩。|'
                                        r'应各当事人要求，我会于2003年6月25日依法举行听证，听取了联合证券及其他当事人及其代理人的陈述与申辩。|'
                                        r'于.*?举行听.*?听取了.*?陈述与申辩。|'
                                        r'当事人未在法定期限内提出听证、陈述和申辩的要求。|'
                                        r'案件处理过程中听取了当事人的陈述及申辩。|'
                                        r'本会2002年7月30日依法举行了听证会，听取了尹文健及其代理人的陈述与申辩。|'
                                        r'听取了当事人的陈述及申辩。|'
                                        r'应有关当事人申请，我会于2005年12月13日召开听证会，听取了当事人的陈述与申辩。|'
                                        r'我会依法向当事人告知了作出行政处罚的事实、理由及依据，并对当事人的陈述、申辩意见进行了复核。|'
                                        r'于2002年7月30日举行听证会，听取了当事人及其代理人的陈述和申辩。)', content_text).group(1).strip()
                    defense_response = ''
            else:
                defense = defense_response = ''

            punishment_basis_text_str = truth_list[-1][0].replace(r'(', r'\(').replace(r')', r'\)') \
                                            .replace(r'.', r'\.') \
                                            .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                            .replace(r'+', r'\+') \
                                        + r'([\s\S]*?)' \
                                        + r'(' + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                            .replace(r'.', r'\.') \
                                            .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                            .replace(r'+', r'\+') \
                                        + r'|' + defense.replace(r'(', r'\(').replace(r')', r'\)') \
                                            .replace(r'.', r'\.') \
                                            .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                            .replace(r'+', r'\+') \
                                        + r')'
            punishment_basis_compiler = re.compile(punishment_basis_text_str, re.MULTILINE)
            punishment_basis_list = punishment_basis_compiler.findall(content_text)
            if len(punishment_basis_list) > 0:
                punishment_basis = punishment_basis_list[-1][0]
                punishment_basis = re.sub('((上述|以上)(违法(违规)?)?(事实|情况|行为).*?等?[^。，\n]*?(证据|佐证).*?。|'
                                          '陈述等证据证明，事实清楚，证据充分，足以认定。|'
                                          '上述违法事实，有.*?谈话笔录等。|'
                                          '上述事实有.*?等在案作证。)', '', punishment_basis).strip()
            else:
                punishment_basis = ''

            result_map = {
                'announcementTitle': title,
                'announcementOrg': org,
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
                logger.info('证监会 数据解析 ' + doc_type + ' -- 数据导入完成')
            else:
                logger.info('证监会 数据解析 ' + doc_type + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('证监会 数据解析 ' + doc_type + ' -- 修改parsed完成')
        elif doc_type == '责令整改通知':
            real_publish_date = format_date(head_info.find_all('tr')[2].find_all('td')[2].text.split(':')[1].strip())
            document_code = re.search(r'(证监责改字.\d{4}.\d+号)', content_text).group(1).strip()
            litigant = re.search(document_code.replace(r'[', r'\[').replace(r']', r'\]')
                                 + r'\n([\s\S]*?)：', content_text).group(1).strip()

            truth_text_str = litigant + '：' + r'([\s\S]*?)' \
                                              r'(对此，我会将依据有关规定对你公司进行处罚。|' \
                                              r'依据|经.*?审核.*?及约见公司董事长谈话，我会认为|' \
                                              r'你行上述行为严重违反|' \
                                              r'你公司以个人名义开立账户买卖证券的行为，违反了|' \
                                              r'你公司的上述行为违反了《中华人民共和国证券法》第七十四条的规定|' \
                                              r'现决定)'
            truth_compiler = re.compile(truth_text_str, re.MULTILINE)
            truth_list = truth_compiler.findall(content_text)
            truth = '\n'.join([kk[0] for kk in truth_list])

            punishment_decision = re.search(r'(([^。\n]*?(根据|依据).*?规定.*?(责令|作出了行政处罚决定)|[^。\n]*?现责令|'
                                            r'经研究决定，责令|对此，我会将依据有关规定对你公司进行处罚。|现决定)'
                                            r'[\s\S]*?)'
                                            r'(中国证券监督管理委员会|$)', content_text).group(1).strip()

            defense = defense_response = ''

            punishment_basis_text_str = truth_list[-1][0].replace(r'(', r'\(').replace(r')', r'\)') \
                                            .replace(r'.', r'\.') \
                                            .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                            .replace(r'+', r'\+') \
                                        + r'([\s\S]*?)' \
                                        + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                            .replace(r'.', r'\.') \
                                            .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                            .replace(r'+', r'\+')

            punishment_basis_compiler = re.compile(punishment_basis_text_str, re.MULTILINE)
            punishment_basis = punishment_basis_compiler.search(content_text).group(1).strip()

            result_map = {
                'announcementTitle': title,
                'announcementOrg': org,
                'announcementDate': real_publish_date,
                'announcementCode': document_code,
                'facts': truth,
                'defenseOpinion': defense,
                'defenseResponse': defense_response,
                'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                'punishmentBasement': punishment_basis,
                'punishmentDecision': punishment_decision,
                'type': '责令整改通知',
                'oss_file_id': file_id,
                'status': 'not checked'
            }
            logger.info(result_map)
            if db.announcement.find({'announcementTitle': title, 'oss_file_id': file_id}).count() == 0:
                db.announcement.insert_one(result_map)
                logger.info('证监会 数据解析 ' + doc_type + ' -- 数据导入完成')
            else:
                logger.info('证监会 数据解析 ' + doc_type + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('证监会 数据解析 ' + doc_type + ' -- 修改parsed完成')
        elif doc_type == '市场禁入决定':
            real_publish_date = format_date(head_info.find_all('tr')[2].find_all('td')[2].text.split(':')[1].strip())
            document_code = re.search(
                r'(\n(证监罚字|证监禁入字|证监法律字)?.\d{3,4}..?\d+.?号)\n',
                content_text).group(1).strip()
            litigant = re.search(document_code.replace(r'[', r'\[').replace(r']', r'\]')
                                 + r'\n([\s\S]*?)'
                                   r'(\n依据|\n经查|\n.*?一案.*?调查|\n.*?案.*调查)', content_text).group(1).strip()
            truth_text_str = r'((.*?经查|二、|三、|四、|（二）|五、|（二）|司法机关(依法)?认定)[\s\S]*?)' \
                             r'(\n(上述|以上)(违法(违规)?)?(事实|情况|行为)[\s\S]*?等?[^。，\n]*?(证据|佐证|谈话笔录等|作证|询问笔录|相关年报)|' \
                             r'以上违法事实，有.*?刑事判决书证明，足以认定。|' \
                             r'以上事实，有五洋建设发布的公告清单等证据证明，足以认定。|' \
                             r'上述事实，有相关当事人的工商档案登记材料、证券营业部出具的有关账户开户资料、授权委托书、股票交易清单、' \
                             r'资金存取凭证、上海证券交易所出具的有关证明材料等证据在案证实，证据确实、充分，足以认定。|' \
                             r'根据上述事实与证据，经审理，作出以下认定|' \
                             r'以上违法事实，有账户开户、交易、资金流水记录及相关银行凭证，委托下单\nIP\n地址，下单电脑硬盘序列号记录，相关协议，询问笔录等证据证明，足以认定。|' \
                             r'以上违法事实，有南纺股份\n2006\n年、\n2007\n年、\n2008\n年、\n2009\n年、\n2010\n年年度报告，' \
                             r'南纺股份的会计记录，相关部门提供的书证，相关董事会决议，相关人员的谈话笔录等证据证明\n，足以认定。|' \
                             r'国能集团2005\n年年度报告、\n2006\n年年度报告、\n2007\n年年度报告和\n2008\n年年度报告及\n国能集团2006\n年|' \
                             r'在听证会上，海纳通投资有限公司崔某某作为证人提供证言称|' \
                             r'冯久田、袁金亮、吴玉瑞、田玉新是对鲁北化工多项信息披露违法行为直接负责的主管人员。|' \
                             r'叶志刚在陈述申辩意见中提出|' \
                             r'岳远斌提出了自己不知悉、未利用内幕信息的辩解|' \
                             r'同时，我会还关注到，沧州化工信息披露违法行为持续时间长，违法次数多，涉及金额特别巨大，情节严重，应当对相关责任人员予以从重处罚。|' \
                             r'我会还进一步查明了银河科技有关董事、监事、高级管理人员的涉案情况|' \
                             r'我会认为，根据上述第一项至第十项事实|' \
                             r'我会认为，上海科技在2004年年报中未披露重大银行借款与应付票据事项|' \
                             r'我会认为，南京中北2003年、2004年的年报披露行为，违反了原《证券法》第六十一条的规定|' \
                             r'陈克根是昌源投资的实际控制人之一|' \
                             r'证明以上违法事实的证据有|' \
                             r'时任副董事长、董事长黄先锋|' \
                             r'我会认为，王艳平、毛义等人的上述行为违反了原《证券法》第七十一条的规定|' \
                             r'我会认为，董宗祺、何平等人的上述行为违反了原《证券法》第七十一条的规定|' \
                             r'我会认为，辛乃奇、刘军等人的上述行为违反了原《证券法》第七十一条的规定|' \
                             r'郝一平、朱格利等人的上述行为违反了原《中华人民共和国证券法》第七十一条的规定|' \
                             r'徐水师、卫建华等人的上述行为违反了原《中华人民共和国证券法》第七十一条的规定|' \
                             r'富成证券的上述行为违反了原《证券法》第一百三十条|' \
                             r'我会认定上述事实的主要证据有|' \
                             r'高建华、高建民所控制的上述3家公司以个人名义开立账户买卖“ST宏峰”股票的行为违反了原《中华人民共和国证券法》|' \
                             r'刘大力是民安证券违反证券法律法规及涉嫌犯罪行为的主要决策人及组织者|' \
                             r'上述行为违反《证券法》第一百三十八条“证券公司办理经纪业务|' \
                             r'孙成刚的行为违反了《证券、期货投资咨询管理暂行办法》第二十四条|' \
                             r'宋如华作为托普软件的时任董事长，对托普软件的违法行为负有不可推卸的法律责任|' \
                             r'天歌科技上述信息披露虚假的行为违反了《中华人民共和国证券法》|' \
                             r'达尔曼的上述行为违反了《证券法》第五十九条|' \
                             r'上述行为分别违反了《股票发行和交易管理暂行条例》|' \
                             r'天发股份的上述行为违反了《证券法》第五十九条|' \
                             r'陈克根、陈克恩作为福建省神龙企业集团有限公司（神龙发展的控股股东，以下简称“神龙集团”）仅有的两名股东|' \
                             r'雷立军上述行为违反了《证券、期货投资咨询管理暂行办法》第三条之规定|' \
                             r'同时，周林辉骗取并挪用该营业部客户账户上的资金后携款潜逃，至今未归案|' \
                             r'上述问题.*?不符合.*?第.*?条.*?规定.*?作为.*?对上述问题负有主要责任。|' \
                             r'在公司对外签署了大量担保协议后，公司未及时向公众投资者披露相关信息，使投资者无法了解公司经营风险)'
            truth_compiler = re.compile(truth_text_str, re.MULTILINE)
            truth_list = truth_compiler.findall(content_text)
            truth = '\n'.join([kk[0] for kk in truth_list])

            punishment_decision = re.search(
                r'((\n.*?(根据|属于|按照|依照|鉴于|依据).*?(我会决定|对郭建兴采取|(我会|本会)认定.*?市场禁入|本会决定|认定刘敏为市场禁入者|'
                r'经研究决定|对张晓伟、黄玉麟实施永久性市场禁入|决定认定其为市场禁入者|我会决定对许宗林、高芳实施永久性市场禁入))'
                r'[\s\S]*?)\n'
                r'(.*?应自收到本处罚决定书之日|(当事人)?.*?[如若](果)?对本(市场禁入|处罚|决定)?决定不服|'
                r'中国证券监督管理委员会|二○○六年二月二十二日|二○○五年五月十七日|二○○四年八月九日)', content_text).group(1).strip()

            if '申辩' in content_text:
                defense_text_str = r'(((\n|。).*?(申辩|听证).*?(提出|认为|称|陈述)|' \
                                   r'(\n|。)[^。\n]*?(提出|要求|陈述).*?(申辩|辩解)|(\n|。).*?辩称|' \
                                   r'当事人未提交陈述、申辩意见，也未申请听证。|' \
                                   r'李友收到我会《行政处罚事先告知书》和《市场禁入事先告知书》后，委托代理人参加了听证会，并向我会递交了陈述申辩材料|' \
                                   r'余丽收到我会《行政处罚事先告知书》和《市场禁入事先告知书》后，向我会递交了相关的陈述申辩材料。|' \
                                   r'上述当事人未提交陈述和申辩意见，也未要求听证。|' \
                                   r'鲜言在提交我会的材料中提出|当事人提出，|' \
                                   r'[^。，\n]*?在听证及陈述申辩中提出|' \
                                   r'针对涉案违法事实，曹春华认为我会认定事实不够准确|' \
                                   r'当事人周山、李纪、王海棠在陈述、申辩材料中对涉案违法行为造成的不良影响表示歉意|' \
                                   r'在听证会上，银河证券屈某作为证人提供证言称|' \
                                   r'应当事人冯斌的申请，我会举行了听证会，听取了冯斌的陈述和申辩。)' \
                                   r'([\s\S]*?))' \
                                   r'(经复核|本案现已调查、审理终结。|经复核，本案现已调查、审理终结。|现已调查、审理终结。|' \
                                   r'我会复核认为|' \
                                   r'\n我会认为|' \
                                   r'针对.*?申辩意见，基于下述事实和理由，我会|' \
                                   r'针对关于违法事实的申辩意见，经复核，我会意见如下|' \
                                   r'根据何学葵违法行为的严重性|对此我会予以部分采纳|' \
                                   r'我会对部分意见予以采纳。|' \
                                   r'因此，我会对|' \
                                   r'经审理.*?申辩理由不成立|' \
                                   r'我会认为，作为上市公司的董事|' \
                                   r'综上，|' \
                                   r'我会认为，张钢、王邦志虽然对中科证券违法行为的历史形成负有责任|' \
                                   r'我会复核意见如下)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense_list = [kk for kk in defense_compiler.findall(content_text) if '我会同时认为' not in kk[0]]
                if len(defense_list) != 0:
                    defense = defense_list[-1][0].strip()
                    defense_response_str = defense.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                               .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                               .replace(r'+', r'\+') + \
                                           r'([\s\S]*?)' \
                                           + r'((经复核，)?本案现已调查、审理(并复核)?终结。|现已调查、审理终结。|' \
                                           + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                               .replace(r'.', r'\.') \
                                               .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                               .replace(r'+', r'\+') \
                                           + ')'

                    defense_response_compiler = re.compile(defense_response_str, re.MULTILINE)
                    defense_response = defense_response_compiler.search(content_text).group(1).strip()
                else:
                    defense = re.search(r'(严芳、卞明二人未提出陈述、申辩意见，也未要求听证。|'
                                        r'毕杰善在公告送达期限内既未提出陈述、申辩意见，也未要求听证。|'
                                        r'应当事人申请，我会举行了听证会，听取并复核了当事人的陈述与申辩意见，现已调查、审理终结。|'
                                        r'复核了当事人的陈述、申辩意见，现已调查、审理终结。|'
                                        r'应当事人申请，我会举行了听证会，听取了当事人及其代理人的陈述与申辩意见，现已调查、审理终结。|'
                                        r'复核了当事人的陈述与申辩意见，现已调查、审理终结。|'
                                        r'于2006年9月21日召开听证会，听取了当事人的陈述申辩。|'
                                        r'我会已经向陈克根、陈克恩送达了行政处罚及市场禁入事先告知书，并应其要求举行了听证会，听取了他们的陈述和申辩意见。|'
                                        r'我会已经向周林辉公告送达了《行政处罚及市场禁入事先告知公告书》，其未向我会提出陈述、申辩意见，也未要求听证。|'
                                        r'我会已经向艾克拉木\?艾沙由夫公告送达了行政处罚及市场禁入事先告知书，其未在法定期限内向我会提出陈述、申辩意见，也未要求听证。)',
                                        content_text).group(1).strip()
                    defense_response = ''
            else:
                defense = defense_response = ''

            if defense != '':
                punishment_basis_text_str = truth_list[-1][0].replace(r'(', r'\(').replace(r')', r'\)') \
                                                .replace(r'.', r'\.') \
                                                .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                .replace(r'+', r'\+') \
                                            + r'([\s\S]*?)' \
                                            + r'(' + defense.replace(r'(', r'\(').replace(r')', r'\)') \
                                                .replace(r'.', r'\.') \
                                                .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                .replace(r'+', r'\+') \
                                            + r'|' \
                                            + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                                .replace(r'.', r'\.') \
                                                .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                .replace(r'+', r'\+') \
                                            + r')'
            else:
                punishment_basis_text_str = truth_list[-1][0].replace(r'(', r'\(').replace(r')', r'\)') \
                                                .replace(r'.', r'\.') \
                                                .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                .replace(r'+', r'\+') \
                                            + r'([\s\S]*?)' \
                                            + r'(' + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                                .replace(r'.', r'\.') \
                                                .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                .replace(r'+', r'\+') \
                                            + r')'
            punishment_basis_compiler = re.compile(punishment_basis_text_str, re.MULTILINE)
            if punishment_basis_compiler.search(content_text):
                punishment_basis = punishment_basis_compiler.search(content_text).group(1).strip()
                punishment_basis = re.sub(
                    r'((上述|以上)(违法(违规)?)?(事实|情况|行为)[\s\S]*?等?[^。，\n]*?(证据|佐证|谈话笔录等|作证|询问笔录|相关年报).*?。|'
                    r'以上违法事实，有.*?刑事判决书证明，足以认定。|'
                    r'以上违法事实，有账户开户、交易、资金流水记录及相关银行凭证，委托下单\nIP\n地址，下单电脑硬盘序列号记录，相关协议，询问笔录等证据证明，足以认定。|'
                    r'以上违法事实，有南纺股份\n2006\n年、\n2007\n年、\n2008\n年、\n2009\n年、\n2010\n年年度报告，'
                    r'南纺股份的会计记录，相关部门提供的书证，相关董事会决议，相关人员的谈话笔录等证据证明\n，足以认定。)',
                    '',
                    punishment_basis).strip()
            else:
                punishment_basis = ''

            result_map = {
                'announcementTitle': title,
                'announcementOrg': org,
                'announcementDate': real_publish_date,
                'announcementCode': document_code,
                'facts': truth,
                'defenseOpinion': defense,
                'defenseResponse': defense_response,
                'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                'punishmentBasement': punishment_basis,
                'punishmentDecision': punishment_decision,
                'type': '市场禁入决定',
                'oss_file_id': file_id,
                'status': 'not checked'
            }
            logger.info(result_map)
            if db.announcement.find(result_map).count() == 0:
                db.announcement.insert_one(result_map)
                logger.info('证监会 数据解析 ' + doc_type + ' -- 数据导入完成')
            else:
                logger.info('证监会 数据解析 ' + doc_type + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('证监会 数据解析 ' + doc_type + ' -- 修改parsed完成')
    else:
        content_text = get_content_text(content_soup.find(class_='content'))
        title = content_soup.find(class_='title').text

        if db.parsed_data.find({'origin_url': url, 'oss_file_origin_url': url}).count() == 0:
            oss_file_map = {
                'origin_url': url,
                'oss_file_origin_url': url,
                'origin_url_id': data_id,
                'oss_file_type': 'html',
                'oss_file_name': title,
                'oss_file_content': r.text.encode(r.encoding).decode('utf-8'),
                'parsed': False
            }
            response = db.parsed_data.insert_one(oss_file_map)
            file_id = response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + title + '.html',
                         r.text.encode(r.encoding).decode('utf-8'))
            db.csrc_data.update_one({'_id': data_id}, {'$set': {'status': 'parsed'}})
        else:
            db.csrc_data.update_one({'_id': data_id}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': url, 'oss_file_origin_url': url})['_id']

        result_map_list = []
        if db.punishAnnouncement.find({'url': url, 'announcementType': '要闻'}).count() == 0:
            return
        for each_punishment in db.punishAnnouncement.find({'url': url, 'announcementType': '要闻'}):
            result_map = {
                'announcementTitle': each_punishment['announcementTitle'],
                'announcementOrg': each_punishment['announcementOrg'],
                'announcementDate': each_punishment['announcementDate'],
                'announcementCode': each_punishment['announcementCode'],
                'facts': each_punishment['facts'],
                'defenseOpinion': each_punishment['defenseOpinion'],
                'defenseResponse': each_punishment['defenseResponse'],
                'litigant': each_punishment['litigant'],
                'punishmentBasement': each_punishment['punishmentBasement'],
                'punishmentDecision': each_punishment['punishmentDecision'],
                'type': '要闻',
                'oss_file_id': file_id,
                'status': 'checked'
            }
            result_map_list.append(result_map)
        logger.info('证监会 数据解析 ' + doc_type + ' -- 一共有%d条数据' % len(result_map_list))
        db.announcement.insert_many(result_map_list)
        logger.info('证监会 数据解析 ' + doc_type + ' -- 数据导入完成')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('证监会 数据解析 ' + doc_type + ' -- 修改parsed完成')
        db.punishAnnouncement.update_many({'url': url, 'announcementType': '要闻'}, {'$set': {'status': 'transfer'}})


def parse():
    for each_data in db.csrc_data.find({'origin': '证监会', 'status': {'$nin': ['ignored']},
                                        # 'url': 'http://www.csrc.gov.cn/pub/zjhpublic/G00306212/201901/t20190102_349109.htm'
                                        }).sort("_id", 1):
        if db.csrc_data.find({'url': each_data['url'], 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': each_data['url'], 'parsed': False}).count() == 0 and \
                db.parsed_data.find({'origin_url': each_data['url'], 'parsed': True}).count() > 0:
            continue
        parse_csrc(each_data['url'], each_data['type'], each_data['_id'], each_data['origin'])


if __name__ == "__main__":
    parse()
