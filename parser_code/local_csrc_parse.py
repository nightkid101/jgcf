import subprocess
import docx
import re
import os
import sys
from urllib.parse import urljoin

from pymongo import MongoClient
from init import logger_init, config_init
from utility import cn2dig, get_year, request_site_page, get_content_text, get_chinese_proportion
from oss_utils import init_ali_oss, oss_add_file
from pdf2html import pdf_to_text, pdf_ocr_to_text
from bs4 import BeautifulSoup as bs

logger = logger_init('地方证监局 数据解析')
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


# pdf
def pdf_parse(url, href, title, origin_data_id):
    pdf_link = urljoin(url, href)
    pdf_name = title.replace('.pdf', '').replace('.PDF', '')

    response = request_site_page(pdf_link)
    if response is None:
        logger.error('网页请求错误')
        return '', ''
    with open('./test/tmp.pdf', 'wb') as out_file:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                out_file.write(chunk)
    with open('./test/tmp.pdf', 'rb') as pdf_file:
        pdf_content = pdf_file.read()

    result_text = pdf_to_text('./test/tmp.pdf')
    if_ocr_flag = False
    logger.info('pdf to text: \n' + result_text)

    if get_chinese_proportion(result_text)[0] < 0.6 or get_chinese_proportion(result_text)[1] < 30:
        result_text, if_ocr_flag = pdf_ocr_to_text('./test/tmp.pdf')
        result_text = result_text.replace('査', '查')
    logger.info('ocr pdf to text: \n' + result_text)

    if result_text == '':
        logger.error('pdf解析出错')
        return '', ''

    if db.parsed_data.find({'origin_url': url, 'oss_file_origin_url': pdf_link}).count() == 0:
        if not if_ocr_flag:
            oss_file_map = {
                'origin_url': url,
                'oss_file_origin_url': pdf_link,
                'origin_url_id': origin_data_id,
                'oss_file_type': 'pdf',
                'oss_file_name': pdf_name,
                'oss_file_content': pdf_content,
                'parsed': False
            }
        else:
            oss_file_map = {
                'origin_url': url,
                'oss_file_origin_url': pdf_link,
                'origin_url_id': origin_data_id,
                'oss_file_type': 'pdf',
                'oss_file_name': pdf_name,
                'oss_file_content': pdf_content,
                'parsed': False,
                'if_ocr': True,
                'ocr_result': result_text
            }
        response = db.parsed_data.insert_one(oss_file_map)
        file_id = response.inserted_id
        oss_add_file(ali_bucket, str(file_id) + '/' + pdf_name + '.pdf', pdf_content)
        db.csrc_data.update_one({'_id': origin_data_id}, {'$set': {'status': 'parsed'}})
    else:
        db.csrc_data.update_one({'_id': origin_data_id}, {'$set': {'status': 'parsed'}})
        if if_ocr_flag:
            db.parsed_data.update_one({'origin_url': url,
                                       'oss_file_origin_url': pdf_link},
                                      {'$set': {'if_ocr': True, 'ocr_result': result_text}})
        file_id = db.parsed_data.find_one({'origin_url': url, 'oss_file_origin_url': pdf_link})['_id']

    logger.info('删除TMP文件')
    if os.path.exists('./test/tmp.pdf'):
        os.remove('./test/tmp.pdf')
    if os.path.exists('./test/tmp.txt'):
        os.remove('./test/tmp.txt')
    if os.path.exists('./test/tmp/'):
        for each_txt in os.listdir('./test/tmp'):
            os.remove('./test/tmp/' + each_txt)
        os.rmdir('./test/tmp')

    return result_text, file_id


# doc
def doc_parse(url, href, title, origin_data_id):
    doc_link = urljoin(url, href)
    doc_name = title.replace('.docx', '').replace('.doc', '')
    link_type = doc_link.split('.')[-1]

    response = request_site_page(doc_link)
    if response is None:
        logger.error('网页请求错误')
        return '', ''
    with open('./test/tmp.' + link_type, 'wb') as out_file:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                out_file.write(chunk)

    if link_type == 'doc' or link_type == 'wps':
        shell_str = config['soffice']['soffice_path'] + ' --headless --convert-to docx ' + \
                    './test/tmp.' + link_type + ' --outdir ./test/'
        process = subprocess.Popen(shell_str.replace(r'(', r'\(').replace(r')', r'\)'),
                                   shell=True, stdout=subprocess.PIPE)
        process.wait()

    with open('./test/tmp.docx', 'rb') as docx_file:
        docx_content = docx_file.read()

    if db.parsed_data.find({'origin_url': url, 'oss_file_origin_url': doc_link}).count() == 0:
        oss_file_map = {
            'origin_url': url,
            'oss_file_origin_url': doc_link,
            'origin_url_id': origin_data_id,
            'oss_file_type': 'docx',
            'oss_file_name': doc_name,
            'oss_file_content': docx_content,
            'parsed': False
        }
        response = db.parsed_data.insert_one(oss_file_map)
        file_id = response.inserted_id
        oss_add_file(ali_bucket, str(file_id) + '/' + doc_name + '.docx', docx_content)
        db.csrc_data.update_one({'_id': origin_data_id}, {'$set': {'status': 'parsed'}})
    else:
        db.csrc_data.update_one({'_id': origin_data_id}, {'$set': {'status': 'parsed'}})
        file_id = db.parsed_data.find_one({'origin_url': url, 'oss_file_origin_url': doc_link})['_id']

    doc = docx.Document('./test/tmp.docx')
    result_text_list = []
    for para in doc.paragraphs:
        result_text_list.append(para.text)
    result_text = '\n'.join(result_text_list)

    logger.info('删除TMP文件')
    if os.path.exists('./test/tmp.pdf'):
        os.remove('./test/tmp.pdf')
    if os.path.exists('./test/tmp.txt'):
        os.remove('./test/tmp.txt')
    if os.path.exists('./test/tmp/'):
        for each_txt in os.listdir('./test/tmp'):
            os.remove('./test/tmp/' + each_txt)
        os.rmdir('./test/tmp')
    if os.path.exists('./test/tmp.doc'):
        os.remove('./test/tmp.doc')
    if os.path.exists('./test/tmp.docx'):
        os.remove('./test/tmp.docx')
    if os.path.exists('./test/tmp.wps'):
        os.remove('./test/tmp.wps')

    return result_text, file_id


def parse_local_csrc(url, doc_type, data_id, org, origin_title):
    logger.info('url to parse ' + url)
    r = request_site_page(url)
    if r is None:
        logger.error('网页请求错误')
        return

    content_soup = bs(r.text.encode(r.encoding).decode('utf-8'), 'lxml')
    if len(content_soup.find_all(class_='mainContainer')) > 0:
        if content_soup.find(class_='title'):
            title = content_soup.find(class_='title').text.strip()
        else:
            title = ''
        if content_soup.find(class_='time'):
            time = content_soup.find(class_='time').text.replace('\n', '').strip()
        else:
            time = ''
        content_text = get_content_text(content_soup.find(class_='mainContainer'))
        content_text = re.sub('^' + title, '', content_text).strip()
        content_text = re.sub('^' + time, '', content_text).strip()
        all_content = content_soup.find(class_='mainContainer')
    else:
        if content_soup.find(class_='title'):
            title = content_soup.find(class_='title').text.strip()
        else:
            title = ''
        if content_soup.find(class_='time'):
            time = content_soup.find(class_='time').text.replace('\n', '').strip()
        else:
            time = ''
        content_text = get_content_text(content_soup.find(class_='content'))
        content_text = re.sub('^' + title.replace(r'(', r'\(').replace(r')', r'\)'), '', content_text).strip()
        content_text = re.sub('^' + time, '', content_text).strip()
        all_content = content_soup.find(class_='content')
    content_text = re.sub(r'#TRS_AUTOADD.*\n', '', content_text).strip()
    content_text = re.sub(r'varstrssre=.*?\n', '', content_text).strip()

    if len(content_soup.find_all(id='headContainer')) > 0:
        header_content_text = get_content_text(content_soup.find(id='headContainer')) \
            .replace(' ', '').replace('\n', '').replace('　', '').replace(' ', '')
    else:
        header_content_text = ''

    if len(content_soup.find_all(class_='title')) > 0:
        title = content_soup.find(class_='title').text.strip()
    else:
        if len(content_soup.find_all(id='lTitle')) > 0:
            title = content_soup.find(id='lTitle').text.strip()
        else:
            content = get_content_text(content_soup.find(id='ContentRegion'))
            title = content_text.replace(content, '')

    if '行政处罚听证规则' in title or '被中国证监会立案调查' in title or '上市公司报备情况' in title \
            or '上市辅导备案情况' in title or '上市辅导工作的总结报告' in title or '/hunfdqyxx/' in url \
            or '管理方式和衔接措施' in title or '激励支持政策措施的解读' in title or '监督管理措施实施' in title \
            or '工作规程' in title or '认真落实各项维稳措施' in title or '知识问答' in title \
            or '诚信监督管理暂行办法' in title or '行政处罚工作听证规则' in title or '廉洁自律基本准则' in title:
        logger.warning('与监管处罚没有关系！！')
        db.csrc_data.update_one({'_id': data_id}, {'$set': {'status': 'ignored'}})
        return

    try:
        content_links = [urljoin(url, each_a.attrs['href']) for each_a in all_content.find_all('a')
                         if 'href' in each_a.attrs.keys()]
        if len(content_links) == 0:
            if re.search(r'varfile_appendix=\'<ahref="(.*)?">', content_text):
                pdf_href = re.search(r'varfile_appendix=\'<ahref="(.*?)\">', content_text).group(1).strip()
                content_links = [urljoin(url, pdf_href)]
        logger.info(content_text)
        if doc_type == '行政处罚决定':
            if len(content_links) >= 1 and content_links[0].lower().endswith('pdf') and '当事人' not in content_text:
                if len(content_soup.find_all(class_='mainContainer')) > 0:
                    content_html = content_soup.find(class_='mainContainer')
                else:
                    content_html = content_soup.find(class_='content')
                if len(all_content.find_all('a')) > 0:
                    title = all_content.find('a').text.replace('.pdf', '').replace('.PDF', '').strip()
                else:
                    title = origin_title
                content_text, file_id = pdf_parse(url, content_links[0], title, data_id)
                if content_text == '':
                    logger.warning('pdf 解析出现问题！！')
                    return
            else:
                if len(content_links) >= 1 and content_links[0].lower().endswith(('doc', 'wps', 'docx')) \
                        and '当事人' not in content_text:
                    if len(content_soup.find_all(class_='mainContainer')) > 0:
                        content_html = content_soup.find(class_='mainContainer')
                    else:
                        content_html = content_soup.find(class_='content')
                    if len(all_content.find_all('a')) > 0:
                        title = all_content.find('a').text.replace('.doc', '') \
                            .replace('.wps', '').replace('.docx', '').strip()
                    else:
                        title = origin_title
                    content_text, file_id = doc_parse(url, content_links[0], title, data_id)
                    if content_text == '':
                        logger.warning('docx 解析出现问题！！')
                        return
                else:
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

            content_text = re.sub('\n+', '\n', content_text).strip()

            if re.search(r'((\n|^)(沪)?.\d{4}. ?\d+号\n)', content_text):
                document_code = re.search(r'((\n|^)(沪)?.\d{4}. ?\d+号\n)', content_text).group(1).strip()
            else:
                if re.search(r'((沪)?.\d{4}. ?\d+号)', title):
                    document_code = re.search(r'((沪)?.\d{4}. ?\d+号)', title).group(1).strip()
                else:
                    if re.search(r'文号:(.*?.\d{4}.\d+号|.*?\d{4}.\d+号.)主题词', header_content_text):
                        document_code = re.search(r'文号:(.*?.\d{4}.\d+号|.*?\d{4}.\d+号.)主题词',
                                                  header_content_text).group(1).strip()
                    else:
                        document_code = ''
            document_code = document_code.replace('\n', '')
            if document_code == '无':
                document_code = ''
            try:
                litigant = re.search(r'((当事人|事人|谢锦芬|钟佳顺)[\s\S]*?)((依据|依照|根据)[^\n，。]*?有关规定|'
                                     r'依据《中华人民共和国证券法》\(以下简称《证券法》\)的\n有关规定|'
                                     r'依据《中中华人民共和国证券法》\(以下简称《证券法》\)的\n有关规定|'
                                     r'依据《中华人民共和国证券法》（以下简称《证券法》）\n的有关规定|'
                                     r'依据《中华人民共和国证券法》\(以下筒称《证券法》\)的\n有关规定|'
                                     r'依据证券期货有关法律法规的规定|'
                                     r'经查|因你涉嫌证券从业人员私下接受客户委托买卖证券|'
                                     # ocr结果单独解析
                                     r'(依据|依照|根据)[^\n，。]*?\n?有\n?关规定)', content_text).group(1).strip()
            except Exception as e:
                litigant = re.search(r'时间.*?来源：\n([\s\S]*?)\n((依据|依照|根据)[^\n，。]*?有关规定)',
                                     content_text).group(1).strip()

            punishment_decision = re.search(
                r'[\n。，](((综上[^。\n]*?|鉴于[^。\n]*?)?(根据|考虑|综合|鉴于).*?(依据|依照|根据|按照).*?第.*?条.*?(规定)?.([我本]局)?.*?决定|'
                r'根据(当事人)?违法行为的事实、性质、情节与社会危害程度，我局决定|'
                r'(依据|依照|根据|按照).*?第.*?条.*?规定.*?决定|'
                r'根据当事人违法行为的事实、性质、情节与社会危害程度，依据《证券法》一百九十九条的规定，我局决定|'
                r'据华中汇富、张仲侃、刘劲松违规行为的事实、性质、情节与社会危害程度，依据《暂行办法》第三十八的规定，我局决定|'
                r'综上，本局决定，根据|因多次与你联系未果，现依法向你公告送达|'
                r'根据当事人违规行为的事实、性质、情节与社会危害程度，依据《暂行办法》第三十八的规定，我局决定|'
                r'根据《非上市公众公司重大资产重组管理办法》第三十条、《非上市公众公司监督管理办法》第六十条的规定，'
                r'依照《证券法》第一百九十三条的规定，结合当事人违法行为的事实、性质、情节、社会危害程度、积极配合调查的情况，我局决定|'
                r'根据当事人违规行为的事实、性质、情节与社会危害程度，依据《证券法》第一百九十三条的规定，我局决定|'
                r'依据《私募投资基金监督管理暂行办法》第三十八条的规定，我局决定|'
                r'宋铁农、宋铁兵、王燕彬在调查过程中，能够主动配合调查，主动承认相关违法事实。根据当事人违法行为的事实、性质、情节与社会危害程度，依据《证券法》第二百零二条规定，我局决定|'
                r'根据当事人违法行为的事实、性质、情节与社会危害程度，依据《证券法》第一百九十三的规定，我局决定|'
                r'本案，刘江勇积极配合调查且主动赔偿损失。根据当事人违法行为的事实、性质、情节与社会危害程度，依据《证券法》第二百一十五条、《中华人民共和国行政处罚法》第二十七条第一款第（一）项之规定，我局决定|'
                r'根据(当事人)?违法行为的事实、性质、情节与社会危害程度，依据《证券法》第.*?条.*?规定，我局决定|'
                r'考虑到王向远配合调查情节，依据《证券法》第二百一十五条规定，我局决定|'
                r'根据.*?违法行为的事实、性质、情节与社会危害程度，依据《证券法》第二百零二条的规定，我局决定|'
                r'根据当事人违法行为的事实、性质、情节与社会危害程度,\n依据《证券法》第一百九十三条第二款、第二百零四条的规定\n我局决定|'
                r'根据当事人违法行为的事实、性质、情节与社会危害程度,\n依据《证券法》第一百九十三条的规定,我局决定|'
                r'根据当事人违法行为的事实、性质、情节与社会危害程度,\n依据《证券法》第二百零二条,我局决定|'
                r'根据当事人违法行为的事实、性质、情节与社会危害程\n度,依据《私募投资基金监督管理暂行办法》第三十八条的\n规定,我局决定|'
                r'根据当事人违法行为的事实、性质、情节与社会危害程\n度,依据《证券法》第一百九十九条、第二百一十五条的规\n定,我局决定|'
                r'根据当事人违法行为的事实、性质、情节与社会危害程\n度,依据据《期货交易管理条例》第六十七条第三款的规定\n我局决定|'
                r'根据当事人违法行为的事实、性质、情节与社会危害程\n度,依据《期货交易管理条例》第六十七条第三款的规定\n我局决定|'
                r'根据《上市公司信息披露管理办法》第六十五条的规定，现要求|'
                # ocr 解析
                r'根据当事人上述违法行为的事实、性质、情节与社会危害程\n度,依据《证券法》第一百九十三条第一款的规定,我局决定|'
                r'根据当事人违法行为的事实、性质、情节与社会危害程度\n依据《证券法》第二百零二条的规定,我局决定)'
                r'[\s\S]*?)\n'
                r'(.*?应自收到本处罚决定书之日|(当事人)?.*?[如若](果)?对本(处罚)?决定不服|当事人如果对本市场禁入决定不服|'
                r'中国证券监督管理委员会上海监管局)',
                content_text).group(1).strip()

            truth_text_str = r'((经查|经査|一、|二、|三、|四、|五、|六、|七、|八、|九、)' \
                             r'[\s\S]*?)' \
                             r'((上述|以上)(违法(违规)?|违规)?(事实|情况|行为).*?等[^。，\n]*?(证据|佐证|谈话笔录等|作证|为证|证明)|' \
                             r'综上，根据《上市公司收购管理办法》第八十三条第二款第十二项“投资者之间具有其他关联关系”的规定|' \
                             r'述违规事实，有工商资料、相关协议书、询问笔录、情况说明等证据证明。|' \
                             r'上述违法事实,有相关笔录、情况说明、交易流水、公告等\n证据证明。|' \
                             r'上述违法事实,有公司公告、担保合同、借款合同、相关人\n员询问笔录等证据证明。|' \
                             r'上述违法事实,有证券账户资料、相关情况说明、询问笔录\n等证据证明,足以认定。|' \
                             r'以上事实,有登记备案情况说明、合伙协议、合伙人出\n资协议及财务收据、银行流水、入伙协议、询问笔录等证据\n证明,足以认定。|' \
                             r'\n我局认为|' \
                             r'\n根据《公司债券发行与交易管理办法》第五十八条的规定，我局决定对你公司采取责令改正的行政监管措施|' \
                             r'根据《期货公司监督管理办法》第八十七条的规定，现要求)'
            truth_compiler = re.compile(truth_text_str, re.MULTILINE)
            truth_list = truth_compiler.findall(content_text)
            if len(truth_list) >= 1:
                truth = '\n'.join([kk[0] for kk in truth_list]).strip()
            else:
                truth_text_str = r'((经查|一、|二、|三、|四、|五、|六、|七、|八、|九、)' \
                                 r'[\s\S]*?)' \
                                 + punishment_decision
                truth_compiler = re.compile(truth_text_str, re.MULTILINE)
                truth_list = truth_compiler.findall(content_text)
                truth = '\n'.join([kk[0] for kk in truth_list]).strip()

            if '申辩' in content_text:
                defense_text_str = r'((\n[^\n。，]*?(申辩|听证)[^\n。《]*?(提出|有以下两点|表示)|\n[^\n。，]*?(提出|提交)[^\n。，、]*?申辩|' \
                                   r'\n[^\n。，]*?辩称|天目药业、胡新笠、杨宗昌提出|徐欢晓提出|王长林提出|方宝康提出|张玲提出|当事人提出，涉案交易期间|' \
                                   r'\n[^\n。，、]*?提出如下陈述、申辩意见|当事人.*?在听证过程中，提出如下申辩意见|应当事人.*?要求，我局举行了听证会|' \
                                   r'在听证过程中.*?提出的主要陈述、申辩意见如下|当事人均?提出.*?陈述申辩意见|' \
                                   r'在我局向上述当事人送达《行政处罚事先告知书》后，当事人提出以下陈述申辩意见|' \
                                   r'当事人(均)?未(提交|要求|提出)陈述(和|、)?申辩|当事人虽要求陈述申辩，但期限内未提交陈述申辩意见。|' \
                                   r'当事人放弃陈述、申辩|当事人提出：被我局调查后|当事人.*?未陈述、?申辩|当事人.*?(进行|提出).*?陈述(和|、)申辩|' \
                                   r'[^\n。，]*?未要求陈述和申辩(，|,)也未(要求|申请)听证。|当事人没有陈述申辩意见。|' \
                                   r'当事人提交了陈述申辩|当事人均放弃了申辩和听证权利。|当事人海龙精密不要求陈述申辩和听证；张陈松娜、张家龙和罗雪娥不要求陈述申辩。|' \
                                   r'参加听证的当事人对鑫秋农业信息披露违法行为事实和证据无异议，但是认为应当从轻、减轻或者不予处罚，主要申辩意见如下|' \
                                   r'广东顾地、邱丽娟、林超群、林昌华、林昌盛在听证与申辩材料中，麦浩文在申辩材料中均对调查部门认定的基本事实不持异议，同时恳请减轻处罚，理由如下：|' \
                                   r'\n.*?表示不.*?申辩.*?但提交了|' \
                                   r'本案《行政处罚事先告知书》送达后，当事人王立提交了书面陈述、申辩意见，我局进行了复核。 王立提出|' \
                                   r'冯泽良在陈述申辩和听证中表示认错|本案《行政处罚事先告知书》送达后，当事人朱礼英提交了书面陈述、申辩意见，并提出听证申请。|' \
                                   r'2016年6月20日，我局向上述当事人发出了《行政处罚事先告知书》（厦证监处罚字〔2016〕2号）。' \
                                   r'当事人黄建忠、戴亦一、张白于6月30日向我局提交《申诉书》，并要求听证。7月15日，我局举行了听证会，黄建忠、戴亦一、张白出席了听证会。)' \
                                   r'([\s\S]*?))' \
                                   r'(经复核|针对上述申辩意见，我局认为|我局认为|我局经复核认为|对当事人冯泽良所提陈述申辩意见，我局进行了复核。|' \
                                   r'根据现有证据，结合.*?申辩意见，我局对.*?依法予以认定|' \
                                   r'上述情节，我局已依据《中华人民共和国行政处罚法》第二十七条的规定予以了充分考虑|' \
                                   r'本案(现)?已调查.*?审理终结。|' \
                                   r'对韩健华提出的不知、不懂相关法律规定申辩理由我局不予采纳|' \
                                   r'根据《中华人民共和国公司法》第一百四十七条第一款、《证券法》第六十八条第三款以及中国证监会的有关规定|' \
                                   r'对刘爱华提出的申辩理由，我局认为|' \
                                   r'我局根据在案各项证据|我局已对上述陈述申辩意见进行充分复核|' \
                                   r'根据前文所述理由|我局经审理认为|我局对申辩意见进行了复核。)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense_list = [kk for kk in defense_compiler.findall(content_text)]
                defense_response_list = []
                for defense_index, each_defense in enumerate(defense_list):
                    each_defense_str = each_defense[0]
                    if defense_index == len(defense_list) - 1:
                        defense_response_str = each_defense_str.replace(r'(', r'\(').replace(r')', r'\)') \
                                                   .replace(r'.', r'\.') \
                                                   .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                   .replace(r'+', r'\+') + \
                                               r'([\s\S]*?)' \
                                               + r'((经复核，)?本案(现)?已调查.*?审理(并复核)?终结。|' \
                                               + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)') \
                                                   .replace(r'.', r'\.') \
                                                   .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                   .replace(r'+', r'\+') \
                                               + r')'
                    else:
                        defense_response_str = each_defense_str.replace(r'(', r'\(').replace(r')', r'\)') \
                                                   .replace(r'.', r'\.') \
                                                   .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                   .replace(r'+', r'\+') + \
                                               r'([\s\S]*?)' \
                                               + r'((经复核，)?本案(现)?已调查.*?审理(并复核)?终结。|' \
                                               + defense_list[defense_index + 1][0].replace(r'(', r'\(') \
                                                   .replace(r')', r'\)').replace(r'.', r'\.') \
                                                   .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                   .replace(r'+', r'\+') \
                                               + r')'
                    defense_response_compiler = re.compile(defense_response_str)
                    defense_response_list.append(defense_response_compiler.search(content_text).group(1).strip())
                if len(defense_list) == 1:
                    defense = defense_list[0][0].strip()
                    defense_response = defense_response_list[0].strip()
                else:
                    defense = '\n'.join([kk[0] for kk in defense_list[1:]]).strip()
                    defense_response = '\n'.join(defense_response_list[1:]).strip()
            else:
                defense = defense_response = ''

            punishment_basis_str_list = [
                r'([^\n。；]*?)(问题|行为|事项|情况|事实|属于|情形)([^\n。；\s]*?)(违反|构成|不符合).*?\n?.*?第?.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            ]
            punishment_basis_str = '|'.join(punishment_basis_str_list)
            punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                   '(依据|根据|鉴于|按照)', re.MULTILINE)
            punishment_basis_list = punishment_basis_compiler.findall(content_text)
            punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

            publish_date_text = re.search(
                punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
                replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')
            if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1].replace(' ', '').replace(' ',
                                                                                                                 '')
                m = re.match("([0-9零一二两三四五六七八九十〇○ＯOΟО]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?", publish_date)
                real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(cn2dig(m.group(3))) + '日'
            else:
                publish_date_text = content_soup.find(class_='content').find(class_='time').find('span').text \
                    .replace('时间：', '')
                publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
                real_publish_date = publish_date.split('-')[0] + '年' + str(int(publish_date.split('-')[1])) + '月' + str(
                    int(publish_date.split('-')[2])) + '日'

            result_map = {
                'announcementTitle': title,
                'announcementOrg': org,
                'announcementDate': real_publish_date,
                'announcementCode': document_code,
                'facts': truth,
                'defenseOpinion': defense,
                'defenseResponse': defense_response,
                'litigant': litigant[:-1] if (litigant != '' and litigant[-1] in ['：', ':']) else litigant,
                'punishmentBasement': punishment_basis,
                'punishmentDecision': punishment_decision,
                'type': '行政处罚决定',
                'oss_file_id': file_id,
                'status': 'not checked'
            }
            logger.info(result_map)
            if db.announcement.find({'announcementTitle': title, 'oss_file_id': file_id}).count() == 0 \
                    and db.announcement.find({'announcementTitle': title, 'announcementDate': real_publish_date,
                                              'announcementCode': document_code,
                                              'litigant': litigant[:-1] if (
                                                      litigant != '' and litigant[-1] in ['：', ':']) else litigant}) \
                    .count() == 0:
                db.announcement.insert_one(result_map)
                logger.info(org + ' 数据解析 ' + doc_type + ' -- 数据导入完成')
            else:
                logger.info(org + ' 数据解析 ' + doc_type + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info(org + ' 数据解析 ' + doc_type + ' -- 修改parsed完成')
        if doc_type == '监管措施':
            if len(content_links) >= 1 and content_links[0].lower().endswith('pdf') and '经查' not in content_text \
                    and '现场检查' not in content_text and '核查时发现' not in content_text:
                if len(content_soup.find_all(class_='mainContainer')) > 0:
                    content_html = content_soup.find(class_='mainContainer')
                else:
                    content_html = content_soup.find(class_='content')
                if len(all_content.find_all('a')) > 0:
                    title = all_content.find('a').text.replace('.pdf', '').replace('.PDF', '').strip()
                else:
                    title = origin_title
                content_text, file_id = pdf_parse(url, content_links[0], title, data_id)
                if content_text == '':
                    logger.warning('pdf 解析出现问题！！')
                    return
            else:
                if len(content_links) >= 1 and content_links[0].lower().endswith(('doc', 'wps', 'docx')) \
                        and '经查' not in content_text and '现场检查' not in content_text and '核查时发现' not in content_text:
                    if len(content_soup.find_all(class_='mainContainer')) > 0:
                        content_html = content_soup.find(class_='mainContainer')
                    else:
                        content_html = content_soup.find(class_='content')
                    if len(all_content.find_all('a')) > 0:
                        title = all_content.find('a').text.replace('.doc', '') \
                            .replace('.docx', '').replace('.wps', '').strip()
                    else:
                        title = origin_title
                    content_text, file_id = doc_parse(url, content_links[0], title, data_id)
                    if content_text == '':
                        logger.warning('docx 解析出现问题！！')
                        return
                else:
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

            content_text = re.sub(r'\n+', r'\n', content_text).strip()
            if re.search(r'((\n|^).*?\d{4}.\d+号\n)', content_text):
                document_code = re.search(r'((\n|^).*?\d{4}.\d+号\n)', content_text).group(1).strip()
            else:
                if re.search(r'(.\d{4}.\d+号)', title):
                    document_code = re.search(r'(.\d{4}.\d+号)', title).group(1).strip()
                else:
                    if re.search(r'文号:(.*?.\d{4}.\d+号|.*?\d{4}.\d+号.)主题词', header_content_text):
                        document_code = re.search(r'文号:(.*?.\d{4}.\d+号|.*?\d{4}.\d+号.)主题词',
                                                  header_content_text).group(1).strip()
                    else:
                        document_code = ''
            document_code = document_code.replace('\n', '')
            document_code = re.sub('时间：.*?来源：', '', document_code)
            if document_code == '无':
                document_code = ''
            try:
                litigant = re.search(r'((\n|^).*?\n)(经查|经核查|近期我局在日常监管中发现|我局日常监管发现|'
                                     '我局在对深圳市长方集团股份有限公司（以下简称“长方集团”或“公司”）的专项检查中|'
                                     '根据.*?要求|我局.*?现场检查|根据《中华人民共和国证券法》、《上市公司现场检查办法》|'
                                     '2016年2月3日，|根据《证券法》和《公司债券发行与交易管理办法》|'
                                     '我局在日常监管中关注到|经现场检查，我局发现|发现.*?存在.*?问题|'
                                     '(根据|依据).*?有关规定|根据湖南尔康制药股份有限公司（以下简称“尔康制药”）2018年4月25日披露的|'
                                     '我局(在)?日常监管中发现|根据中国证监会公司债券监管部的统一部署|'
                                     '你作为英唐智控持股5%以上的股东|2018年5月25日，我局向你公司|'
                                     '你公司未能于2016年6月30日前召开2015年年度股东大会|'
                                     '你公司2014年定期报告和2015年半年报中|2017年12月29日，我局向你公司下发|'
                                     '我局在对你公司检查中发现|'
                                     '我局关注到你公司存在以下情况|因你涉嫌证券从业人员私下接受客户委托买卖证券|'
                                     '你司未按照|你.*?作为.*?股东|'
                                     '根据厦门蒙发利科技（集团）股份有限公司2016年7月25日披露|'
                                     '根据我局对你司2013年度检查|根据《中华人民共和国证券法》、中国证监会《非上市公众公司监督管理办法》（中国证监会令第96号）|'
                                     '你公司在2018年4月24日披露的2017年财务报告中|'
                                     '经中国证监会核准|.*?构成一致行动关系|你于2018年3月8日通过集中竞价方式减持杭州炬华科技股份有限公司|'
                                     '2018年3月7日，你公司通过集中竞价交易减持牧高笛户外用品股份有限公司|2015年5月29日，你公司发生一起重大信息安全事件|'
                                     r'近日,我局根据《证券法》、《上市公司现场检查办法》（证监会公告\[2010\]12号）等规定对你公司开展了现场检查|'
                                     # ocr 解析
                                     '我局对你公司进\n?行\n?了现场检查|'
                                     '你从2015年6月23日至今担任昆明百货大楼|'
                                     '根据我局对你公司进行的现场检查,你公司存在以下问题)', content_text).group(1).strip()
            except Exception as e:
                if re.search(r'(时间.*?来源.*?)\n(.*?)\n', content_text):
                    litigant = re.search(r'(时间.*?来源.*?)\n(.*?)\n', content_text).group(2).strip()
                else:
                    if re.search(r'(关于.*?\n?.*?决定\n|采取监管谈话措施的决定\n)?(.*?\d{4}.\d+号\n)?(.*?)\n', content_text):
                        litigant = re.search(r'(关于.*?\n?.*?决定\n|采取监管谈话措施的决定\n)?(.*?\d{4}.\d+号\n)?(.*?)\n',
                                             content_text).group(3).strip()
                    else:
                        if re.search(r'^var.*?决定.*?\n+(.*?)\n', content_text):
                            litigant = re.search(r'^var.*?决定.*?\n+(.*?)\n', content_text).group(1).strip()
                        else:
                            if re.search(r'^(.*?)\n', content_text):
                                litigant = re.search(r'^(.*?)\n', content_text).group(1).strip()
                            else:
                                litigant = ''

            punishment_decision = re.search(
                r'[\n。，,]'
                r'(((根据|按照|现根据|依据|依照)[^。\n]*?第.*?条.*?(我局)?(决定|现责令|现对.*?(予以|采取|出具)|现?要求|现提醒|请你于|责令|我局作出|你公司应采取切实有效的措施进行改正)|'
                r'(鉴于|针对|对于|综上).*?(依据|根据|按照).*?(我局((决定)?对|作出)|对.*?采取|我局决定如下)|'
                r'现?根据.*?第.*?条.*?对.*(予以|出具|采取)|针对你公司存在的上述问题，我局提出以下处理意见|'
                r'(现按照|按照|根据)[^。\n]*?第[^。\n]*?条的?规定，(现对你营业部予以|现责令你公司|现对你|现决定对|决定对你|责令你|现要求你|对.*?出具警示函)|'
                r'针对本次检查发现你公司存在的问题，根据|为规范(大股东|上市公司董事|股东)行为，维护.*?秩序，(我局决定|现决定)|'
                r'按照.*?的规定，我局决定|依据《上市公司信息披露管理办法》第五十九条，现决定对你公司采取责令改正的监管措施|'
                r'鉴于你减持交易具有一定的连续性，短线交易未获得收益，违法行为较为轻微，且及时采取措施自查自纠，通过公司公告终止此次减持计划，未造成严重后果，我局决定|'
                r'综合整个事件的性质、影响和情节，根据《上市公司信息披露管理办法》第五十九条第二项的规定，我局决定|'
                r'综合考虑整个事件的性质、影响和情节等情况，我局决定依照|'
                r'根据《上市公司收购管理办法》第七十五条，我局决定|因多次与你联系未果，现依法向你公告送达|'
                r'综上，根据《股东大会规则》第四十八条的规定，我局决定|我局决定暂不解除|'
                r'为加强对你的警示和教育，提高你的遵规守法意识，我局决定|'
                r'依据《非上市公众公司监督管理办法》第五十六条、第六十二条的规定，金马扬名、吕江应采取有效措施及时整改|'
                r'我局决定对你公司采取责令改正的监督管理措施|按照.*?第.*?条的\n规定，我局决定对你采取|'
                r'根据《上市公司信息披露管理办法》第五十九条的规定，我局决定|'
                r'为维护中小投资者利益、保障其知情权，现要求你公司在收到本决定的30日内，在中国证监会指定信息披露媒体上，对如下事项进行说明|'
                r'根据《期货公司资产管理业务试点办法》第四十九条第一款第四项，《私募投资基金监督管理暂行办法》第三十三条，现决定暂停你|'
                r'根据《上市公司监管指引第\n4\n号——上市公司实际控制人、股东、关联方、收购人以及上市公司承诺及履行》第六条规定，现决定对|'
                r'按照《上市公司收购管理办法》第七十五条的规定，决定对你们采取|'
                r'根据《关于上市公司建立内幕信息知情人登记管理制度的规定》第十五条的规定，要求你|'
                r'你司应制定详细的整改措施和整改计划，健全内控机制，完善合规管理，认真履行信息报送义务，并根据|'
                r'根据《证券投资顾问业务暂行规定》、《证券期货市场监督管理措施实施办法（试行）》的有关规定，决定对你|'
                r'根据《上市公司信息披露管理办法》第五十九条，现责令你公司予以改正|'
                r'你公司应在.*?前，向我局提交书面报告，我局将组织检查验收。|'
                r'鉴于你及时采取措施.*?未造成严重影响，我局决定|'
                r'按照《上市公司信息披露管\n理办法》第五十八条和第五十九条的规定，我局决定|'
                r'为督促你营业部依法合规开展证券经纪业务，根据.*?规定，我局拟作出如下监督管理措施决定|'
                r'为加强警示教育，我局决定|依据.*?第.*?条，现决定对|'
                r'现责令.*?予以改正|现对你予以警示。|'
                r'现要求你行对上述(问题|行为)进行(改正|整改)|'
                r'(现|我局)?(根据|按照|依据|结合|依照).*?(第.*?条|规定).*?(我局决定|现?要求|对.*?采取|现?责令|现决定|现警示|我局拟作出)|'
                r'根据.*?第.*?条，现决定|现对你采取责令改正的监管措施|'
                r'我局决定对.*?采取.*?监督管理措施|现责令.*?立即改正上述违法行为|'
                r'按照《上市公司信息披露管理办法》（证监会令第40号）第五十九的规定，对你公司实施|'
                r'你营业部应当在2015年3月13日前向我局提交书面报告|'
                r'根据证监会公告〔\n2013\n〕\n55\n号文第六条的规定，我局决定|'
                r'我局决定对你所及注册会计师曹忠志、王亚平采取出具警示函的监督管理措施。|'
                r'针对上述问题，我局已于2017年10月17日向你公司出具《监管关注函》，但你公司至今仍未解决。现对你|'
                r'    现按照《上市公司收购管理办法》第七十五条规定，对你予以警示|'
                r'按照相关法律法规的规定，现要求|现要求你公司予以改正，达到如下要求|'
                r'我局按照《上市公司信息披露管理办法》第五十九条和《证券期货市场监督管理措施实施办法》第十一条的相关规定，现对你公司予以警示|'
                r'《上市公司信息披露管理办法》第五十九条和《上市公司收购管理办法》第七十五条规定，决定对你公司采取责令改正的监管措施|'
                r'我局拟依据《上市公司现场检查办法》（证监会公告\[2010\]12号）第二十一条之规定，要求你公司立即作出改正|'
                r'我局责成你们将短线交易所产生收益按《证券法》第四十七条的规定上缴公司，并决定|'
                r'我局决定对.*?采取.*?并记入中国证监会诚信档案。|根据《上市公司现场检查办法》，我局定于|'
                r'对你公司存在的上述问题，我局决定对你公司采取责令改正的措施|针对你司上述违规情况，我局责令你司做出如下整改|'
                r'你公司应当加强公司管理，提高合规经营意识，严格按照整改要求，切实落实改正事项|'
                r'我局决定对你予以警示。请你认真吸取教训|现对你公司提出警示。\n|你公司应当加强内部管理，认真学习落实法律法规|'
                r'现要求你公司针对上述事项采取有效措施进行整改|按照《证券公司监督管理条例》（国务院令第|你公司应采取切实有效的措施进行改正'
                r'按照《上市公司信息披露管理办法》第五十九的规定，现提醒你|你.*?应采取切实有效的措施进行改正|'
                r'你公司应在收到本决定书后15个工作日内进行整改，并及时披露本决定书相关内容和整改情况。|'
                r'针对以上违规事实，我局对你公司提出以下要求|为了督促上述人员提高守法意识、提升职业操守和执业能力，现要求|'
                r'鉴于你公司已按要求完成整改并经我局验收合格，根据《期货交易管理条例》第五十九条等有关规定，我局决定|'
                r'现责令你公司进行改正，同时要求你公司做好以下工作|现对.*?采取出具警示函的监管措施.*?应认真吸取教训|'
                r'针对上述违规行为，我局决定对你采取出具警示函的监督管理措施。现提醒你|'
                r'现责令你公司在2014年4月15日前完成整改，且达到如下要求|'
                r'鉴于你公司尚未展业，针对我局现场检查反馈意见，立即着手整改了上述问题|'
                r'根据《证券公司客户资产管理业务管理办法》、《证券公司集合资产管理业务实施细则》，我局决定责令|'
                r'针对你公司存在的上述问题，根据《上市公司现场检查办法》第二十一条第一款规定，我局决定|'
                r'按照中国证监会《证券期货市场监督管理措施实施办法（试行）》（证监发|'
                r'我局决定：\n责令你营业部|现已通过我局验收|'
                r'上述问题的发生，反映出你公司在规范运作、内部管理、信息披露等方面存在着重大缺陷，尤其在合同、印章、票据及凭证管理方面内控未能发挥作用。现要求你公司针对上述问题进行改正且达到如下要求|'
                # ocr 解析
                r'鉴于你买卖股票涉\n?及\n?的金额较小.*?且没有造\n?成危害\n?后果,根据.*?第二十七条第二款有关规\n?定,我\n?局依法不予行政处罚|'
                r'(现根据|根据|按照|依据|结合|依照|接照).*?\n?.*?第.*?\n?.*?条\n?[的之]?规定,\n?(我局\n?决定|现要求|我局\n?责令|我局对你公司予以警示|现对你公司采取)|'
                r'现要求你公司立即停止上述违规行为，并在2017年4月7日前予以改正|'
                r'鉴于.*?\n?.*?根据.*?\n?.*?第.*?\n?.*?条[的之]?规定.*?(对你公\n?司\n?采取)|'
                r'但是,鉴于你已按\n照我局\(云证监函2016\)153号\)的要求履行了承诺,并积\n极争取中小股东谅解,'
                r'现按照《上市公司监管指引第4号\n上市公司实际控制人、股东、关联方、收购人以及上市公司承\n诺及履行》第六条的规定,对你采取|'
                r'按照《中国人民共和国证券法》第一百九十五条的规定\n我局对你实施的短线交易行为进行警示|'
                r'依照《证券、期货投资咨询管理暂行办法》及《证券投资顾\n问业务暂行规定》等规定,我局责令|'
                r'依据《公司债券\n发行与交易管理办法》第五十八条,我局决定|'
                r'我局决定对你公司采取责令改正的监督管理措\n施\n你公司应引以为戒|'
                r'按照《非上市公众公司监督管\n理办法》第五十五条和第六十二条,我局决定|'
                r'根据《非上市公众公司监督管理办法》第\n六十二条,我局决定对你公司采取|'
                r'根据《上市公司信息披露管理办法》第五十九条,现要\n求|'
                r'根据《公司债\n券发行与交易管理办法》第五十八条、六十六条规定,我局\n要求|'
                r'针对你司上述违规情况,我局责令你司做出如下整改|'
                r'根据《上市公司信息披露管理办法》第五十九条、《关\n于规范上市公司与关联方资金往来及上市公司对外担保若|'
                r'按照《证券投资顾问业务暂行规定》第三\n十三条的规定,现责令你|'
                r'按照《上市公司现场检查办法》第ニ十ー条、第\nニ十五条等有关规定,我局决定对|'
                r'鉴于上述违规事实及情节,根据《业务规则》第6.1条、《信\n息披露细则\(试行\)》第四十七条的规定,我司做出如下决定)'
                r'[\s\S]*?)'
                r'(\n([^\d\n]*?应高度重视|(你分公司|你营业部)?如果对本监督管理(措施|拮施)不服|如你?对本监督管理措施不服|'
                r'你公司对上述问题如有异议|你公司如对上述问题持有异议|特此公告。|'
                r'你公司如对本监督管理措施不服|你如对本监督管理措施不服|你可以在收到本决定书之日|你公司如果对本监督管理措施不服|'
                r'上述整改落实情况及说明事项应经董事会和监事会审议通过|如果你?对本监督管理措施不服|你部如果对本监督管理措施不服|'
                r'.*?(如果|若|如).*?对(本|上述)(监督管理|监管|行政监管|行政监督管理)措施不服|如果对本决定不服|你.*?在收到本决定|'
                r'因多次.*?联系未果|.*?应在收到本决定书|你所应在收到本定书之日|江苏证监局|福建证监局|二ОО九年七月八日)|'
                r'你[^。，\n]*?应在收到本决定|你[^。，\n]*?如果对本监管措施不服|浙江证监局|中国证券监督管理委员会上海监管局)',
                content_text).group(1).strip()

            truth_text_str = r'((经查|二、|三、|四、|五、|六、|七、|八、|九、|\n1\.|\n2\.|\n3\.|\n4\.|\n5\.|' \
                             r'发现[^，。\n]*?存在(以下|下列|如下)问题|' \
                             r'近期我局在日常监管中发现|我局日常监管发现|我局在对深圳市长方集团股份有限公司（以下简称“长方集团”或“公司”）的专项检查中|' \
                             r'检查发现|发现[^，。\n]*?存在以下问题|检查中，发现以下问题|你作为|2016年2月3日，|' \
                             r'存在以下问题|发现你公司存在以下情况|存在以下情况|' \
                             r'我局在日常监管中关注到|你作为英唐智控持股5%以上的股东|' \
                             r'你公司未能于2016年6月30日前召开2015年年度股东大会|' \
                             r'我局向你公司下发了《深圳证监局关于对深圳市零七股份有限公司采取责令改正措施的决定》|' \
                             r'你司未按照|2017年9月27日|2017年5月22日|' \
                             r'近期，我局正在按照中国证监会的统一部署和安排|' \
                             r'经\n查,你于2016年3月4日买入昆百大A股票18\.200股)' \
                             r'[\s\S]*?)' \
                             r'((上述|以上)(违法(违规)?|违规)?(事实|情况|行为).*?等[^。，\n]*?(证据|佐证|谈话笔录等|作证|为证|证明)|' \
                             r'综上，根据《上市公司收购管理办法》第八十三条第二款第十二项“投资者之间具有其他关联关系”的规定|' \
                             r'述违规事实，有工商资料、相关协议书、询问笔录、情况说明等证据证明。|' \
                             r'[^\n。]*(上述|以上)(行为|情形).*?(违反|构成|不符合)|' \
                             r'深大通相关披露违反了|你公司相关披露违反了|[^\n。]*?上述(行为|情况)不符合|' \
                             r'[^\n。]*上述问题(分别)?违反|根据你公司提交的相关资料和我局日常监管了解到的情况|' \
                             r'2013年8月，我局曾对你公司进行过现场检查|上述情形反映出|' \
                             r'根据《公司债券发行与交易管理办法》第五十八条的规定，我局决定对你公司采取责令改正的行政监管措施。|' \
                             r'作为两面针公司的董事，你在发生上述买卖行为时未按)'
            truth_compiler = re.compile(truth_text_str, re.MULTILINE)
            truth_list = truth_compiler.findall(content_text)
            if len(truth_list) >= 1:
                truth = '\n'.join([kk[0] for kk in truth_list]).strip()
            else:
                truth_text_str = r'((经查|发现.*?存在以下问题|检查中，发现以下问题|检查发现|存在以下情况|存在不规范情况|2013年11月6日|' \
                                 r'你司未按照|发现以下问题|' \
                                 r'根据厦门蒙发利科技（集团）股份有限公司2016年7月25日披露|' \
                                 r'天津鹏翎集团股份有限公司（简称鹏翎股份）于2018年9月14日披露)' \
                                 r'[\s\S]*?)' \
                                 + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                     .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                     .replace(r'+', r'\+')
                truth_compiler = re.compile(truth_text_str, re.MULTILINE)
                truth_list = truth_compiler.findall(content_text)
                if len(truth_list) >= 1:
                    truth = '\n'.join([kk[0] for kk in truth_list]).strip()
                else:
                    truth_text_str = litigant.replace(r'*', r'\*') + r'([\s\S]*?)' \
                                     + '(' + punishment_decision \
                                         .replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                         .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                         .replace(r'+', r'\+') + '|' + r'(\n|。)[^。\n]*?行为[^。\n]*?违反[^。\n]*?规定' + ')'
                    truth_compiler = re.compile(truth_text_str, re.MULTILINE)
                    truth_list = truth_compiler.findall(content_text)
                    if len(truth_list) >= 1:
                        truth = '\n'.join([kk[0] for kk in truth_list]).strip()

            if '申辩' in content_text:
                defense_text_str = r'((\n[^\n。，]*?(申辩|听证)[^\n。《]*?(提出|有以下两点|表示)|\n[^\n。，]*?(提出|提交)[^\n。，、]*?申辩|' \
                                   r'\n[^\n。，]*?辩称|天目药业、胡新笠、杨宗昌提出|徐欢晓提出|王长林提出|方宝康提出|张玲提出|当事人提出，涉案交易期间|' \
                                   r'\n[^\n。，、]*?提出如下陈述、申辩意见|当事人.*?在听证过程中，提出如下申辩意见|应当事人.*?要求，我局举行了听证会|' \
                                   r'在听证过程中.*?提出的主要陈述、申辩意见如下|当事人均?提出.*?陈述申辩意见|' \
                                   r'在我局向上述当事人送达《行政处罚事先告知书》后，当事人提出以下陈述申辩意见|' \
                                   r'当事人(均)?未(提交|要求|提出)陈述(和|、)?申辩|当事人虽要求陈述申辩，但期限内未提交陈述申辩意见。|' \
                                   r'当事人放弃陈述、申辩|当事人提出：被我局调查后|当事人.*?未陈述、?申辩|当事人.*?(进行|提出).*?陈述(和|、)申辩|' \
                                   r'[^\n。，]*?未要求陈述和申辩(，|,)也未(要求|申请)听证。|当事人没有陈述申辩意见。|' \
                                   r'当事人提交了陈述申辩|当事人均放弃了申辩和听证权利。|当事人海龙精密不要求陈述申辩和听证；张陈松娜、张家龙和罗雪娥不要求陈述申辩。|' \
                                   r'参加听证的当事人对鑫秋农业信息披露违法行为事实和证据无异议，但是认为应当从轻、减轻或者不予处罚，主要申辩意见如下|' \
                                   r'广东顾地、邱丽娟、林超群、林昌华、林昌盛在听证与申辩材料中，麦浩文在申辩材料中均对调查部门认定的基本事实不持异议，同时恳请减轻处罚，理由如下：|' \
                                   r'\n.*?表示不.*?申辩.*?但提交了|' \
                                   r'本案《行政处罚事先告知书》送达后，当事人王立提交了书面陈述、申辩意见，我局进行了复核。 王立提出|' \
                                   r'冯泽良在陈述申辩和听证中表示认错|本案《行政处罚事先告知书》送达后，当事人朱礼英提交了书面陈述、申辩意见，并提出听证申请。|' \
                                   r'2016年6月20日，我局向上述当事人发出了《行政处罚事先告知书》（厦证监处罚字〔2016〕2号）。' \
                                   r'当事人黄建忠、戴亦一、张白于6月30日向我局提交《申诉书》，并要求听证。7月15日，我局举行了听证会，黄建忠、戴亦一、张白出席了听证会。)' \
                                   r'([\s\S]*?))' \
                                   r'(经复核|针对上述申辩意见，我局认为|我局认为|我局经复核认为|对当事人冯泽良所提陈述申辩意见，我局进行了复核。|' \
                                   r'根据现有证据，结合.*?申辩意见，我局对.*?依法予以认定|' \
                                   r'上述情节，我局已依据《中华人民共和国行政处罚法》第二十七条的规定予以了充分考虑|' \
                                   r'本案(现)?已调查.*?审理终结。|' \
                                   r'对韩健华提出的不知、不懂相关法律规定申辩理由我局不予采纳|' \
                                   r'根据《中华人民共和国公司法》第一百四十七条第一款、《证券法》第六十八条第三款以及中国证监会的有关规定|' \
                                   r'对刘爱华提出的申辩理由，我局认为|' \
                                   r'我局根据在案各项证据|我局已对上述陈述申辩意见进行充分复核|' \
                                   r'根据前文所述理由|我局经审理认为|我局对申辩意见进行了复核。)'
                defense_compiler = re.compile(defense_text_str, re.MULTILINE)
                defense_list = [kk for kk in defense_compiler.findall(content_text)]
                defense_response_list = []
                for defense_index, each_defense in enumerate(defense_list):
                    each_defense_str = each_defense[0]
                    if defense_index == len(defense_list) - 1:
                        defense_response_str = each_defense_str \
                                                   .replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                                   .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                   .replace(r'+', r'\+') + \
                                               r'([\s\S]*?)' \
                                               + r'((经复核，)?本案(现)?已调查.*?审理(并复核)?终结。|' \
                                               + punishment_decision \
                                                   .replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                                   .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                   .replace(r'+', r'\+') \
                                               + r')'
                    else:
                        defense_response_str = each_defense_str \
                                                   .replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                                   .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                   .replace(r'+', r'\+') + \
                                               r'([\s\S]*?)' \
                                               + r'((经复核，)?本案(现)?已调查.*?审理(并复核)?终结。|' \
                                               + defense_list[defense_index + 1][0] \
                                                   .replace(r'(', r'\(').replace(r')', r'\)').replace(r'.', r'\.') \
                                                   .replace(r'[', r'\[').replace(r']', r'\]').replace(r'*', r'\*') \
                                                   .replace(r'+', r'\+') \
                                               + ')'
                    defense_response_compiler = re.compile(defense_response_str)
                    defense_response_list.append(defense_response_compiler.search(content_text).group(1).strip())
                if len(defense_list) == 1:
                    defense = defense_list[0][0].strip()
                    defense_response = defense_response_list[0].strip()
                else:
                    defense = '\n'.join([kk[0] for kk in defense_list[1:]]).strip()
                    defense_response = '\n'.join(defense_response_list[1:]).strip()
            else:
                defense = defense_response = ''

            punishment_basis_str_list = [
                r'([^\n。；]*?)(问题|行为|事项|情况|事实|属于|情形)([^\n。；\s]*?)(违反|不符合|构成).*?\n?.*?第?.*?条?\n?.*?((的|之|等)(相关)?规定)?',
            ]
            punishment_basis_str = '|'.join(punishment_basis_str_list)
            punishment_basis_compiler = re.compile(r'[。\n；]' + '(' + punishment_basis_str + ')' +
                                                   '(依据|根据|鉴于|按照|我局决定|你公司及董事长、董事会秘书)', re.MULTILINE)
            punishment_basis_list = punishment_basis_compiler.findall(content_text)
            punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

            publish_date_text = re.search(
                punishment_decision.replace(r'(', r'\(').replace(r')', r'\)').replace(r'[', r'\[').replace(r']', r'\]').
                replace(r'*', r'\*') + r'([\s\S]*?)$', content_text).group(1).replace('\n', '')
            try:
                if re.search(r'.{4}年.{1,2}月.{1,3}日', publish_date_text):
                    publish_date = re.findall('.{4}年.{1,2}月.{1,3}日', publish_date_text)[-1] \
                        .replace(' ', '').replace(' ', '')
                    m = re.match("([0-9零一二两三四五六七八九十〇○ＯOΟО]+年)?([0-9一二两三四五六七八九十]+)月?([0-9一二两三四五六七八九十]+)[号日]?",
                                 publish_date)
                    real_publish_date = get_year(m.group(1)) + str(cn2dig(m.group(2))) + '月' + str(
                        cn2dig(m.group(3))) + '日'
                else:
                    publish_date_text = content_soup.find(class_='content').find(class_='time').find('span').text \
                        .replace('时间：', '')
                    publish_date = re.findall(r'\d{4}-\d{1,2}-\d{1,2}', publish_date_text)[-1]
                    real_publish_date = publish_date.split('-')[0] + '年' + str(
                        int(publish_date.split('-')[1])) + '月' + str(
                        int(publish_date.split('-')[2])) + '日'
            except Exception as e:
                real_publish_date = ''

            result_map = {
                'announcementTitle': title,
                'announcementOrg': org,
                'announcementDate': real_publish_date,
                'announcementCode': document_code,
                'facts': truth,
                'defenseOpinion': defense,
                'defenseResponse': defense_response,
                'litigant': litigant[:-1] if (litigant != '' and litigant[-1] in ['：', ':']) else litigant,
                'punishmentBasement': punishment_basis[1:] if len(punishment_basis) > 0 and punishment_basis[
                    0] == '。' else punishment_basis,
                'punishmentDecision': punishment_decision,
                'type': '监管措施',
                'oss_file_id': file_id,
                'status': 'not checked'
            }
            logger.info(result_map)
            if db.announcement.find({'announcementTitle': title, 'oss_file_id': file_id}).count() == 0 \
                    and db.announcement.find({'announcementTitle': title, 'announcementDate': real_publish_date,
                                              'announcementCode': document_code,
                                              'litigant': litigant[:-1] if (
                                                      litigant != '' and litigant[-1] in ['：', ':']) else litigant}) \
                    .count() == 0:
                db.announcement.insert_one(result_map)
                logger.info(org + ' 数据解析 ' + doc_type + ' -- 数据导入完成')
            else:
                logger.info(org + ' 数据解析 ' + doc_type + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info(org + ' 数据解析 ' + doc_type + ' -- 修改parsed完成')
    except Exception as exc:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        logger.error('出错行数：%s' % str(exc_tb.tb_lineno))
        logger.error(exc)
        logger.info('删除TMP文件')
        if os.path.exists('./test/tmp.pdf'):
            os.remove('./test/tmp.pdf')
        if os.path.exists('./test/tmp.txt'):
            os.remove('./test/tmp.txt')
        if os.path.exists('./test/tmp/'):
            for each_txt in os.listdir('./test/tmp'):
                os.remove('./test/tmp/' + each_txt)
            os.rmdir('./test/tmp')
        if os.path.exists('./test/tmp.doc'):
            os.remove('./test/tmp.doc')
        if os.path.exists('./test/tmp.docx'):
            os.remove('./test/tmp.docx')
        if os.path.exists('./test/tmp.wps'):
            os.remove('./test/tmp.wps')


def parse():
    # 地方证监局
    for each_data in db.csrc_data.find({'origin': {'$nin': ['证监会']},
                                        'status': {'$nin': ['ignored']}}) \
            .sort("_id", 1):
        if db.csrc_data.find({'url': each_data['url'], 'status': 'parsed'}).count() == 1 and \
                (db.parsed_data.find({'origin_url': each_data['url']}).count() == 0 or db.parsed_data.find(
                    {'origin_url': each_data['url'], 'parsed': True}).count() == 1):
            continue
        parse_local_csrc(each_data['url'], each_data['type'], each_data['_id'], each_data['origin'], each_data['title'])


if __name__ == "__main__":
    parse()
