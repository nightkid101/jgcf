from pymongo import MongoClient
import re
import os
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from docx import Document
from xlrd import open_workbook, xldate_as_tuple
from init import logger_init, config_init
from utility import format_date, remove_special_char, request_site_page, get_content_text
from oss_utils import init_ali_oss, oss_add_file
import subprocess
from pdf2html import pdf_ocr_to_text

logger = logger_init('律协 数据解析')
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


# 全国律师协会
def acla_parse():
    for each_document in db.lawyers_data.find({'origin': '全国律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title']

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')

        org_info = content_soup.find(class_='acla_text_info').text
        announcement_org = re.search(r'作者：(.*?)\n来源：', org_info).group(1).strip()

        if db.parsed_data.find({'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            img_link_list = [urljoin(announcement_url, kk.attrs['src'])
                             for kk in content_soup.find(class_='acla_text_left_main').find_all('img')]

            for index, each_img_link in enumerate(img_link_list):
                img_response = request_site_page(each_img_link)
                with open('./test/' + str(index) + '.jpg', 'wb') as tmp_file:
                    for chunk in img_response.iter_content(chunk_size=1024):
                        if chunk:
                            tmp_file.write(chunk)

            if not os.path.exists('./test/tmp.pdf'):
                shell_str = 'img2pdf '
                for index in range(len(img_link_list)):
                    shell_str += './test/' + str(index) + '.jpg '
                shell_str += '-o  ./test/tmp.pdf'
                process = subprocess.Popen(shell_str.replace(r'(', r'\(').replace(r')', r'\)'),
                                           shell=True, stdout=subprocess.PIPE)
                process.communicate()

            result_text, ocr_flag = pdf_ocr_to_text('./test/tmp.pdf')

            with open('./test/tmp.pdf', 'rb') as pdf_file:
                pdf_content = pdf_file.read()

            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_document['_id'],
                'oss_file_type': 'pdf',
                'oss_file_name': announcement_title,
                'oss_file_content': pdf_content,
                'parsed': False,
                'if_ocr': True,
                'ocr_result': result_text
            }
            response = db.parsed_data.insert_one(oss_file_map)
            file_id = response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.pdf', pdf_content)
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            content_text = result_text

            logger.info('删除TMP文件')
            if os.path.exists('./test/tmp.pdf'):
                os.remove('./test/tmp.pdf')
            for index in range(len(img_link_list)):
                if os.path.exists('./test/' + str(index) + '.jpg'):
                    os.remove('./test/' + str(index) + '.jpg')
        else:
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']
            content_text = db.parsed_data.find_one({'origin_url': announcement_url,
                                                    'oss_file_origin_url': announcement_url})['ocr_result']

        logger.info(content_text)
        announcement_code_compiler = re.compile(r'((京律纪处|苏律|川律协惩处字|珠律纪字|沪律发).*?\d{4}.*?第?\d+号)')
        if announcement_code_compiler.search(announcement_title):
            announcement_code = announcement_code_compiler.search(announcement_title).group(1).strip()
        else:
            announcement_code = ''

        litigant = re.search(r'((技投诉律师|被调査会员|被处分人|北京嘉盾律师事务所|被投诉会员|被处分会员|北京市北元律师事务所律师王振)'
                             r'[\s\S]*?)'
                             r'(2015年12月30日,北京市第三中级人民法院[\s\S]*?向北京市司法局发出|'
                             r'苏州市第四看守所投诉称|'
                             r'2016年1月14日,宋先利\(以下称“投诉人”\)来函反|本会受理投诉人张|'
                             r'因违反《中华人民共和国律师法》的相关规定|2015年9月21日,本会收到上海市司法局行政处罚决定|'
                             r'二、事实和证据|经查|因犯合同诈骗罪被北京市东城区人民法院判处)',
                             content_text).group(1).strip()
        facts = re.search(r'((2015年12月30日,北京市第三中级人民法院[\s\S]*?向北京市司法局发出|经查|经调查|'
                          r'苏州市第四看守所投诉称|经调查查明|2016年12月28日,北京嘉盾律师事务所|本会调查査明|'
                          r'因犯合同诈骗罪被北京市东城区人民法院判处)'
                          r'[\s\S]*?)'
                          r'(根据本会查明的事实,柱慧律师在从事专职律师期间|本会认为|以上事实有下列证据予以证实|'
                          r'依据上述行政处罚决定和中华全国律师协会|を明的上述事实有王本人陈述、提交的授权委托书|'
                          r'上述事实,有|以上事实,有)',
                          content_text).group(1).strip()
        real_publish_date = format_date(each_document['publishDate'].split(' ')[0])
        try:
            punishment_basis = re.search(r'((根据本会查明的事实,柱慧律师在从事专职律师期间|本会认为)'
                                         r'[\s\S]*?)'
                                         r'(综上,依据《律师执业管理办法》第四十七条|'
                                         r'根据《律师协会会员违规行为处分规则\(试行\)》第十|'
                                         r'为了规范体师执业行为,维护律师职业形象,根据中华人|'
                                         r'根据《中\n华全国律师协会会员违规行为处分规则\(试行》第十三条|'
                                         r'按《律师协会会员违规行为处分规则\(试行\)》第九条|'
                                         r'根据《律师协会会员违规行为处分)',
                                         content_text).group(1).strip()
        except Exception as e:
            logger.warning(e)
            punishment_basis = ''
        punishment_decision = re.search(r'((综上,依据《律师执业管理办法》第四十七条|'
                                        r'根据《律师协会会员违规行为处分规则\(试行\)》第十|'
                                        r'为了规范体师执业行为,维护律师职业形象,根据中华人|'
                                        r'依据上述行政处罚决定和中华全国律师协会|'
                                        r'经本会研究后认为|'
                                        r'根据《律师协会会员违规行为处分规则\(试行》第十|'
                                        r'根据《中\n华全国律师协会会员违规行为处分规则\(试行》第十三条|'
                                        r'按《律师协会会员违规行为处分规则\(试行\)》第九条|'
                                        r'根据《律师协会会员违规行为处分)'
                                        r'[\s\S]*?)\n'
                                        r'(被处分的会员如不服本决定|由扫描全能王扫描创建|被处分人如不服本处分决定|'
                                        r'二0械年月に思|宁夏律师协会|如不服本处分决定|主题词:律师行业纪律处分法定|'
                                        r'依据《广东省律师协会会员违规行为处分实施细则》第)',
                                        content_text).group(1).strip()

        result_map = {
            'announcementTitle': announcement_title,
            'announcementOrg': announcement_org,
            'announcementDate': real_publish_date,
            'announcementCode': announcement_code,
            'facts': facts,
            'defenseOpinion': '',
            'defenseResponse': '',
            'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
            'punishmentBasement': punishment_basis,
            'punishmentDecision': punishment_decision,
            'type': '行政处罚决定',
            'oss_file_id': file_id,
            'status': 'not checked'
        }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('全国律师协会 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('全国律师协会 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('全国律师协会 数据解析 ' + ' -- 修改parsed完成')


# 辽宁省律协
def liaoning_parse():
    for each_document in db.lawyers_data.find({'origin': '辽宁省律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title']

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')
        content_text = get_content_text(content_soup.find(class_='sideline_blue_RLTB'))

        if '撤销行政许可决定书' in announcement_title:
            if db.parsed_data.find(
                    {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                oss_file_map = {
                    'origin_url': announcement_url,
                    'oss_file_origin_url': announcement_url,
                    'origin_url_id': each_document['_id'],
                    'oss_file_type': 'html',
                    'oss_file_name': announcement_title,
                    'oss_file_content': content_response.text,
                    'parsed': False,
                    'content_class_name': 'sideline_blue_RLTB'
                }
                response = db.parsed_data.insert_one(oss_file_map)
                file_id = response.inserted_id
                oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                             content_response.text)
                db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            else:
                db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
                file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                   'oss_file_origin_url': announcement_url})['_id']

            litigant = re.search(r'撤销行政许可决定书（(.*?)）', announcement_title).group(1).strip()
            document_code = re.search(r'撤销行政许可决定书\n([\s\S]*?)\n' + litigant + '：', content_text).group(1).strip()
            facts = re.search(litigant + r'：\n([\s\S]*?)\n依据', content_text).group(1).strip()
            punishment_decision = re.search(r'\n(依据[\s\S]*?)\n如你不服本决定', content_text).group(1).strip()
            publish_date = re.search(r'\n辽宁省司法厅\n(\d{4}年\d+月\d+日)$', content_text).group(1).strip()

            result_map = {
                'announcementTitle': announcement_title,
                'announcementOrg': '辽宁律师协会',
                'announcementDate': publish_date,
                'announcementCode': document_code,
                'facts': facts,
                'defenseOpinion': '',
                'defenseResponse': '',
                'litigant': litigant,
                'punishmentBasement': '',
                'punishmentDecision': punishment_decision,
                'type': '撤销许可',
                'oss_file_id': file_id,
                'status': 'not checked'
            }
            logger.info(result_map)
            if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
                db.announcement.insert_one(result_map)
                logger.info('辽宁省律协 数据解析 ' + ' -- 数据导入完成')
            else:
                logger.info('辽宁省律协 数据解析 ' + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('辽宁省律协 数据解析 ' + ' -- 修改parsed完成')
        else:
            if '.docx' in content_text:
                href = content_soup.find_all(class_='sideline_blue_RLTB')[0].find_all('a')[-1].attrs['href']
                docx_url = urljoin(announcement_url, href)
                response = requests.get(docx_url)

                with open('./test/tmp.' + 'docx', 'wb') as tmp_file:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            tmp_file.write(chunk)

                with open('./test/tmp.docx', 'rb') as docx_file:
                    docx_content = docx_file.read()

                if db.parsed_data.find({'origin_url': announcement_url, 'oss_file_origin_url': docx_url}).count() == 0:
                    oss_file_map = {
                        'origin_url': announcement_url,
                        'oss_file_origin_url': docx_url,
                        'origin_url_id': each_document['_id'],
                        'oss_file_type': 'docx',
                        'oss_file_name': announcement_title,
                        'oss_file_content': docx_content,
                        'parsed': False
                    }
                    response = db.parsed_data.insert_one(oss_file_map)
                    file_id = response.inserted_id
                    oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.docx', docx_content)
                    db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
                else:
                    db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
                    file_id = db.parsed_data.find_one(
                        {'origin_url': announcement_url, 'oss_file_origin_url': docx_url})['_id']

                document = Document('./test/tmp.docx')
                content_text = '\n'.join([each_paragraph.text for each_paragraph in document.paragraphs])

                result_map_list = []
                publish_date = format_date(re.search(r'(\n *\d{4}年\d{1,2}月\d{1,2}日$)', content_text).group(1).strip())
                content_text = re.sub(r'\n *\d{4}年\d{1,2}月\d{1,2}日$', '', content_text).strip()
                for each_punishment_text in re.split('.、', content_text)[1:]:
                    litigant = re.search(r'^([\s\S]*?)因违反', each_punishment_text.strip()).group(1).strip()
                    facts = re.search(r'(因违反.*?[，；])(.*?(受到|收到|根据))', each_punishment_text.strip()).group(
                        1).strip()
                    punishment_decision = re.search(r'因违反.*?[，；]([^，；]*?(受到|收到|根据).*$)',
                                                    each_punishment_text.strip()).group(1).strip()
                    announcement_code = re.search(r'((沈律协处字|大律纪字|营律协惩处|抚律惩字).\d{4}.第?\d+号?)',
                                                  each_punishment_text.strip()).group(1).strip()
                    result_map = {
                        'announcementTitle': announcement_title,
                        'announcementOrg': '辽宁律师协会',
                        'announcementDate': publish_date,
                        'announcementCode': announcement_code,
                        'facts': facts,
                        'defenseOpinion': '',
                        'defenseResponse': '',
                        'litigant': litigant,
                        'punishmentBasement': '',
                        'punishmentDecision': punishment_decision,
                        'type': '纪律处分',
                        'oss_file_id': file_id,
                        'status': 'not checked'
                    }
                    logger.info(result_map)
                    result_map_list.append(result_map)
                if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
                    db.announcement.insert_many(result_map_list)
                    logger.info('辽宁省律协 数据解析 ' + ' -- 数据导入完成')
                    logger.info('辽宁省律协 数据解析 导入%d条数据' % len(result_map_list))
                else:
                    logger.info('辽宁省律协 数据解析 ' + ' -- 数据已经存在')
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('辽宁省律协 数据解析 ' + ' -- 修改parsed完成')


# 湖南省律协
def hunan_parse():
    for each_document in db.lawyers_data.find({'origin': '湖南省律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title']

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')
        content_text = get_content_text(content_soup.find(class_='text'))

        if db.parsed_data.find(
                {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_document['_id'],
                'oss_file_type': 'html',
                'oss_file_name': announcement_title,
                'oss_file_content': content_response.text,
                'parsed': False,
                'content_class_name': 'text'
            }
            response = db.parsed_data.insert_one(oss_file_map)
            file_id = response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                         content_response.text)
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']

        if re.search(r'^(.*?)\n被处分人', content_text):
            document_code = re.search(r'^(.*?)\n被处分人', content_text).group(1).strip()
        else:
            document_code = ''
        litigant = re.search(r'(被处分人.*?\n)', content_text).group(1).strip()
        publish_date = format_date(re.findall(r'(\d{4}年\d+月\d+日)', content_text)[-1])
        facts = re.search(litigant + r'([\s\S]*?)((根据|依据).*?第.*?条.*?规定|本会认为)',
                          content_text).group(1).strip()
        punishment_decision = re.search(r'(((根据|依据).*?第.*?条的规定.*?(本会.*?决定|作出行业处分决定如下)|本会认为)'
                                        r'[\s\S]*?)(被处分人如对.*?(处分|处理)决定不服)',
                                        content_text).group(1).strip()

        result_map = {
            'announcementTitle': '湖南省律协处分决定书（' +
                                 document_code + '）' if document_code != '' else '湖南省律协处分决定书',
            'announcementOrg': '湖南律师协会',
            'announcementDate': publish_date,
            'announcementCode': document_code,
            'facts': facts,
            'defenseOpinion': '',
            'defenseResponse': '',
            'litigant': litigant,
            'punishmentBasement': '',
            'punishmentDecision': punishment_decision,
            'type': '纪律处分',
            'oss_file_id': file_id,
            'status': 'not checked'
        }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('湖南省律协 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('湖南省律协 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('湖南省律协 数据解析 ' + ' -- 修改parsed完成')


# 浙江省律协
def zhejiang_parse():
    for each_document in db.lawyers_data.find({'origin': '浙江省律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title'].replace('\u3000', '')

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        # ignored
        if announcement_title.strip() in ['李爱军（中止会员权利三个月，温州）']:
            logger.warning('url has nothing to do with punishment ...')
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('update lawyers data success')
            continue

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')

        if '中止会员权利' in announcement_title:
            announcement_type = '纪律处分'
        elif '取消会员资格' in announcement_title:
            announcement_type = '纪律处分'
        elif '公开谴责' in announcement_title:
            announcement_type = '公开谴责'
        else:
            announcement_type = ''

        pdf_list = [urljoin(announcement_url, kk.attrs['href'])
                    for kk in content_soup.find(class_='m-info').findAll('a')
                    if 'href' in kk.attrs.keys() and str(kk.attrs['href']).endswith('download')]
        img_link_list = [urljoin(announcement_url, kk.attrs['src'])
                         for kk in content_soup.find(class_='m-info').find_all('img')
                         if 'src' in kk.attrs.keys() and str(kk.attrs['src']).endswith('png')]
        if len(pdf_list) == 1:
            pdf_link = pdf_list[0]
            if db.parsed_data.find({'origin_url': announcement_url, 'oss_file_origin_url': pdf_link}).count() == 0:
                response = request_site_page(pdf_link)
                if response is None:
                    logger.error('网页请求错误')
                    return
                with open('./test/tmp.pdf', 'wb') as tmp_file:
                    for chunk in response.iter_content(chunk_size=1024):
                        if chunk:
                            tmp_file.write(chunk)
                with open('./test/tmp.pdf', 'rb') as pdf_file:
                    pdf_content = pdf_file.read()
                content_text, ocr_flag = pdf_ocr_to_text('./test/tmp.pdf')
                logger.info('删除TMP文件')
                if os.path.exists('./test/tmp.pdf'):
                    os.remove('./test/tmp.pdf')
                oss_file_map = {
                    'origin_url': announcement_url,
                    'oss_file_origin_url': pdf_link,
                    'origin_url_id': each_document['_id'],
                    'oss_file_type': 'pdf',
                    'oss_file_name': announcement_title,
                    'oss_file_content': pdf_content,
                    'parsed': False,
                    'if_ocr': True,
                    'ocr_result': content_text
                }
                response = db.parsed_data.insert_one(oss_file_map)
                file_id = response.inserted_id
                oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.pdf', pdf_content)
                db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            else:
                db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
                file_id = db.parsed_data.find_one({'origin_url': announcement_url, 'oss_file_origin_url': pdf_link})[
                    '_id']
                content_text = db.parsed_data.find_one(
                    {'origin_url': announcement_url, 'oss_file_origin_url': pdf_link})['ocr_result']
        elif len(img_link_list) > 1:
            if db.parsed_data.find(
                    {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                for index, each_img_link in enumerate(img_link_list):
                    img_response = request_site_page(each_img_link)
                    with open('./test/' + str(index) + '.png', 'wb') as tmp_file:
                        for chunk in img_response.iter_content(chunk_size=1024):
                            if chunk:
                                tmp_file.write(chunk)

                if not os.path.exists('./test/tmp.pdf'):
                    shell_str = 'img2pdf '
                    for index in range(len(img_link_list)):
                        shell_str += './test/' + str(index) + '.png '
                    shell_str += '-o  ./test/tmp.pdf'
                    process = subprocess.Popen(shell_str.replace(r'(', r'\(').replace(r')', r'\)'),
                                               shell=True, stdout=subprocess.PIPE)
                    process.communicate()

                result_text, ocr_flag = pdf_ocr_to_text('./test/tmp.pdf')

                with open('./test/tmp.pdf', 'rb') as pdf_file:
                    pdf_content = pdf_file.read()

                oss_file_map = {
                    'origin_url': announcement_url,
                    'oss_file_origin_url': announcement_url,
                    'origin_url_id': each_document['_id'],
                    'oss_file_type': 'pdf',
                    'oss_file_name': announcement_title,
                    'oss_file_content': pdf_content,
                    'parsed': False,
                    'if_ocr': True,
                    'ocr_result': result_text
                }
                response = db.parsed_data.insert_one(oss_file_map)
                file_id = response.inserted_id
                oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.pdf', pdf_content)
                db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
                content_text = result_text

                logger.info('删除TMP文件')
                if os.path.exists('./test/tmp.pdf'):
                    os.remove('./test/tmp.pdf')
                for index in range(len(img_link_list)):
                    if os.path.exists('./test/' + str(index) + '.png'):
                        os.remove('./test/' + str(index) + '.png')
            else:
                db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
                file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                   'oss_file_origin_url': announcement_url})['_id']
                content_text = db.parsed_data.find_one({'origin_url': announcement_url,
                                                        'oss_file_origin_url': announcement_url})['ocr_result']
        else:
            if db.parsed_data.find(
                    {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
                oss_file_map = {
                    'origin_url': announcement_url,
                    'oss_file_origin_url': announcement_url,
                    'origin_url_id': each_document['_id'],
                    'oss_file_type': 'html',
                    'oss_file_name': announcement_title,
                    'oss_file_content': content_response.text,
                    'parsed': False,
                    'content_class_name': 'm-info'
                }
                response = db.parsed_data.insert_one(oss_file_map)
                file_id = response.inserted_id
                oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                             content_response.text)
                db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            else:
                db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
                file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                   'oss_file_origin_url': announcement_url})['_id']
            content_soup.style.decompose()
            content_text = get_content_text(content_soup.find(class_='m-info'))

        logger.info(content_text)
        content_text = content_text.replace('偉', '律').replace('査', '查')

        document_code = re.search(r'(处分决定书|处分決定书)\n(.*?)\n(被调查会员|被处分人|投诉人|当事人|投 诉 人|被欠分人)',
                                  content_text).group(2).strip()
        litigant = re.search(r'((被调查会员|被处分人|投诉人|当事人|投 诉 人|被欠分人)'
                             r'([\s\S]*?))\n'
                             r'(.*?本会.*?(接到|收到)|'
                             r'.*?一案[\s\S]*?(立案并已调查完毕|经初步调査|立案,现已调査完毕|予\n以立案调查|立案并已调査完毕)|'
                             r'.*?作出.*认定|'
                             r'根据.*?有关规定|'
                             r'投诉人兰溪市人民检察院|'
                             r'.*?予以立案调查。经金华市司法局查明|'
                             r'浙江五义律师事务所曾永土律师违反律师执业规范|'
                             r'关于浙江游龙律师事务所律师童小明“在会见在押被告|'
                             r'\d{4}年\d{1,2}月\d{1,3}日.*?(人民法院作出|本会收到)|'
                             r'2017年10月31日晚,史忠兴醉酒驾车与其他车辆发生|'
                             r'2018年2月8日,胡桥因违反了《中华人民共和国律师|'
                             r'关于浙江省司法厅.*?律师向司法行政部|'
                             r'杨志囊律师向司法行政机关提交虚假材料或者有其他弄|'
                             r'关于张旭强律师私自收取费用一案)',
                             content_text).group(1).strip()

        facts = re.search(r'((经查|经.*?查明|现已查明|具体违法事实有|\n.*?作出.*认定|经査|经.*?査明|'
                          r'\n\d{4}年\d{1,2}月\d{1,3}日.*?(人民法院作出|本会收到)|'
                          r'2017年10月31日晚,史忠兴醉酒驾车与其他车辆发生|'
                          r'杨志囊律师向司法行政机关提交虚假材料或者有其他弄|'
                          r'经本案调査组查明的事实如下|'
                          r'2018年2月8日,胡桥因违反了《中华人民共和国律师|'
                          r'.*?一案[\s\S]*?(立案并已调查完毕|经初步调査|立案,现已调査完毕|予\n以立案调查|立案并已调査完毕))'
                          r'[\s\S]*?)'
                          r'\n(本会认为|我会认为|证明以上事实的证据材料|以上事实认定|根据《中华人民共和国律师法》的相关规定|'
                          r'依据上述行政处罚决定和中华全国律师协会|以上事实的证据材料有|'
                          r'依据中华全国律师协会《律师协会会员违规行为处分规|'
                          r'证明以上事实的证据材料有|'
                          r'依据上述行政处罚决定及中华全国律师协会《律师协会|'
                          r'以上事实.有)',
                          content_text).group(1).strip()

        if '申辩' in content_text:
            if re.search(
                    r'(一、投诉与申辩|\n.*?律师答辩称：|被调查会员叶伟荣的答辩意见：|谢可培在第一份陈述中申辩：)'
                    r'([\s\S]*?)'
                    r'(二、调查事实与听证|\n经查|本会于.*?一案予以立案调查|我会认为)', content_text):
                defense = re.search(
                    r'(一、投诉与申辩|\n.*?律师答辩称：|被调查会员叶伟荣的答辩意见：|谢可培在第一份陈述中申辩：)'
                    r'([\s\S]*?)'
                    r'(二、调查事实与听证|\n经查|本会于.*?一案予以立案调查|我会认为)', content_text).group(2).strip()
                if re.search(
                        r'(三、本会立场观点与处分理由)([\s\S]*?)(四、处分决定)', content_text):
                    defense_response = re.search(
                        r'(三、本会立场观点与处分理由)([\s\S]*?)(四、处分决定)', content_text).group(2).strip()
                else:
                    defense_response = ''
            else:
                if re.search(r'(李良才律师放弃了对本案的陈述、申辩和听证的权利)', content_text):
                    defense = re.search(r'(李良才律师放弃了对本案的陈述、申辩和听证的权利)', content_text).group(1).strip()
                    defense_response = ''
                else:
                    defense = defense_response = ''
        else:
            defense = defense_response = ''

        try:
            punishment_basement = re.search(
                r'((本协?会认为|金华市司法局认为|我会认为)[\s\S]*?)'
                r'(为此.根据|\n本协会依据|李良才律师放弃了|为此.依据|'
                r'现根据|根据中华全国律师协会《律师协会会员违规行为处分规|'
                r'现依据中华全国律师协会《律师协会会员违规行为处分|综上,根据|'
                r'现依据《律师法》第四十六条第\(六\)项、中华全国律|'
                r'现依据《律师法》第四十六条（六）项及中华全国律师协会《律师协会会员违规|'
                r'综上，根据上述查明的事实、证据和处分依据|'
                r'现依据《律师法》第.*?条.*?规定)',
                content_text).group(1).strip()
        except Exception as e:
            logger.warning(e)
            punishment_basement = ''
        punishment_decision = re.search(r'(((为此.根据|本协会依据|根据|现依据|依据).*?第.*?条.*规定.*?'
                                        r'(作出如下处分决定|对.*?作出.*?行政处罚|我会决定|本会决定如下|本会纪律与惩戒委员会决定)|'
                                        r'综上，根据上述查明的事实、证据和本会的立场观点，按照《处分规则（试行）》|'
                                        r'为此，根据.*?第.*?条(第.*?款)?第.*?项，本会纪律与惩戒委员会作出如下处分决定|'
                                        r'为惩戒和教育其本人，市律协纪律惩戒委员会根据.*?第.*?条.*?规定，决定给予|'
                                        r'根据《中华人民共和国律师法》的相关规定，浙江省司法厅给予吊销|'
                                        # ocr规则
                                        r'为此,(根据|依据)中华全国律师协会《律师协会会员违规行为|'
                                        r'现根据《律师协会会员违规行为处分规则\(试行\)》|'
                                        r'根据中华全国律师协会《律师协会会员违规行为处分规\n|'
                                        r'现依据中华全国律师协会《律师协会会员违规行为处分|'
                                        r'依据中华全国律师协会《律师协会会员违规行为处分规|'
                                        r'综上,根据上述查明的事实、证据和处分依据,按照|'
                                        r'依据上述行政处罚决定及中华全国律师协会《律师协会|'
                                        r'现依据《律师法》第四十六条第\(六\)项、中华全国律)'
                                        r'[\s\S]*?)'
                                        r'([^。\n]*?如不服本会纪律与惩戒委员会作出的(上述)?(处分)?决定|'
                                        r'[^。\n]*?如不服本处分决定|浙江省律师协会|'
                                        r'[^。\n]*?如对本会纪律与惩戒委员会作出的决定不服|'
                                        r'如不服本处分决定|宁波市律师称会|'
                                        r'中止会员权利的处分期限相应于停止执业行政处罚期|'
                                        r'满江省律师协会)',
                                        content_text).group(1).strip()

        announcement_org = re.search(r'(^|\n)(浙江省)?(.*?市律师协会|.*?市律师协|浙江省律师协会)\n?'
                                     r'(纪律处分決定书|纪律处分决定书|溯州市律师协会|处分决定书|行业处分决定书)',
                                     content_text).group(3).strip()
        announcement_org = announcement_org.replace(r'浙江省律师协会', r'浙江律师协会').strip()
        try:
            publish_date = format_date(re.findall(r'(\d{4}年\d+月\d+日)', content_text)[-1])
        except Exception as e:
            logger.warning(e)
            publish_date = format_date(each_document['publishDate'])

        result_map = {
            'announcementTitle': announcement_title,
            'announcementOrg': announcement_org,
            'announcementDate': publish_date,
            'announcementCode': document_code,
            'facts': facts,
            'defenseOpinion': defense,
            'defenseResponse': defense_response,
            'litigant': litigant,
            'punishmentBasement': punishment_basement,
            'punishmentDecision': punishment_decision,
            'type': announcement_type,
            'oss_file_id': file_id,
            'status': 'not checked'
        }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('浙江省律协 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('浙江省律协 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('浙江省律协 数据解析 ' + ' -- 修改parsed完成')


# 广西自治区律协
def guangxi_parse():
    for each_document in db.lawyers_data.find({'origin': '广西壮族自治区律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title']

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        # ignored
        if '广西壮族自治区司法厅公告' == announcement_title:
            logger.warning('url has nothing to do with punishment ...')
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('update lawyers data success')
            continue

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')

        if db.parsed_data.find(
                {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_document['_id'],
                'oss_file_type': 'html',
                'oss_file_name': announcement_title,
                'oss_file_content': content_response.text,
                'parsed': False,
                'content_class_name': 'info'
            }
            response = db.parsed_data.insert_one(oss_file_map)
            file_id = response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                         content_response.text)
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']

        content_text = get_content_text(content_soup.find(class_='info'))

        if '通报批评' in announcement_title:
            announcement_type = '通报批评'
        elif '中止会员权利' in announcement_title:
            announcement_type = '纪律处分'
        elif '公开谴责' in announcement_title:
            announcement_type = '公开谴责'
        elif '实施行政处罚' in announcement_title:
            announcement_type = '行政处罚决定'
        elif '撤销对其律师执业许可' in announcement_title:
            announcement_type = '撤销许可'
        else:
            announcement_type = '行政处罚决定'

        if re.search(r'((桂律警示|桂司通).\d{4}.\d+号)', content_text):
            document_code = re.search(r'((桂律警示|桂司通).\d{4}.\d+号)', content_text).group(1).strip()
        else:
            document_code = ''

        try:
            litigant = re.search(r'((被处分会员)[\s\S]*?)'
                                 r'(经查|.*?(广西壮族自治区司法厅|南宁市司法局)依法对|'
                                 r'投诉人胡某就房屋租赁和债务纠纷问题，委托|'
                                 r'2015年7月8日，投诉人苏京荣向广西律师协会递交《投诉书》)',
                                 content_text).group(1).strip()
        except Exception as e:
            logger.warning(e)
            litigant = re.search(r'现将有关情况通报如下：\n(.*?)'
                                 r'(因冒用他人学历报名参加全国律师资格考试被有关单位和群众举报|'
                                 r'2016年7月，南宁市司法局接到广西玉林市某公司投诉陈家鸿律师违规收取律师费、服务不尽职等问题。)',
                                 content_text).group(1).strip()

        document_code = document_code.replace('\n', '').replace(' ', '')

        facts = re.search(
            litigant + r'([\s\S]*?)'
                       r'([^。\n]*?行为(已经|同时)?(违反|，属于)|[^。\n]*?依据.*?第.*?条的规定|'
                       r'根据司法部《律师事务所年度检查考核办法》相关规定)',
            content_text).group(1).strip()

        try:
            punishment_basement = re.search(
                r'[\n。]([^。\n]*?行为(已经|同时)?(违反|，属于)[\s\S]*?)'
                r'(根据.*?第.*?条.*?规定.*?决定给予|'
                r'广西律师协会.*?决定给予|'
                r'南宁市律师协会.*?决定给予|'
                r'经有关程序，梧州市律师协会根据|'
                r'梧州市律师协会.*?决定给予|'
                r'经本会惩戒委员会常务委员会会议研究，决定|'
                r'经广西律师协会研究，决定给予)', content_text).group(1).strip()
        except Exception as e:
            logger.warning(e)
            punishment_basement = ''

        punishment_decision = re.search(r'(((根据|依据).*?第.*?条.*?规定.*?(决定给予|确认)|广西律师协会.*?决定给予|'
                                        r'南宁市律师协会.*?决定给予|梧州市律师协会.*?决定给予|'
                                        r'根据司法部《律师事务所年度检查考核办法》相关规定|'
                                        r'经有关程序，梧州市律师协会根据|'
                                        r'经本会惩戒委员会常务委员会会议研究，决定|'
                                        r'经广西律师协会研究，决定给予)'
                                        r'[\s\S]*?)\n'
                                        r'(广西壮族自治区律师协会|覃永沛无视国家法律法规和政策|'
                                        r'百举鸣所及陈家鸿律师的违法违规行为|希望广大律师以此为鉴|'
                                        r'希望全区广大律师以此为鉴)',
                                        content_text).group(1).strip()

        if re.search(r'(\n\d{4}年\d{1,2}月 *\d{1,2} *日($|\n))', content_text):
            publish_date = re.search(r'(\n\d{4}年\d{1,2}月 *\d{1,2} *日($|\n))', content_text).group(1).strip()
        else:
            publish_date = format_date(each_document['publishDate'])
        publish_date = publish_date.replace(' ', '')

        result_map = {
            'announcementTitle': announcement_title,
            'announcementOrg': '广西律师协会',
            'announcementDate': publish_date,
            'announcementCode': document_code,
            'facts': facts,
            'defenseOpinion': '',
            'defenseResponse': '',
            'litigant': litigant,
            'punishmentBasement': punishment_basement,
            'punishmentDecision': punishment_decision,
            'type': announcement_type,
            'oss_file_id': file_id,
            'status': 'not checked'
        }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('广西自治区律协 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('广西自治区律协 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('广西自治区律协 数据解析 ' + ' -- 修改parsed完成')


# 黑龙江省律协
def heilongjiang_parse():
    for each_document in db.lawyers_data.find({'origin': '黑龙江省律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title']

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')

        # EXCEL文件处理
        if len(content_soup.find(class_='temmainm').find_all('a')) == 1:
            # 解析excel格式文件
            xlsx_url = urljoin(announcement_url, content_soup.find(class_='temmainm').find_all('a')[0].attrs['href'])
            link_type = xlsx_url.split('.')[-1]
            response = request_site_page(xlsx_url)

            with open('./test/tmp.' + link_type, 'wb') as tmp_file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        tmp_file.write(chunk)

            with open('./test/tmp.' + link_type, 'rb') as xlsx_file:
                xlsx_content = xlsx_file.read()

            if db.parsed_data.find(
                    {'origin_url': announcement_url, 'oss_file_origin_url': xlsx_url}).count() == 0:
                oss_file_map = {
                    'origin_url': announcement_url,
                    'oss_file_origin_url': xlsx_url,
                    'origin_url_id': each_document['_id'],
                    'oss_file_type': link_type,
                    'oss_file_name': announcement_title,
                    'oss_file_content': xlsx_content,
                    'parsed': False
                }
                response = db.parsed_data.insert_one(oss_file_map)
                file_id = response.inserted_id
                oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.' + link_type, xlsx_content)
                db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            else:
                db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
                file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                                   'oss_file_origin_url': xlsx_url})['_id']

            # try:
            excel_data = open_workbook('./test/tmp.' + link_type)
            result_map_list = []
            for i in range(excel_data.nsheets):
                sheet = excel_data.sheets()[i]
                for each_line in sheet._cell_values:
                    if len(each_line) != 6:
                        continue
                    if each_line[0] in ['律师基本信息', '律所基本信息']:
                        litigant = ''
                    else:
                        if each_line[0] in \
                                ['中文名称：', '英文名称：', '许可机关：', '发证日期：', '组织形式：', '当前执业状态：',
                                 '最近年度考核状态：', '身份证号：',
                                 '姓名：', '性别：', '民族：', '籍贯：', '政治面貌：', '出生年月：', '文化程度：',
                                 '电话：', '传真：', '邮箱：', '执业所在地区：', '执业律所：', '办公地址：'] \
                                and each_line[1] != '' and each_line[1] != '无':
                            if type(each_line[1]) == float:
                                try:
                                    real_date = xldate_as_tuple(each_line[1], excel_data.datemode)
                                    real_date = str(real_date[0]) + '年' + str(real_date[1]) + '月' + str(
                                        real_date[2]) + '日'
                                    litigant += str(each_line[0]).strip() + str(real_date).strip() + '\n'
                                except Exception as e:
                                    logger.warning(e)
                                    litigant += str(each_line[0]).strip() + str(int(each_line[1])).strip() + '\n'
                            else:
                                litigant += str(each_line[0]).strip() + str(each_line[1]).strip() + '\n'

                        if each_line[2] in \
                                ['负责人（主任）：', '传真：', '办公地址：', '邮箱：', '网址：', '最近考核日期：',
                                 '律师资格证号：', '资格证取得时间：', '许可机关：', '发证日期：', '最近年度考核状态：',
                                 '最近年度考核日期：', '考核年度：'
                                 ] \
                                and each_line[3] != '' and each_line[3] != '无':
                            if type(each_line[3]) == float:
                                try:
                                    real_date = xldate_as_tuple(each_line[3], excel_data.datemode)
                                    real_date = str(real_date[0]) + '年' + str(real_date[1]) + '月' + str(
                                        real_date[2]) + '日'
                                    litigant += str(each_line[2]).strip() + str(real_date).strip() + '\n'
                                except Exception as e:
                                    logger.warning(e)
                                    litigant += str(each_line[2]).strip() + str(int(each_line[3])).strip() + '\n'
                            else:
                                litigant += str(each_line[2]).strip() + str(each_line[3]).strip() + '\n'

                        if each_line[4] in \
                                ['考核年度：'] \
                                and each_line[5] != '' and each_line[5] != '无':
                            if type(each_line[5]) == float:
                                try:
                                    real_date = xldate_as_tuple(each_line[5], excel_data.datemode)
                                    real_date = str(real_date[0]) + '年' + str(real_date[1]) + '月' + str(
                                        real_date[2]) + '日'
                                    litigant += str(each_line[4]).strip() + str(real_date).strip() + '\n'
                                except Exception as e:
                                    logger.warning(e)
                                    litigant += str(each_line[4]).strip() + str(int(each_line[5])).strip() + '\n'
                            else:
                                litigant += str(each_line[4]).strip() + str(each_line[5]).strip() + '\n'

                        if each_line[0] in \
                                ['执业许可证号：', '许可证流水号：', '手机：', '邮编：'
                                 ] \
                                and each_line[1] != '' and each_line[1] != '无':
                            litigant += str(each_line[0]).strip() + str(int(each_line[1])).strip() + '\n'

                        if each_line[2] in \
                                ['执业许可证号：', '许可证流水号：', '邮编：', '律师执业证号：'
                                 ] \
                                and each_line[2] != '' and each_line[2] != '无':
                            litigant += str(each_line[2]).strip() + str(int(each_line[3])).strip() + '\n'

                        if re.search(r'.*?(律师协会|司法局)', str(each_line[1])):
                            if type(each_line[0]) == float:
                                cer_date = xldate_as_tuple(each_line[0], excel_data.datemode)
                                publish_date = str(cer_date[0]) + '年' + str(cer_date[1]) + '月' + str(cer_date[2]) + '日'
                            else:
                                publish_date = format_date(each_line[0])
                            announcement_org = each_line[1]
                            facts = each_line[2]
                            if re.search(r'^(根据|依据)', each_line[3]):
                                punishment_decision = each_line[3].replace(r'\n', '').strip() + '给予当事人' + each_line[4]
                            else:
                                punishment_decision = '根据' + each_line[3].replace(r'\n', '').strip() + '给予当事人' + \
                                                      each_line[4]

                            result_map = {
                                'announcementTitle': announcement_title,
                                'announcementOrg': announcement_org,
                                'announcementDate': publish_date,
                                'announcementCode': '',
                                'facts': facts,
                                'defenseOpinion': '',
                                'defenseResponse': '',
                                'litigant': litigant.strip(),
                                'punishmentBasement': '',
                                'punishmentDecision': punishment_decision,
                                'type': '行业自律监管',
                                'oss_file_id': file_id,
                                'status': 'not checked'
                            }
                            logger.info(result_map)
                            if db.announcement.find(
                                    {'announcementTitle': announcement_title, 'oss_file_id': file_id,
                                     'litigant': litigant}).count() == 0:
                                result_map_list.append(result_map)
                            else:
                                logger.info('广西自治区律协 数据解析 ' + ' -- 数据已经存在')
            if len(result_map_list) > 0:
                logger.info('黑龙江省律协' + '解析 -- 一共有%d条数据' % len(result_map_list))
                db.announcement.insert_many(result_map_list)
                logger.info('黑龙江省律协' + '解析 -- 数据导入完成')
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('黑龙江省律协' + '解析 -- 修改parsed完成')
            else:
                logger.warning('黑龙江省律协' + '解析 -- 解析未能完成')

            logger.info('删除TMP文件')
            if os.path.exists('./test/tmp.xls'):
                os.remove('./test/tmp.xls')

            if os.path.exists('./test/tmp.xlsx'):
                os.remove('./test/tmp.xlsx')


# 重庆市律协
def chongqing_parse():
    for each_document in db.lawyers_data.find({'origin': '重庆市律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title']

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')

        if db.parsed_data.find(
                {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_document['_id'],
                'oss_file_type': 'html',
                'oss_file_name': announcement_title,
                'oss_file_content': content_response.text,
                'parsed': False,
                'content_class_name': 'news_detail_c'
            }
            response = db.parsed_data.insert_one(oss_file_map)
            file_id = response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                         content_response.text)
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']

        content_text = get_content_text(content_soup.find(class_='news_detail_c'))
        litigant = re.search(
            r'^([\s\S]*?)(经查|因同时在|因私自|在未取得|因在停止执业期|因犯|因向|在未取得委托人授权情况)',
            content_text).group(1).strip()

        facts = re.search(r'((经查|因同时在|因私自|在未取得|因在停止执业期|因犯|因向|在未取得委托人授权情况)[\s\S]*?)'
                          r'((\d{4}年\d+月\d+日，)?((被)?[^\n。；，]*?)?(根据|依据).*?第.*?条.*?规定|'
                          r'(其行为)?违反.*?第.*?条.*?规定|'
                          r'重庆市云阳县司法局认为|'
                          r'(被)?[^\n。；，]*?于\d{4}年\d+月\d+日(作出)?给予|'
                          r'，被重庆市渝中区司法局于)',
                          content_text).group(1).strip()

        punishment_basis_str_list = [
            '([^\n。；，]*?)违反.*?规定',
            '重庆市云阳县司法局认为.*?造成了不良影响'
        ]
        punishment_basis_str = '|'.join(punishment_basis_str_list)
        punishment_basis_compiler = re.compile(r'[。\n；，]' + '(' + punishment_basis_str + ')' +
                                               r'.(\n?依据|\n?根据|\n?鉴于|重庆市.*?于.*?给予)', re.MULTILINE)
        punishment_basis_list = punishment_basis_compiler.findall(content_text)
        punishment_basis = '；'.join([kk[0].strip() for kk in punishment_basis_list])

        punishment_decision = re.search(
            r'(((\d{4}年\d+月\d+日，)?([^\n。；，]*?)?(根据|依据).*?第.*?条.*?规定|'
            r'(被)?[^\n。；，]*?于\d{4}年\d+月\d+日(作出)?给予|'
            r'被重庆市渝中区司法局于)'
            r'[\s\S]*?)$', content_text).group(1).strip()
        try:
            publish_date = re.findall(r'\d{4}年\d{1,2}月\d{1,3}日', punishment_decision)[-1].strip()
            real_publish_date = format_date(publish_date)
        except Exception as e:
            logger.warning(e)
            real_publish_date = format_date(each_document['publishDate'])
        result_map = {
            'announcementTitle': announcement_title,
            'announcementOrg': '重庆律师协会',
            'announcementDate': real_publish_date,
            'announcementCode': '',
            'facts': facts,
            'defenseOpinion': '',
            'defenseResponse': '',
            'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
            'punishmentBasement': punishment_basis,
            'punishmentDecision': punishment_decision,
            'type': '纪律处分',
            'oss_file_id': file_id,
            'status': 'not checked'
        }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('重庆市律协 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('重庆市律协 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('重庆市律协 数据解析 ' + ' -- 修改parsed完成')


# 上海市律协
def shanghai_parse():
    for each_document in db.lawyers_data.find({'origin': '上海市律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title'].replace('\u3000', '')

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        # ignored
        if announcement_title.strip() in ['深交所发布《创业板上市公司公开谴责标准》', '广东拟规定工会可公开谴责不作为企业']:
            logger.warning('url has nothing to do with punishment ...')
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('update lawyers data success')
            continue

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')

        if db.parsed_data.find(
                {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_document['_id'],
                'oss_file_type': 'html',
                'oss_file_name': announcement_title,
                'oss_file_content': content_response.text,
                'parsed': False,
                'content_class_name': 'info'
            }
            response = db.parsed_data.insert_one(oss_file_map)
            file_id = response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                         content_response.text)
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']

        content_text_soup = content_soup.find(class_='info')
        content_text_soup.style.decompose()
        content_text_soup.script.decompose()
        content_text = get_content_text(content_text_soup)

        if '处分决定书' in announcement_title:
            announcement_type = '处分决定'
            announcement_org = '上海律师协会'
            announcement_code = re.search(r'案号：(沪律纪案（\d{4}）第\d+号)\n被处分人', content_text).group(1).strip()
            litigant = re.search(r'\n(被处分人.*?)\n.*?上海市第一中级人民法院.*?向上海市司法局发出',
                                 content_text).group(1).strip()
            facts = re.search(r'经调查查明：\n([\s\S]*?)\n以上事实有下列证据予以证实', content_text).group(1).strip()
            punishment_basis = re.search(r'本会认为：\n([\s\S]*?)\n鉴于，在本案调查中', content_text).group(1).strip()
            punishment_decision = re.search(r'(鉴于.*?根据.*?第.*?条.*?规定，决定如下[\s\S]*?)分享到：QQ空间新浪微博微信',
                                            content_text).group(1).strip()
            real_publish_date = format_date(each_document['publishDate'])
        elif '行政处罚决定书' in announcement_title:
            announcement_type = '行政处罚决定'
            announcement_org = '上海律师协会'
            announcement_code = re.search(r'(沪司罚决字.\d{4}.第\d+号)', content_text).group(1).strip()
            litigant = re.search(r'(当事人.*?)经查', content_text).group(1).strip()
            facts = re.search(r'(经查.*?)以上事实.*?等证据证明属实', content_text).group(1).strip()
            punishment_basis = re.search(r'本机关认为.(.*?)鉴于当事人', content_text).group(1).strip()
            punishment_decision = re.search(r'(鉴于.*?依照.*?第.*?条.*?规定，决定处罚如下.*?)如不服本处罚决定',
                                            content_text).group(1).strip()
            real_publish_date = format_date(re.findall(r'.{4}年.{1,2}月.{1,3}日', content_text)[-1].strip())
        else:
            announcement_type = '公开谴责'
            announcement_code = ''
            real_publish_date = format_date(each_document['publishDate'])
            if '深交所' in announcement_title:
                facts = re.search(r'(经查.*?)\n万家乐既未在上述事实发生时及时履行临时报告义务', content_text).group(1).strip()
                announcement_org = '深交所'
                litigant = re.search(r' (.*?)遭深交所公开谴责', announcement_title).group(1).strip()
                punishment_decision = re.search(r'(鉴于万家乐的行为严重违反了有关规定，深交所决定.*?)\n',
                                                content_text).group(1).strip()
                punishment_basis = re.search(r'(万家乐既未在上述事实发生时及时履行临时报告义务.*?)鉴于万家乐的行为严重违反了有关规定',
                                             content_text).group(1).strip()
            else:
                facts = re.search(r'(太平洋百货不让供应商进入其它商店)', content_text).group(1).strip()
                announcement_org = '全国贸联会'
                litigant = re.search(r'全国贸联会公开谴责(.*?)变相垄断', announcement_title).group(1).strip()
                punishment_decision = re.search(r'(由全国19家百货大集团组成的全国贸联会22日公开谴责.*?)\n贸联会成员近日在专门召开',
                                                content_text).group(1).strip()
                punishment_basis = re.search(r'(贸联会成员近日在专门召开的“市场竞争与市场秩序”研讨会上.*?)\n',
                                             content_text).group(1).strip()

        result_map = {
            'announcementTitle': announcement_title,
            'announcementOrg': announcement_org,
            'announcementDate': real_publish_date,
            'announcementCode': announcement_code,
            'facts': facts,
            'defenseOpinion': '',
            'defenseResponse': '',
            'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
            'punishmentBasement': punishment_basis,
            'punishmentDecision': punishment_decision,
            'type': announcement_type,
            'oss_file_id': file_id,
            'status': 'not checked'
        }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('上海市律师协会 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('上海市律师协会 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('上海市律师协会 数据解析 ' + ' -- 修改parsed完成')


# 深圳市律协
def shenzhen_parse():
    for each_document in db.lawyers_data.find({'origin': '深圳市律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title']

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        if announcement_title.strip() in ['深圳市律师诚信档案管理办法']:
            logger.warning('url has nothing to do with punishment ...')
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'ignored'}})
            logger.info('update lawyers data success')
            continue

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')

        if db.parsed_data.find(
                {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_document['_id'],
                'oss_file_type': 'html',
                'oss_file_name': announcement_title,
                'oss_file_content': content_response.text,
                'parsed': False,
                'content_class_name': 'info '
            }
            response = db.parsed_data.insert_one(oss_file_map)
            file_id = response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                         content_response.text)
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']

        content_text = get_content_text(content_soup.find(class_='info '))
        logger.info(content_text)
        announcement_code = re.search(r'((深司罚决字|深律纪处?字|粤律协处字|粤司罚决字).\d{4}.\d+号)',
                                      announcement_title).group(1).strip()
        litigant_list = re.findall(r'\n(\d{4}年\d{1,2}月(\d{1,3}日)?.)?(.*?)'
                                   r'(经查|[^，。\n]*在.*?(任职律师期间|执业期间|案件期间|纠纷案件中|担任授薪律师期间|任专职律师期间|管理松懈)|'
                                   r'广东粤商律师事务所于2016年4月11日从登记办公地址|'
                                   r'广东丽景律师事务所办公场所地址变更|'
                                   r'广东瑞仁律师事务所擅自变更办公地点|'
                                   r'广东文功律师事务在收取律师费一年之后才开具律师费发票|'
                                   r'[^，。\n]*(事务所|律师)(疏于管理|未经批准|在[^，。\n]*?所有权纠纷案件中)|'
                                   r'与卓某签订《委托辩护合同》|'
                                   r'北京市中银（深圳）律师事务所为完成律师事务所年度考核手续|'
                                   r'2017年5月3日，深圳市罗湖区人民法院作出|'
                                   r'广东省东莞市中级人民法院作出|'
                                   r'广东[^，。\n]*?律师事务所存在[^，。\n]*?行为|'
                                   r'北京金诚同达（深圳）律师事务所在未与委托人签订|'
                                   r'陈勇律师作为一名执业律师|'
                                   r'\n广东度道律师事务所存在刑事案件风险收费|'
                                   r'广东怀明律师事务所在与胡某建立委托代理关系后|'
                                   r'2015年6月18日，广东信荣律师事务所接受陈某的委托|'
                                   r'广东利人律师事务所在明知没有与当事人签订代理合同的情况下|'
                                   r'在为汪某亲友提供刑事案件二审代理服务过程中|'
                                   r'在担任赵某某刑事案件辩护人过程中|'
                                   r'在代理.*?(一案中|案件中|过程中|一审中)|'
                                   r'在实习期间以公民身份代理案件|'
                                   r'在HC公司为客户进行法律服务过程中|'
                                   r'私自收取当事人大额费用且未依法出具合法票据|'
                                   r'未经当事人许可|接受深圳市法律援助处指派|'
                                   r'在办理见证业务过程中存在对一房二卖进行律师见证|'
                                   r'违规为当时尚未取得律师执业证书的王彤彤提供执业便利|'
                                   r'在明知律师事务所出具的《律师事务所所函》中表述自己为执业律师的情况与事实不符|'
                                   r'2011年，广东国晖律师事务所曹抚全律师作为执业律师|'
                                   r'\d{4}年(\d+月)?(\d+日)?.*?(服务中|接受.*?委托|实习期间以公民身份代理诉讼|'
                                   r'以非律师身份参加庭审|明知房屋为违法建筑|违反.*?规定|作出刑事裁定)|'
                                   r'原广东海利律师事务所现广东朗正律师事务所王孝春律师在代理|'
                                   r'为违法建筑交易行为提供(律师)?见证服务|'
                                   r'在代理岗夏彩福大厦商铺124位业主商铺租赁买卖合同纠纷案中)',
                                   content_text)
        litigant = '\n'.join([kk[2] for kk in litigant_list])
        if litigant == '':
            litigant = re.search(r'.*?号(.*?)被给予', announcement_title).group(1).strip()
        punishment_decision_list = re.findall(r'((深圳市司法局于\n?\d{4}年\d{1,2}月\d{1,2}日\n?决定|'
                                              r'广东省司法厅于\n?\d{4}年\d{1,2}月\d{1,2}日\n?决定|'
                                              r'深圳市司法局于\n2017年9月27日\n决定|'
                                              r'2016年9月21日，根据《深圳市律师协会会员违规行为处分细则》六条第（一）项之规定|'
                                              r'根据《深圳市律师协会会员违规行为处分细则》.*?条第.*?项之规定|'
                                              r'鉴于.*?律师态度诚恳，积极配合本会调查处理，本会决定对其予以减轻处分。|'
                                              r'根据《深圳市律师协会会员违规行为处分细则》七条第（二）项，本会决定|'
                                              r'(\d{4}年\d{1,2}月\d{1,2}日.)?(本会)?(依据|根据).*?第.*?条.*?(规定)?，'
                                              r'(\d{4}年\d{1,2}月\d{1,2}日.)?本会决定|'
                                              r'\d{4}年\d{1,2}月\d{1,2}日.本会决定对.*?予以|'
                                              r'深圳市司法局决定对.*?予以|'
                                              r'2019年1月9日，广东省律师协会决定给予|'
                                              r'深圳市司法局于\d{4}年\d{1,2}月\d{1,2}日对.*?作出|'
                                              r'根据中华全国律师协会《律师协会会员违规行为处分规则（试行）》第十一条第二十八款、第十四条第二十三款的规定|'
                                              r'((依据|根据).*?第.*?条.*?(规定)?.)?\d{4}年\d{1,2}月\d{1,2}日.深圳市律师协会决定，?对|'
                                              r'根据《深圳市律师协会会员违纪违规行为处分细则》第六条第十五项之规定，2015年6月8日，本会对)'
                                              r'.*?)'
                                              r'($|\n)',
                                              content_text)
        punishment_decision = ''
        for kk in punishment_decision_list:
            punishment_decision += kk[0].replace('\n', '').strip() + '\n'
        punishment_decision = punishment_decision.strip()

        facts_list = []
        for index, each_litigant in enumerate(litigant_list):
            facts_list.append(re.search(each_litigant[2].replace(r'(', r'\(').replace(r')', r'\)') + '\n?(.*?)' +
                                        punishment_decision_list[index][0].replace(r'(', r'\(').replace(r')', r'\)'),
                                        content_text).group(1).strip())
        facts = '\n'.join(facts_list)

        if facts == '':
            facts = re.search(r'(^|\n)(.*?)' + punishment_decision.replace(r'(', r'\(').replace(r')', r'\)'),
                              content_text).group(2).strip()

        publish_date = re.findall(r'\d{4}年\d{1,2}月\d{1,3}日', content_text)[-1].strip()
        real_publish_date = format_date(publish_date)

        if '行政处罚' in announcement_title:
            announcement_type = '行政处罚决定'
        elif re.search('吊销.*?执业证', announcement_title):
            announcement_type = '吊销执业证书'
        elif re.search('取消.*?会员资格行业处分', announcement_title):
            announcement_type = '取消会员资格'
        else:
            announcement_type = '行政处罚决定'

        result_map = {
            'announcementTitle': announcement_title,
            'announcementOrg': '深圳律师协会',
            'announcementDate': real_publish_date,
            'announcementCode': announcement_code,
            'facts': facts,
            'defenseOpinion': '',
            'defenseResponse': '',
            'litigant': litigant,
            'punishmentBasement': '',
            'punishmentDecision': punishment_decision,
            'type': announcement_type,
            'oss_file_id': file_id,
            'status': 'not checked'
        }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('深圳市律师协会 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('深圳市律师协会 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('深圳市律师协会 数据解析 ' + ' -- 修改parsed完成')


# 山东省律协
def shandong_parse():
    for each_document in db.lawyers_data.find({'origin': '山东省律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title']

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')

        iframe_url = urljoin(announcement_url, content_soup.find(id='aaaa').attrs['src'])
        iframe_response = request_site_page(iframe_url)
        if iframe_response is None:
            logger.error('网页请求错误 %s' % iframe_url)
            continue
        iframe_response.encoding = iframe_response.apparent_encoding
        iframe_content_soup = BeautifulSoup(iframe_response.text, 'lxml')
        if db.parsed_data.find(
                {'origin_url': announcement_url, 'oss_file_origin_url': iframe_url}).count() == 0:
            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': iframe_url,
                'origin_url_id': each_document['_id'],
                'oss_file_type': 'html',
                'oss_file_name': announcement_title,
                'oss_file_content': iframe_response.text,
                'parsed': False,
                'content_class_name': 'Section1'
            }
            response = db.parsed_data.insert_one(oss_file_map)
            file_id = response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                         iframe_response.text)
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': iframe_url})['_id']

        if '律师行业处分情况通报' in announcement_title:
            result_map_list = []
            table_content = iframe_content_soup.find(class_='MsoNormalTable')
            for each_tr in table_content.find_all('tr'):
                td_list = [kk.text.strip() for kk in each_tr.find_all('td')]
                if td_list[0] == '被处分会员':
                    continue
                each_title = announcement_title + '（' + td_list[0] + '）'
                each_real_publish_date = format_date(each_document['publishDate'])
                each_facts = td_list[2]
                each_litigant = '被处分会员：' + td_list[0] + '\n执业机构：' + td_list[1]
                each_punishment_decision = td_list[4]

                result_map = {
                    'announcementTitle': each_title,
                    'announcementOrg': td_list[3],
                    'announcementDate': each_real_publish_date,
                    'announcementCode': '',
                    'facts': each_facts,
                    'defenseOpinion': '',
                    'defenseResponse': '',
                    'litigant': each_litigant,
                    'punishmentBasement': '',
                    'punishmentDecision': each_punishment_decision,
                    'type': '行业处分决定',
                    'oss_file_id': file_id,
                    'status': 'not checked'
                }
                logger.info(result_map)
                if db.announcement.find(
                        {'litigant': each_litigant, 'punishmentDecision': each_punishment_decision,
                         'facts': each_facts}).count() == 0:
                    result_map_list.append(result_map)
                else:
                    logger.info('山东省律师协会 数据解析 ' + ' -- 数据已经存在')
            if len(result_map_list) > 0:
                logger.info('山东省律师协会解析 -- 一共有%d条数据' % len(result_map_list))
                db.announcement.insert_many(result_map_list)
                logger.info('山东省律师协会解析 -- 数据导入完成')
                db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
                logger.info('山东省律师协会解析 -- 修改parsed完成')
            else:
                logger.warning('山东省律师协会解析 -- 解析未能完成')
        elif '处罚发表声明' in announcement_title:
            content_text = get_content_text(iframe_content_soup.find(class_='Section1'))
            litigant = re.search(r'山东省司法厅拟对(.*?)律师进行行政处罚', content_text).group(1).strip()
            real_publish_date = format_date(each_document['publishDate'])
            facts = re.search(r'行政处罚发表以下声明：([\s\S]*?)山东省律师协会支持', content_text).group(1).strip()
            punishment_decision = re.search(r'(山东省律师协会支持.*?)$', content_text).group(1).strip()

            result_map = {
                'announcementTitle': announcement_title,
                'announcementOrg': '山东律师协会',
                'announcementDate': real_publish_date,
                'announcementCode': '',
                'facts': facts,
                'defenseOpinion': '',
                'defenseResponse': '',
                'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
                'punishmentBasement': '',
                'punishmentDecision': punishment_decision,
                'type': '处罚声明',
                'oss_file_id': file_id,
                'status': 'not checked'
            }
            logger.info(result_map)
            if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
                db.announcement.insert_one(result_map)
                logger.info('山东律师协会 数据解析 ' + ' -- 数据导入完成')
            else:
                logger.info('山东律师协会 数据解析 ' + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('山东律师协会 数据解析 ' + ' -- 修改parsed完成')
        else:
            result_map = {
                'announcementTitle': announcement_title,
                'announcementOrg': '山东律师协会',
                'announcementDate': '',
                'announcementCode': '',
                'facts': '',
                'defenseOpinion': '',
                'defenseResponse': '',
                'litigant': '',
                'punishmentBasement': '',
                'punishmentDecision': '',
                'type': '公开谴责',
                'oss_file_id': file_id,
                'status': 'not checked'
            }
            logger.info(result_map)
            if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
                db.announcement.insert_one(result_map)
                logger.info('山东律师协会 数据解析 ' + ' -- 数据导入完成')
            else:
                logger.info('山东律师协会 数据解析 ' + ' -- 数据已经存在')
            db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
            logger.info('山东律师协会 数据解析 ' + ' -- 修改parsed完成')


# 甘肃省律师协会
def gansu_parse():
    for each_document in db.lawyers_data.find({'origin': '甘肃省律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title']

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')

        content_text = get_content_text(content_soup.find(id='ContentDetail'))

        if db.parsed_data.find(
                {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_document['_id'],
                'oss_file_type': 'html',
                'oss_file_name': announcement_title,
                'oss_file_content': content_response.text,
                'parsed': False,
                'content_id_name': 'ContentDetail'
            }
            response = db.parsed_data.insert_one(oss_file_map)
            file_id = response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                         content_response.text)
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']

        logger.info(content_text)

        litigant = re.search(r'((被处分人|被处罚人)[\s\S]*?)'
                             r'(现查明|一、投诉与申辩|经查|'
                             r'2016年5月23日，甘肃省司法厅向甘肃省律师协会转来|'
                             r'2016年5月3日，白银市人民检察院刑事执行检察局向甘肃省司法厅律师管理处寄来|'
                             r'2016年1月6日，投诉人向甘肃省律协提交书面投诉信)',
                             content_text).group(1).strip()
        if '一、投诉与申辩' in content_text:
            facts = re.search(r'二、调查情况及查明的事实([\s\S]*?)三、处分理由[和及]依据',
                              content_text).group(1).strip()
        else:
            facts = re.search(litigant +
                              r'([\s\S]*?)'
                              r'(以上事实有.*?为证。|现有.*?等证据材料在案为证。|证明上述事实的证据有|证明以上事实的证据有)',
                              content_text).group(1).strip()
        try:
            punishment_basis = re.search(
                r'(\n(根据以上事实和证据，)?本(会|机关)认为[\s\S]*?)'
                r'(经(本会惩戒委员会|甘肃省司法厅)\d{4}年\d+月\d+日.*?(会议讨论并表决|集体研究)|四、处分决定|为此，根据)',
                content_text).group(1).strip()
        except Exception as e:
            logger.warning(e)
            punishment_basis = re.search(r'三、处分理由和依据([\s\S]*?)四、处分决定', content_text).group(1).strip()
        try:
            punishment_decision = re.search(
                r'(经(本会惩戒委员会|甘肃省司法厅)\d{4}年\d+月\d+日.*?(会议讨论并表决|集体研究).决定[\s\S]*?|'
                r'为此，根据.*?第.*?条.*?规定[\s\S]*?)'
                r'(如不服本(处分|处罚)?决定|'
                r'如对上述(处分)?决定不服)',
                content_text).group(1).strip()
        except Exception as e:
            logger.warning(e)
            punishment_decision = re.search(
                r'四、处分决定([\s\S]*?)(如不服本(处分)?决定|如对上述(处分)?决定不服)',
                content_text).group(1).strip()
        if '申辩' in content_text:
            defense = re.search(r'一、投诉与申辩([\s\S]*?)二、调查情况及查明的事实', content_text).group(1).strip()
            defense_response = ''
        else:
            defense = defense_response = ''

        try:
            publish_date = re.findall(r'\d{4}年\d{1,2}月\d{1,3}日', content_text)[-1].strip()
            real_publish_date = format_date(publish_date)
        except Exception as e:
            logger.warning(e)
            real_publish_date = format_date(each_document['publishDate'])
        result_map = {
            'announcementTitle': announcement_title,
            'announcementOrg': '甘肃律师协会',
            'announcementDate': real_publish_date,
            'announcementCode': '',
            'facts': facts,
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
        if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('甘肃省律师协会 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('甘肃省律师协会 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('甘肃省律师协会 数据解析 ' + ' -- 修改parsed完成')


# 海南省律师协会
def hainan_parse():
    for each_document in db.lawyers_data.find({'origin': '海南省律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title']

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')

        content_text = get_content_text(content_soup.find(class_='news_view_n'))

        if db.parsed_data.find(
                {'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_document['_id'],
                'oss_file_type': 'html',
                'oss_file_name': announcement_title,
                'oss_file_content': content_response.text,
                'parsed': False,
                'content_class_name': 'news_view_n'
            }
            response = db.parsed_data.insert_one(oss_file_map)
            file_id = response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.html',
                         content_response.text)
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
        else:
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']

        logger.info(content_text)

        announcement_code = re.search(r'海南省律师协会\n(.*?)\n海南省律师协会', content_text).group(1).strip()
        litigant = re.search(r'(被处分会员[\s\S]*?)\n(\d{4}年\d{1,2}月\d{1,3}日)',
                             content_text).group(1).strip()
        facts = re.search(litigant + r'([\s\S]*?)(以上事实有.*?等证据证实。|根据.*?第十七条.*?规定，本会决定)',
                          content_text).group(1).strip()
        try:
            defense = re.search(r'([^，。\n]*?在规定的期限内未提出申请听证，放弃了听证权利。)',
                                content_text).group(1).strip()
            defense_response = ''
        except Exception as e:
            logger.warning(e)
            defense = defense_response = ''

        try:
            punishment_basis = re.search(r'(综上所述，本会纪律委员会认为[\s\S]*?)'
                                         r'(\d{4}年\d{1,2}月\d{1,3}日，本会向.*?发出了听证告知书)',
                                         content_text).group(1).strip()
        except Exception as e:
            logger.warning(e)
            punishment_basis = ''

        punishment_decision = re.search(r'(\n(对于.*?违规行为，)?根据.*?第.*?条.*?规定，本会决定[\s\S]*?)(如不服本处分决定)',
                                        content_text).group(1).strip()

        try:
            publish_date = re.findall(r'\n\d{4}年\d{1,2}月\d{1,3}日\n', content_text)[-1].strip()
            real_publish_date = format_date(publish_date)
        except Exception as e:
            logger.warning(e)
            real_publish_date = format_date(each_document['publishDate'])

        result_map = {
            'announcementTitle': announcement_title,
            'announcementOrg': '海南律师协会',
            'announcementDate': real_publish_date,
            'announcementCode': announcement_code,
            'facts': facts,
            'defenseOpinion': defense,
            'defenseResponse': defense_response,
            'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
            'punishmentBasement': punishment_basis,
            'punishmentDecision': punishment_decision,
            'type': '行业处分决定',
            'oss_file_id': file_id,
            'status': 'not checked'
        }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('海南省律师协会 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('海南省律师协会 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('海南省律师协会 数据解析 ' + ' -- 修改parsed完成')


# 广东省律师协会
def guangdong_parse():
    for each_document in db.lawyers_data.find({'origin': '广东省律师协会', 'status': {'$nin': ['ignored']}}):

        announcement_url = each_document['url']
        announcement_title = each_document['title']

        # 判断是否解析过
        if db.lawyers_data.find({'url': announcement_url, 'status': 'parsed'}).count() == 1 and db.parsed_data.find(
                {'origin_url': announcement_url, 'parsed': True}).count() == 1:
            continue

        logger.info('url to parse ' + announcement_url)

        content_response = request_site_page(announcement_url)
        if content_response is None:
            logger.error('网页请求错误 %s' % announcement_url)
            continue
        content_response.encoding = content_response.apparent_encoding
        content_soup = BeautifulSoup(content_response.text, 'lxml')

        if db.parsed_data.find({'origin_url': announcement_url, 'oss_file_origin_url': announcement_url}).count() == 0:
            img_link_list = [urljoin(announcement_url, kk.attrs['src'])
                             for kk in content_soup.find(class_='article-content').find_all('img')]

            for index, each_img_link in enumerate(img_link_list):
                img_response = request_site_page(each_img_link)
                with open('./test/' + str(index) + '.jpg', 'wb') as tmp_file:
                    for chunk in img_response.iter_content(chunk_size=1024):
                        if chunk:
                            tmp_file.write(chunk)

            if not os.path.exists('./test/tmp.pdf'):
                shell_str = 'img2pdf '
                for index in range(len(img_link_list)):
                    shell_str += './test/' + str(index) + '.jpg '
                shell_str += '-o  ./test/tmp.pdf'
                process = subprocess.Popen(shell_str.replace(r'(', r'\(').replace(r')', r'\)'),
                                           shell=True, stdout=subprocess.PIPE)
                process.communicate()

            result_text, ocr_flag = pdf_ocr_to_text('./test/tmp.pdf')

            with open('./test/tmp.pdf', 'rb') as pdf_file:
                pdf_content = pdf_file.read()

            oss_file_map = {
                'origin_url': announcement_url,
                'oss_file_origin_url': announcement_url,
                'origin_url_id': each_document['_id'],
                'oss_file_type': 'pdf',
                'oss_file_name': announcement_title,
                'oss_file_content': pdf_content,
                'parsed': False,
                'if_ocr': True,
                'ocr_result': result_text
            }
            response = db.parsed_data.insert_one(oss_file_map)
            file_id = response.inserted_id
            oss_add_file(ali_bucket, str(file_id) + '/' + announcement_title + '.pdf', pdf_content)
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            content_text = result_text

            logger.info('删除TMP文件')
            if os.path.exists('./test/tmp.pdf'):
                os.remove('./test/tmp.pdf')
            for index in range(len(img_link_list)):
                if os.path.exists('./test/' + str(index) + '.jpg'):
                    os.remove('./test/' + str(index) + '.jpg')
        else:
            db.lawyers_data.update_one({'_id': each_document['_id']}, {'$set': {'status': 'parsed'}})
            file_id = db.parsed_data.find_one({'origin_url': announcement_url,
                                               'oss_file_origin_url': announcement_url})['_id']
            content_text = db.parsed_data.find_one({'origin_url': announcement_url,
                                                    'oss_file_origin_url': announcement_url})['ocr_result']

        logger.info(content_text)
        if '行政处罚决定' in announcement_title:
            announcement_type = '行政处罚决定'
        else:
            announcement_type = '行业处分决定'
        content_text = content_text.replace('被处分入', '被处分人').replace('!月', '1月')
        announcement_code = re.search(r'处[分罚]决定书\n(.*?)\n(被处分人|当事人)',
                                      content_text).group(1).strip()
        litigant = re.search(r'((被处分人|当事人)[\s\S]*?)(本会收到广东省司法厅|本机关经查发现)',
                             content_text).group(1).strip()
        facts = re.search(litigant.replace(r'(', r'\(').replace(r')', r'\)') +
                          r'([\s\S]*?)(基于上述(查明的)?事实,根据|上述事实,有)',
                          content_text).group(1).strip()
        try:
            punishment_basis = re.search(r'(本机关认为[\s\S]*?)结合当事人违法行为的事实、性质、情节、社会危程度和相关',
                                         content_text).group(1).strip()
        except Exception as e:
            logger.warning(e)
            punishment_basis = ''

        punishment_decision = re.search(r'((结合当事人违法行为的事实、性质、情节、社会危程度和相关|'
                                        r'基于上述(查明的)?事实,根据)'
                                        r'[\s\S]*?)'
                                        r'(被处分人如不服本处分决定|当事人如不服本决定)',
                                        content_text).group(1).strip()

        try:
            publish_date = re.search(r'(\n\d{4}年\d{1,2}月\d{1,3}日)印发$', content_text).group(1).strip()
            real_publish_date = format_date(publish_date)
        except Exception as e:
            logger.warning(e)
            real_publish_date = format_date(each_document['publishDate'])

        result_map = {
            'announcementTitle': announcement_title,
            'announcementOrg': '广东律师协会',
            'announcementDate': real_publish_date,
            'announcementCode': announcement_code,
            'facts': facts,
            'defenseOpinion': '',
            'defenseResponse': '',
            'litigant': litigant[:-1] if litigant[-1] == '：' else litigant,
            'punishmentBasement': punishment_basis,
            'punishmentDecision': punishment_decision,
            'type': announcement_type,
            'oss_file_id': file_id,
            'status': 'not checked'
        }
        logger.info(result_map)
        if db.announcement.find({'announcementTitle': announcement_title, 'oss_file_id': file_id}).count() == 0:
            db.announcement.insert_one(result_map)
            logger.info('广东省律师协会 数据解析 ' + ' -- 数据导入完成')
        else:
            logger.info('广东省律师协会 数据解析 ' + ' -- 数据已经存在')
        db.parsed_data.update_one({'_id': file_id}, {'$set': {'parsed': True}})
        logger.info('广东省律师协会 数据解析 ' + ' -- 修改parsed完成')


def parse_all():
    acla_parse()
    liaoning_parse()
    hunan_parse()
    zhejiang_parse()
    guangxi_parse()
    heilongjiang_parse()
    chongqing_parse()
    shanghai_parse()
    shenzhen_parse()
    shandong_parse()
    gansu_parse()
    hainan_parse()
    guangdong_parse()


if __name__ == "__main__":
    parse_all()
