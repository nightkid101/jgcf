import re
from pymongo import MongoClient
from init import config_init, logger_init
from pyhanlp import *
from bson import ObjectId

config = config_init()
logger = logger_init('解析当事人')


def demo_chinese_name_recognition(sentence):
    """ 中国人名识别
    """
    segment = HanLP.newSegment().enableNameRecognize(True).enableOrganizationRecognize(
        True).enableJapaneseNameRecognize(True)
    return segment.seg(sentence)


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

# 公司职位列表
role_list = ['股权受让方', '签字注册会计师', '2014年年审注册会计师', '时任法人代表', '财务顾问', '投资经理', '董秘', '运维部负责人',
             '董事长', '董事会秘书', '实际控制人', '独立董事', '副总经理', '董事兼副总经理', '总工程师', '内容中心总编辑',
             '董事', '第一大股东', '法人代表', '法定代表人', '一致行动人', '主要负责人', '副局长兼国资办主任', '交易主管',
             '总裁', '控股股东', '实际控制人', '总经理', '财务总监', '审计委员会召集人', '持续督导负责人', '记者', '部长',
             '证券事务代表', '监事', '一般股东', '高级管理人员', '签字律师', '员工', '行长', '财务经理', '首席分析师', '研究员',
             '2015年审会计师', '关联方', '审计委员会主任委员', '代持股东', '主管人员', '工作人员', '签字会计师', '签字注册评估师',
             '监事会主席', '上市公司', '总会计师', '财务工作负责人', '资产评估师', '经理助理', '签字主办人', '办公室主任',
             '重组项目负责人', '重大资产重组独立财务顾问齐鲁证券有限公司项目主办人', '首席财务官', '中介机构及其相关人员',
             '副董事长', '首席执行官', '控制权受让方', '下属全资子公司', '财务部副总裁', '合规风控负责人', '产品经理',
             '股东', '首次公开发行股份并上市保荐代表人', '间接控股股东', '职工董事', '董监高', '主管', '营业部负责人',
             '财务负责人', '可转换公司债券持有人', '重大资产重组独立财务顾问主办人', '权益变动信息披露义务人', '常务副总经理',
             '财务处长', '上市公司董监高', '其他人员', '董事会', '高管', '财务顾问主办人', '前台负责人', '中心负责人',
             '项目主办人', '相关当事人', '财务部经理', '总经济师', '投资部经理', '事务所注册会计师', '研究所所长',
             '保荐代表人', '主办券商', '2012年至2014年9月审计报告的签字注册会计师', '投资发展部部长', '董事、经理',
             '执行董事', '投行部业务三部负责人', '投资总监', '钢铁中心金融部部长', '投资部负责人', '团队负责人', '发行部副经理',
             '董事长助理', '董事长特别助理', '渠道经理', '顾问', '交易员', '精算责任人', '中介机构', '信息披露负责人']
# 人物关系列表
person_role_list = ['配偶', '朋友', '妻子', '之女', '之子', '之妹', '之妻', '之夫', '之弟', '表叔', '表弟']


# get final person or legal map
def parse_person_legal(litigant_info_list):
    person_info_list = []
    legal_person_info_list = []
    for kk in litigant_info_list:
        if kk.get('person_name', '') != '':
            each_final_person_info = {
                "person_name": "",
                "gender": "",
                "age": "",
                "birthday": "",
                "nationality": "",
                "nation": "",
                "home_address": "",
                "identity_number": "",
                "employment_list": [
                    {
                        "employment_period": "",
                        "working_company": "",
                        "position_role": ""
                    }
                ],
                "practice_category": "",
                "registration_number": "",
                "certificate_number": "",
                "license_number": "",
                "enrollment_number": "",
                "person_code": "",
                "registration_time": "",
                "certificate_time": "",
                "relationship": "",
                "hk_ma_tw_number": "",
                "passport_number": "",
                "office_address": ""
            }
            for each_key in kk.keys():
                if each_key not in ['employment_period', 'working_company', 'position_role']:
                    each_final_person_info[each_key] = kk[each_key]
                else:
                    each_final_person_info['employment_list'][0]['employment_period'] = kk.get('employment_period', '')
                    each_final_person_info['employment_list'][0]['working_company'] = kk.get('working_company', '')
                    each_final_person_info['employment_list'][0]['position_role'] = kk.get('position_role', '')
            person_info_list.append(each_final_person_info)
        else:
            each_final_legal_info = {
                "a_stock_code": "",
                "a_stock_name": "",
                "b_stock_code": "",
                "b_stock_name": "",
                "neeq_stock_code": "",
                "neeq_stock_name": "",
                "company_name": "",
                "involved_company_list": [
                    {
                        "involved_company_name": "",
                        "position_role": ""
                    }
                ],
                "register_address": "",
                "home_address": "",
                "office_address": "",
                "legal_representative": "",
                "principal": "",
                "person_code": "",
                "business_registration_number": "",
                "registration_number": "",
                "agency_code": "",
                "registration_time": "",
                "involved_object": "",
                "legal_representative_number_id": "",
                "establishment_date": ""
            }
            for each_key in kk.keys():
                if each_key not in ['involved_company_name', 'position_role']:
                    each_final_legal_info[each_key] = kk[each_key]
                else:
                    each_final_legal_info['involved_company_list'][0]['involved_company_name'] = kk.get(
                        'involved_company_name', '')
                    each_final_legal_info['involved_company_list'][0]['position_role'] = kk.get('position_role', '')
            legal_person_info_list.append(each_final_legal_info)
    return person_info_list, legal_person_info_list


# 保监机构当事人解析
def parse_circ(litigant):
    litigant = litigant.replace(' ', '').replace('\xa0', '').strip()
    if litigant == '':
        return [], []
    litigant_info_list = []
    if str(litigant).startswith('组织机构代码'):
        litigant = litigant.split('\n')[1] + '\n' + litigant.split('\n')[0] + '\n' + '\n'.join(litigant.split('\n')[2:])
    logger.info(litigant)
    par_list = ['当事人姓名', '当事人名称', '受处罚机构名称', '受处罚单位名称', '受处罚人名称', '处罚个人姓名',
                r'受处罚人：\n姓名', '受处罚人员姓名', r'受处罚人\(个人\)', '兼业代理人名称', '受处罚人（机构）',
                r'受处罚人\n姓名', r'受处罚人\(公民\)：\n姓名', '受处罚单位名称', '受处罚人（公民）',
                '受处罚人姓名', r'受处罚人\(公民\)', '当事人', '受处罚人', '被处罚单位名称', '被处罚人姓名',
                '受处罚单位', '机构名称', '受处罚机构']
    info_par_list = ['\n', '；', '。', '，']
    if re.search(r'^[\s\S]*?' + r'(' + r'|'.join(par_list) + r')[\s\S]*?$', litigant):

        litigant_list = ['当事人' + kk for kk in re.split('(' + '|'.join(par_list) + ')', litigant) if
                         kk not in par_list and kk != '']

        for each_litigant in litigant_list:
            if '当事人：' not in each_litigant:
                continue
            each_litigant = re.sub('当事人：\n', '当事人：', each_litigant)
            each_litigant_map = {}
            each_litigant_info_list = re.split('(' + '|'.join(info_par_list) + ')', each_litigant)
            for each_info in each_litigant_info_list:
                if each_info in info_par_list:
                    continue
                # 名称
                if re.search(r'^(' + '|'.join(par_list) + r')[：:]?(.*)$', each_info):
                    litigant_name = re.search(r'^(' + '|'.join(par_list) + r')[：:]?(.*?)$', each_info).group(
                        2).strip()
                    chinese_name_recognition_list = demo_chinese_name_recognition(litigant_name)
                    com_flag = 0
                    for kk in chinese_name_recognition_list:
                        if str(kk).endswith('/nt'):
                            com_flag = 1
                    if com_flag == 1 or '公司' in litigant_name:
                        each_litigant_map['company_name'] = re.sub('^名称[:：]?', '', litigant_name)
                    else:
                        each_litigant_map['person_name'] = re.sub('^姓名[:：]?', '', litigant_name)

                # 住所地址
                address_list = ['住所', '住址', '地址', '住所', '机构地址']
                if re.search(r'^(' + '|'.join(address_list) + r')[：:]?(.*)$', each_info):
                    address = re.search(r'^(' + '|'.join(address_list) + r')[：:]?(.*)$', each_info).group(2).strip()
                    each_litigant_map['home_address'] = address

                # 办公地址
                working_address_list = ['营业地址']
                if re.search(r'^(' + '|'.join(working_address_list) + r')[：:]?(.*)$', each_info):
                    working_address = re.search(r'^(' + '|'.join(working_address_list) + r')[：:]?(.*)$',
                                                each_info).group(
                        2).strip()
                    each_litigant_map['office_address'] = working_address

                # 注册地址
                register_address_list = ['注册地址']
                if re.search(r'^(' + '|'.join(register_address_list) + r')[：:]?(.*)$', each_info):
                    register_address = re.search(r'^(' + '|'.join(register_address_list) + r')[：:]?(.*)$',
                                                 each_info).group(
                        2).strip()
                    each_litigant_map['register_address'] = register_address

                # 就职公司(单位)
                working_company_list = ['单位']
                if re.search(r'^(' + '|'.join(working_company_list) + r')[：:]?(.*)$', each_info):
                    working_company = re.search(r'^(' + '|'.join(working_company_list) + r')[：:]?(.*)$',
                                                each_info).group(2).strip()
                    each_litigant_map['working_company'] = working_company

                # 年龄
                age_list = ['年龄']
                if re.search(r'^(' + '|'.join(age_list) + r')[：:]?(.*)$', each_info):
                    age = re.search(r'^(' + '|'.join(age_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['age'] = age

                # 性别
                gender_list = ['性别']
                if re.search(r'^(' + '|'.join(gender_list) + r')[：:]?(.*)$', each_info):
                    gender = re.search(r'^(' + '|'.join(gender_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['gender'] = gender

                # 负责人
                principal_list = ['主要负责人姓名', '主要负责人', '负责人姓名', '负责人', '机构负责人']
                if re.search(r'^(' + '|'.join(principal_list) + r')[：:]?(.*)$', each_info):
                    principal = re.search(r'^(' + '|'.join(principal_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['principal'] = principal

                # 法定代表人
                main_person_list = ['法定代表人或者主要负责人姓名', '法定代表人或主要负责人姓名', '法定代表人姓名', '法定代表人']
                if re.search(r'^(' + '|'.join(main_person_list) + r')[：:]?(.*)$', each_info):
                    main_person = re.search(r'^(' + '|'.join(main_person_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['legal_representative'] = main_person

                # 身份证号
                person_id_list = ['身份证号码', '身份证号', '身份证件种类及号码']
                if re.search(r'^(' + '|'.join(person_id_list) + r')[：:]?(.*)$', each_info):
                    person_id = re.search(r'^(' + '|'.join(person_id_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['identity_number'] = person_id

                # 港澳台证件号码
                hk_am_id_list = ['香港永久性居民身份证号', '港澳证件号码', '台湾身份证号码', '台湾居民来往大陆通行证号码']
                if re.search(r'^(' + '|'.join(hk_am_id_list) + r')[：:]?(.*)$', each_info):
                    hk_am_id = re.search(r'^(' + '|'.join(hk_am_id_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['hk_ma_tw_number'] = hk_am_id

                # 护照号
                passport_id_list = ['护照号']
                if re.search(r'^(' + '|'.join(passport_id_list) + r')[：:]?(.*)$', each_info):
                    passport_id = re.search(r'^(' + '|'.join(passport_id_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['passport_number'] = passport_id

                # 职务
                position_list = ['职务']
                position_keywords_list = ['时任']
                if re.search(r'^(' + '|'.join(position_list) + r')[：:]?(.*)$', each_info):
                    position = re.search(r'^(' + '|'.join(position_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['position_role'] = position
                else:
                    if re.search(r'^.*?(' + '|'.join(position_keywords_list) + r').*?$', each_info):
                        each_litigant_map['position_role'] = each_info
                    else:
                        if re.search(r'.*' + '(' + '|'.join(role_list) + ')' + '$', each_info):
                            each_litigant_map['position_role'] = each_info

                # 机构代码
                agency_code_list = ['组织机构代码']
                if re.search(r'^(' + '|'.join(agency_code_list) + r')[：:]?(.*)$', each_info):
                    agency_code = re.search(r'^(' + '|'.join(agency_code_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['agency_code'] = agency_code
            litigant_info_list.append(each_litigant_map)
    else:
        litigant_list = re.split(r'[、，,]', litigant)
        for each_litigant in litigant_list:
            if each_litigant in ['、', '，', ',']:
                continue
            each_litigant_map = {}
            each_litigant_info_list = re.split('(' + '|'.join(info_par_list) + ')', each_litigant)
            for i, each_info in enumerate(each_litigant_info_list):
                if each_info in info_par_list:
                    continue
                # 名称
                if i == 0:
                    litigant_name = each_info
                    chinese_name_recognition_list = demo_chinese_name_recognition(litigant_name)
                    com_flag = 0
                    for kk in chinese_name_recognition_list:
                        if str(kk).endswith('/nt'):
                            com_flag = 1
                    if com_flag == 1 or '公司' in litigant_name:
                        each_litigant_map['company_name'] = re.sub('^名称[:：]?', '', litigant_name)
                    else:
                        each_litigant_map['person_name'] = re.sub('^姓名[:：]?', '', litigant_name)

                # 住所地址
                address_list = ['住所', '住址', '地址', '住所', '机构地址']
                if re.search(r'^(' + '|'.join(address_list) + ')[:：]?(.*)$', each_info):
                    address = re.search(r'^(' + '|'.join(address_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['home_address'] = address

                # 办公地址
                working_address_list = ['营业地址']
                if re.search(r'^(' + '|'.join(working_address_list) + r')[：:]?(.*)$', each_info):
                    working_address = re.search(r'^(' + '|'.join(working_address_list) + r')[：:]?(.*)$',
                                                each_info).group(2).strip()
                    each_litigant_map['office_address'] = working_address

                # 年龄
                age_list = ['年龄']
                if re.search(r'^(' + '|'.join(age_list) + r')[：:]?(.*)$', each_info):
                    age = re.search(r'^(' + '|'.join(age_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['age'] = age

                # 性别
                gender_list = ['性别']
                if re.search(r'^(' + '|'.join(gender_list) + r')[：:]?(.*)$', each_info):
                    gender = re.search(r'^(' + '|'.join(gender_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['gender'] = gender

                # 负责人
                principal_list = ['主要负责人姓名', '主要负责人', '负责人']
                if re.search(r'^(' + '|'.join(principal_list) + r')[：:]?(.*)$', each_info):
                    principal = re.search(r'^(' + '|'.join(principal_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['principal'] = principal

                # 法定代表人
                main_person_list = ['法定代表人或者主要负责人姓名', '法定代表人或主要负责人姓名', '法定代表人姓名', '法定代表人']
                if re.search(r'^(' + '|'.join(main_person_list) + r')[：:]?(.*)$', each_info):
                    main_person = re.search(r'^(' + '|'.join(main_person_list) + r')[：:]?(.*)$',
                                            each_info).group(2).strip()
                    each_litigant_map['legal_representative'] = main_person

                # 身份证号
                person_id_list = ['身份证号码', '身份证号', '身份证件种类及号码']
                if re.search(r'^(' + '|'.join(person_id_list) + r')[：:]?(.*)$', each_info):
                    person_id = re.search(r'^(' + '|'.join(person_id_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['identity_number'] = person_id

                # 港澳台证件号码
                hk_am_id_list = ['香港永久性居民身份证号', '港澳证件号码', '台湾身份证号码', '台湾居民来往大陆通行证号码']
                if re.search(r'^(' + '|'.join(hk_am_id_list) + r')[：:]?(.*)$', each_info):
                    hk_am_id = re.search(r'^(' + '|'.join(hk_am_id_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['hk_ma_tw_number'] = hk_am_id

                # 护照号
                passport_id_list = ['护照号']
                if re.search(r'^(' + '|'.join(passport_id_list) + r')[：:]?(.*)$', each_info):
                    passport_id = re.search(r'^(' + '|'.join(passport_id_list) + r')[：:]?(.*)$',
                                            each_info).group(2).strip()
                    each_litigant_map['passport_number'] = passport_id

                # 职务
                position_list = ['职务']
                position_keywords_list = ['时任']
                if re.search(r'^(' + '|'.join(position_list) + r')[：:]?(.*)$', each_info):
                    position = re.search(r'^(' + '|'.join(position_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['position_role'] = position
                else:
                    if re.search(r'^.*?(' + '|'.join(position_keywords_list) + ').*?$', each_info):
                        each_litigant_map['position_role'] = each_info
                    else:
                        if re.search(r'.*' + '(' + '|'.join(role_list) + ')' + '$', each_info):
                            each_litigant_map['position_role'] = each_info

                # 机构代码
                agency_code_list = ['组织机构代码']
                if re.search(r'^(' + '|'.join(agency_code_list) + r')[：:]?(.*)$', each_info):
                    agency_code = re.search(r'^(' + '|'.join(agency_code_list) + r')[：:]?(.*)$',
                                            each_info).group(
                        2).strip()
                    each_litigant_map['agency_code'] = agency_code
            litigant_info_list.append(each_litigant_map)

    return parse_person_legal(litigant_info_list)


# 证监机构当事人解析
def parse_csrc(litigant):
    litigant = litigant.replace(' ', '').replace('\xa0', '').strip()
    if litigant == '':
        return [], []
    litigant_info_list = []
    if str(litigant).startswith('组织机构代码'):
        litigant = litigant.split('\n')[1] + '\n' + litigant.split('\n')[0] + '\n' + '\n'.join(litigant.split('\n')[2:])
    logger.info(litigant)
    par_list = ['当事人姓名', '当事人名称', '受处罚机构名称', '受处罚单位名称', '受处罚人名称', '处罚个人姓名',
                r'受处罚人：\n姓名', '受处罚人员姓名', r'受处罚人\(个人\)', '兼业代理人名称', '受处罚人（机构）',
                r'受处罚人\n姓名', r'受处罚人\(公民\)：\n姓名', '受处罚单位名称', '受处罚人（公民）',
                '受处罚人姓名', r'受处罚人\(公民\)', '当事人', '受处罚人', '被处罚单位名称', '被处罚人姓名',
                '受处罚单位', '机构名称', '受处罚机构']
    info_par_list = ['\n', '；', '。', '，']
    if re.search(r'^[\s\S]*?' + r'(' + r'|'.join(par_list) + r')[\s\S]*?$', litigant):

        litigant_list = ['当事人' + kk for kk in re.split('(' + '|'.join(par_list) + ')', litigant) if
                         kk not in par_list and kk != '']

        for each_litigant in litigant_list:
            if '当事人：' not in each_litigant:
                continue
            each_litigant = re.sub('当事人：\n', '当事人：', each_litigant)
            each_litigant_map = {}
            each_litigant_info_list = re.split('(' + '|'.join(info_par_list) + ')', each_litigant)
            for each_info in each_litigant_info_list:
                if each_info in info_par_list:
                    continue
                # 名称
                if re.search(r'^(' + '|'.join(par_list) + r')[：:]?(.*)$', each_info):
                    litigant_name = re.search(r'^(' + '|'.join(par_list) + r')[：:]?(.*?)$', each_info).group(
                        2).strip()
                    chinese_name_recognition_list = demo_chinese_name_recognition(litigant_name)
                    com_flag = 0
                    for kk in chinese_name_recognition_list:
                        if str(kk).endswith('/nt'):
                            com_flag = 1
                    if com_flag == 1 or '公司' in litigant_name:
                        each_litigant_map['company_name'] = re.sub('^名称[:：]?', '', litigant_name)
                    else:
                        each_litigant_map['person_name'] = re.sub('^姓名[:：]?', '', litigant_name)

                # 住所地址
                address_list = ['住所', '住址', '地址', '住所', '机构地址']
                if re.search(r'^(' + '|'.join(address_list) + r')[：:]?(.*)$', each_info):
                    address = re.search(r'^(' + '|'.join(address_list) + r')[：:]?(.*)$', each_info).group(2).strip()
                    each_litigant_map['home_address'] = address

                # 办公地址
                working_address_list = ['营业地址']
                if re.search(r'^(' + '|'.join(working_address_list) + r')[：:]?(.*)$', each_info):
                    working_address = re.search(r'^(' + '|'.join(working_address_list) + r')[：:]?(.*)$',
                                                each_info).group(
                        2).strip()
                    each_litigant_map['office_address'] = working_address

                # 注册地址
                register_address_list = ['注册地址']
                if re.search(r'^(' + '|'.join(register_address_list) + r')[：:]?(.*)$', each_info):
                    register_address = re.search(r'^(' + '|'.join(register_address_list) + r')[：:]?(.*)$',
                                                 each_info).group(
                        2).strip()
                    each_litigant_map['register_address'] = register_address

                # 就职公司(单位)
                working_company_list = ['单位']
                if re.search(r'^(' + '|'.join(working_company_list) + r')[：:]?(.*)$', each_info):
                    working_company = re.search(r'^(' + '|'.join(working_company_list) + r')[：:]?(.*)$',
                                                each_info).group(2).strip()
                    each_litigant_map['working_company'] = working_company

                # 年龄
                age_list = ['年龄']
                if re.search(r'^(' + '|'.join(age_list) + r')[：:]?(.*)$', each_info):
                    age = re.search(r'^(' + '|'.join(age_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['age'] = age

                # 性别
                gender_list = ['性别']
                if re.search(r'^(' + '|'.join(gender_list) + r')[：:]?(.*)$', each_info):
                    gender = re.search(r'^(' + '|'.join(gender_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['gender'] = gender

                # 负责人
                principal_list = ['主要负责人姓名', '主要负责人', '负责人姓名', '负责人', '机构负责人']
                if re.search(r'^(' + '|'.join(principal_list) + r')[：:]?(.*)$', each_info):
                    principal = re.search(r'^(' + '|'.join(principal_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['principal'] = principal

                # 法定代表人
                main_person_list = ['法定代表人或者主要负责人姓名', '法定代表人或主要负责人姓名', '法定代表人姓名', '法定代表人']
                if re.search(r'^(' + '|'.join(main_person_list) + r')[：:]?(.*)$', each_info):
                    main_person = re.search(r'^(' + '|'.join(main_person_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['legal_representative'] = main_person

                # 身份证号
                person_id_list = ['身份证号码', '身份证号', '身份证件种类及号码']
                if re.search(r'^(' + '|'.join(person_id_list) + r')[：:]?(.*)$', each_info):
                    person_id = re.search(r'^(' + '|'.join(person_id_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['identity_number'] = person_id

                # 港澳台证件号码
                hk_am_id_list = ['香港永久性居民身份证号', '港澳证件号码', '台湾身份证号码', '台湾居民来往大陆通行证号码']
                if re.search(r'^(' + '|'.join(hk_am_id_list) + r')[：:]?(.*)$', each_info):
                    hk_am_id = re.search(r'^(' + '|'.join(hk_am_id_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['hk_ma_tw_number'] = hk_am_id

                # 护照号
                passport_id_list = ['护照号']
                if re.search(r'^(' + '|'.join(passport_id_list) + r')[：:]?(.*)$', each_info):
                    passport_id = re.search(r'^(' + '|'.join(passport_id_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['passport_number'] = passport_id

                # 职务
                position_list = ['职务']
                position_keywords_list = ['时任']
                if re.search(r'^(' + '|'.join(position_list) + r')[：:]?(.*)$', each_info):
                    position = re.search(r'^(' + '|'.join(position_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['position_role'] = position
                else:
                    if re.search(r'^.*?(' + '|'.join(position_keywords_list) + r').*?$', each_info):
                        each_litigant_map['position_role'] = each_info
                    else:
                        if re.search(r'.*' + '(' + '|'.join(role_list) + ')' + '$', each_info):
                            each_litigant_map['position_role'] = each_info

                # 机构代码
                agency_code_list = ['组织机构代码']
                if re.search(r'^(' + '|'.join(agency_code_list) + r')[：:]?(.*)$', each_info):
                    agency_code = re.search(r'^(' + '|'.join(agency_code_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['agency_code'] = agency_code
            litigant_info_list.append(each_litigant_map)
    else:
        litigant_list = re.split(r'[、，,]', litigant)
        for each_litigant in litigant_list:
            if each_litigant in ['、', '，', ',']:
                continue
            each_litigant_map = {}
            each_litigant_info_list = re.split('(' + '|'.join(info_par_list) + ')', each_litigant)
            for i, each_info in enumerate(each_litigant_info_list):
                if each_info in info_par_list:
                    continue
                # 名称
                if i == 0:
                    litigant_name = each_info
                    chinese_name_recognition_list = demo_chinese_name_recognition(litigant_name)
                    com_flag = 0
                    for kk in chinese_name_recognition_list:
                        if str(kk).endswith('/nt'):
                            com_flag = 1
                    if com_flag == 1 or '公司' in litigant_name:
                        each_litigant_map['company_name'] = re.sub('^名称[:：]?', '', litigant_name)
                    else:
                        each_litigant_map['person_name'] = re.sub('^姓名[:：]?', '', litigant_name)

                # 住所地址
                address_list = ['住所', '住址', '地址', '住所', '机构地址']
                if re.search(r'^(' + '|'.join(address_list) + ')[:：]?(.*)$', each_info):
                    address = re.search(r'^(' + '|'.join(address_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['home_address'] = address

                # 办公地址
                working_address_list = ['营业地址']
                if re.search(r'^(' + '|'.join(working_address_list) + r')[：:]?(.*)$', each_info):
                    working_address = re.search(r'^(' + '|'.join(working_address_list) + r')[：:]?(.*)$',
                                                each_info).group(2).strip()
                    each_litigant_map['office_address'] = working_address

                # 年龄
                age_list = ['年龄']
                if re.search(r'^(' + '|'.join(age_list) + r')[：:]?(.*)$', each_info):
                    age = re.search(r'^(' + '|'.join(age_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['age'] = age

                # 性别
                gender_list = ['性别']
                if re.search(r'^(' + '|'.join(gender_list) + r')[：:]?(.*)$', each_info):
                    gender = re.search(r'^(' + '|'.join(gender_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['gender'] = gender

                # 负责人
                principal_list = ['主要负责人姓名', '主要负责人', '负责人']
                if re.search(r'^(' + '|'.join(principal_list) + r')[：:]?(.*)$', each_info):
                    principal = re.search(r'^(' + '|'.join(principal_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['principal'] = principal

                # 法定代表人
                main_person_list = ['法定代表人或者主要负责人姓名', '法定代表人或主要负责人姓名', '法定代表人姓名', '法定代表人']
                if re.search(r'^(' + '|'.join(main_person_list) + r')[：:]?(.*)$', each_info):
                    main_person = re.search(r'^(' + '|'.join(main_person_list) + r')[：:]?(.*)$',
                                            each_info).group(2).strip()
                    each_litigant_map['legal_representative'] = main_person

                # 身份证号
                person_id_list = ['身份证号码', '身份证号', '身份证件种类及号码']
                if re.search(r'^(' + '|'.join(person_id_list) + r')[：:]?(.*)$', each_info):
                    person_id = re.search(r'^(' + '|'.join(person_id_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['identity_number'] = person_id

                # 港澳台证件号码
                hk_am_id_list = ['香港永久性居民身份证号', '港澳证件号码', '台湾身份证号码', '台湾居民来往大陆通行证号码']
                if re.search(r'^(' + '|'.join(hk_am_id_list) + r')[：:]?(.*)$', each_info):
                    hk_am_id = re.search(r'^(' + '|'.join(hk_am_id_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['hk_ma_tw_number'] = hk_am_id

                # 护照号
                passport_id_list = ['护照号']
                if re.search(r'^(' + '|'.join(passport_id_list) + r')[：:]?(.*)$', each_info):
                    passport_id = re.search(r'^(' + '|'.join(passport_id_list) + r')[：:]?(.*)$',
                                            each_info).group(2).strip()
                    each_litigant_map['passport_number'] = passport_id

                # 职务
                position_list = ['职务']
                position_keywords_list = ['时任']
                if re.search(r'^(' + '|'.join(position_list) + r')[：:]?(.*)$', each_info):
                    position = re.search(r'^(' + '|'.join(position_list) + r')[：:]?(.*)$', each_info).group(
                        2).strip()
                    each_litigant_map['position_role'] = position
                else:
                    if re.search(r'^.*?(' + '|'.join(position_keywords_list) + ').*?$', each_info):
                        each_litigant_map['position_role'] = each_info
                    else:
                        if re.search(r'.*' + '(' + '|'.join(role_list) + ')' + '$', each_info):
                            each_litigant_map['position_role'] = each_info

                # 机构代码
                agency_code_list = ['组织机构代码']
                if re.search(r'^(' + '|'.join(agency_code_list) + r')[：:]?(.*)$', each_info):
                    agency_code = re.search(r'^(' + '|'.join(agency_code_list) + r')[：:]?(.*)$',
                                            each_info).group(
                        2).strip()
                    each_litigant_map['agency_code'] = agency_code
            litigant_info_list.append(each_litigant_map)

    return parse_person_legal(litigant_info_list)


def parse_litigant():
    for each_announcement in db.announcement.find(
            {
                # '_id': ObjectId('5ba4c423c66384695e9a826a'),
                'announcementOrg': {'$regex': '.*保监.*'},
                'litigant_parsed': {'$ne': True}
            },
            no_cursor_timeout=True):

        if db.litigant_parsed_result.find({'origin_announcement_id': each_announcement['_id']}).count() > 0:
            logger.info(each_announcement['_id'])
            if db.announcement.find({'_id': each_announcement['_id'], 'litigant_parsed': True}).count() > 0:
                logger.info('Announcement litigant parsed!!!\n')
            else:
                db.announcement.update_one({'_id': each_announcement['_id']}, {'$set': {'litigant_parsed': True}})
                logger.info('Update announcement litigant parsed status!!!\n')
            continue

        if db.parsed_data.find({'_id': each_announcement['oss_file_id']}).count() > 0:
            origin_url = db.parsed_data.find_one({'_id': each_announcement['oss_file_id']})['origin_url']
            logger.info(each_announcement['_id'])
            logger.info(origin_url)
        else:
            origin_url = ''
            logger.info(each_announcement['_id'])
            logger.info(each_announcement['announcementOrg'])

        if origin_url != '' and 'circ' in origin_url:
            person_info_list, legal_person_info_list = parse_circ(each_announcement['litigant'])
            logger.info('所有自然人列表')
            logger.info('\n'.join([str(kk) for kk in person_info_list]))
            logger.info('所有法人列表')
            logger.info('\n'.join([str(kk) for kk in legal_person_info_list]))
            logger.info('\n')

            # if db.litigant_parsed_result.find({'origin_announcement_id': each_announcement['_id']}).count() == 0:
            #     db.litigant_parsed_result.insert_one({
            #         'origin_announcement_id': each_announcement['_id'],
            #         'origin_litigant': each_announcement['litigant'],
            #         'person_info_list': person_info_list,
            #         'legal_person_info_list': legal_person_info_list,
            #         'status': 'not checked'
            #     })
            # else:
            #     db.litigant_parsed_result.update_one(
            #         {'origin_announcement_id': each_announcement['_id']},
            #         {
            #             '$set': {
            #                 'origin_litigant': each_announcement['litigant'],
            #                 'person_info_list': person_info_list,
            #                 'legal_person_info_list': legal_person_info_list,
            #                 'status': 'not checked'
            #             }
            #         })
            # db.announcement.update_one({'_id': each_announcement['_id']}, {'$set': {'litigant_parsed': True}})
            # logger.info('Update Announcement litigant_parsed status Success!')


def get_abbreviation():
    abb_full_list = []
    for each_litigant_parsed_result in db.litigant_parsed_result.find(no_cursor_timeout=True):
        for each_litigant in each_litigant_parsed_result['parsed_result']:
            if '公司名称' in each_litigant.keys():
                logger.info('\n')
                logger.info(each_litigant_parsed_result['origin_announcement_id'])
                if db.parsed_data.find(
                        {'_id':
                             db.announcement.find_one({'_id': each_litigant_parsed_result['origin_announcement_id']})[
                                 'oss_file_id']}).count() > 0:
                    url = db.parsed_data.find_one(
                        {'_id':
                             db.announcement.find_one({'_id': each_litigant_parsed_result['origin_announcement_id']})[
                                 'oss_file_id']})['oss_file_origin_url']
                else:
                    url = ''
                origin_announcement_id = each_litigant_parsed_result['origin_announcement_id']
                logger.info(each_litigant['公司名称'])
                logger.info(url)
                if 'A股证券简称' in each_litigant.keys():
                    each_abb_full_map = {
                        'fullName': each_litigant['公司名称'],
                        'abbreviation': each_litigant['A股证券简称'],
                        'url': url,
                        'origin_announcement_id': origin_announcement_id
                    }
                    logger.info(each_abb_full_map)
                    if each_abb_full_map not in abb_full_list:
                        db.litigant_abb_full_result.insert_one(each_abb_full_map)
                        abb_full_list.append(each_abb_full_map)
                elif 'B股证券简称' in each_litigant.keys():
                    each_abb_full_map = {
                        'fullName': each_litigant['公司名称'],
                        'abbreviation': each_litigant['B股证券简称'],
                        'url': url,
                        'origin_announcement_id': origin_announcement_id
                    }
                    logger.info(each_abb_full_map)
                    if each_abb_full_map not in abb_full_list:
                        db.litigant_abb_full_result.insert_one(each_abb_full_map)
                        abb_full_list.append(each_abb_full_map)
                elif '新三板证券简称' in each_litigant.keys():
                    each_abb_full_map = {
                        'fullName': each_litigant['公司名称'],
                        'abbreviation': each_litigant['新三板证券简称'],
                        'url': url,
                        'origin_announcement_id': origin_announcement_id
                    }
                    logger.info(each_abb_full_map)
                    if each_abb_full_map not in abb_full_list:
                        db.litigant_abb_full_result.insert_one(each_abb_full_map)
                        abb_full_list.append(each_abb_full_map)
                elif re.search(r'^(.*)[（(].*?(以下简称|以下称|简称)[:：]?(.*?)[)），]', each_litigant['公司名称']):
                    full_name = re.search(r'^(.*)[（(].*?(以下简称|以下称|简称)[:：]?(.*?)[)），]',
                                          each_litigant['公司名称']).group(1).strip()
                    abbreviation = re.search(r'^(.*)[（(].*?(以下简称|以下称|简称)[:：]?(.*?)[)），]',
                                             each_litigant['公司名称']).group(5).strip()
                    each_abb_full_map = {
                        'fullName': re.sub('[“”]', '', full_name),
                        'abbreviation': re.sub('(“|”|或者公司|或公司|或上市公司|或你公司|、公司)', '', abbreviation),
                        'url': url,
                        'origin_announcement_id': origin_announcement_id
                    }
                    logger.info(each_abb_full_map)
                    if each_abb_full_map not in abb_full_list:
                        db.litigant_abb_full_result.insert_one(each_abb_full_map)
                        abb_full_list.append(each_abb_full_map)
                else:
                    logger.info('warning')


parse_litigant()
# get_abbreviation()
