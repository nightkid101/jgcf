import xlrd
from pymongo import MongoClient
from init import config_init, logger_init
from bson import ObjectId

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

com_map = {
    '姓名': 'person_name',
    '性别': 'gender',
    '年龄': 'age',
    '出生日期': 'birthday',
    '国籍': 'nationality',
    '民族': 'nation',
    '住所地址': 'home_address',
    '身份证号': 'identity_number',
    '就职经历': 'employment_list',
    '执业类别': 'practice_category',
    '注册号': 'registration_number',
    '资格证号': 'certificate_number',
    '执业证号': 'license_number',
    '登记编号': 'enrollment_number',
    '一码通代码': 'person_code',
    '取得证书时间': 'certificate_time',
    '人物关系': 'relationship',
    '港澳台证件号码': 'hk_ma_tw_number',
    '护照号': 'passport_number',
    '办公地址': 'office_address',
    'A股证券代码': 'a_stock_code',
    'A股证券简称': 'a_stock_name',
    'B股证券代码': 'b_stock_code',
    'B股证券简称': 'b_stock_name',
    '新三板证券代码': 'neeq_stock_code',
    '新三板证券简称': 'neeq_stock_name',
    '公司名称': 'company_name',
    '涉及公司': 'involved_company_list',
    '注册地址': 'register_address',
    '法定代表人': 'legal_representative',
    '负责人': 'principal',
    '工商注册号': 'business_registration_number',
    '机构代码': 'agency_code',
    '登记时间': 'registration_time',
    '涉及对象': 'involved_object',
    '法人代表证件号': 'legal_representative_number_id',
    '成立日期': 'establishment_date'
}


def litigant_to_db(file_name, sheet_index=0):
    workbook = xlrd.open_workbook(file_name)
    book_sheet = workbook.sheet_by_index(sheet_index)

    final_list = []
    for row in range(book_sheet.nrows):
        if row == 0:
            continue
        row_value = book_sheet.row_values(row)
        if row == 1:
            new_litigant_map = {
                'id': row_value[0],
                'litigant_checked_result': [(row_value[3], row_value[4])]
            }
        else:
            if row_value[0].strip() != '':
                final_list.append(new_litigant_map)
                new_litigant_map = {
                    'id': row_value[0],
                    'litigant_checked_result': [(row_value[3], row_value[4])]
                }
            else:
                new_litigant_map['litigant_checked_result'].append((row_value[3], row_value[4]))

    for each_final_litigant_result in final_list:
        person_info_list = []
        legal_person_info_list = []
        each_litigant = {}
        for index, kk in enumerate(each_final_litigant_result['litigant_checked_result']):
            if kk != ('', ''):
                if kk[0] == '任职期间':
                    if 'employment_list' not in each_litigant.keys():
                        each_litigant['employment_list'] = [{
                            'employment_period': kk[1],
                            'working_company': each_final_litigant_result['litigant_checked_result'][index + 1][1],
                            'position_role': each_final_litigant_result['litigant_checked_result'][index + 2][1]
                        }]
                    else:
                        each_litigant['employment_list'].append({
                            'employment_period': kk[1],
                            'working_company': each_final_litigant_result['litigant_checked_result'][index + 1][1],
                            'position_role': each_final_litigant_result['litigant_checked_result'][index + 2][1]
                        })
                else:
                    if kk[0] == '涉及公司':
                        if 'involved_company_list' not in each_litigant.keys():
                            each_litigant['involved_company_list'] = [{
                                'involved_company_name': kk[1],
                                'position_role': each_final_litigant_result['litigant_checked_result'][index + 1][1]
                            }]
                        else:
                            each_litigant['involved_company_list'].append({
                                'involved_company_name': kk[1],
                                'position_role': each_final_litigant_result['litigant_checked_result'][index + 1][1]
                            })
                    else:
                        if kk[0] in ['就职公司', '职务/角色']:
                            continue
                        else:
                            if kk[0] in com_map.keys():
                                each_litigant[com_map[kk[0]]] = kk[1]
            else:
                if each_litigant != {}:
                    if 'company_name' in each_litigant.keys():
                        legal_person_info_list.append(each_litigant)
                    else:
                        person_info_list.append(each_litigant)
                    each_litigant = {}
                else:
                    continue
        print(each_final_litigant_result['id'])
        print(person_info_list)
        print(legal_person_info_list)
        db.litigant_parsed_result.update_one({
            '_id': ObjectId(each_final_litigant_result['id'])
        },
            {'$set': {'person_info_list': person_info_list,
                      'legal_person_info_list': legal_person_info_list,
                      'status': 'checked'}})
        print('数据更新完成')
        print('\n')


if __name__ == "__main__":
    litigant_to_db('./xlsx_file/litigant/cbrc_parsed_litigant.xlsx')
    # litigant_to_db('./xlsx_file/litigant/circ_parsed_litigant.xlsx')
    # litigant_to_db('./xlsx_file/litigant/csrc_parsed_litigant.xlsx')
    # litigant_to_db('./xlsx_file/litigant/neeq_parsed_litigant.xlsx')
    # litigant_to_db('./xlsx_file/litigant/sse_parsed_litigant.xlsx')
    # litigant_to_db('./xlsx_file/litigant/szse_parsed_litigant.xlsx')
    # litigant_to_db('./xlsx_file/litigant/pbc_sac_amac_safe_nafmii_parsed_litigant.xlsx', 1)
    # litigant_to_db('./xlsx_file/litigant/pbc_sac_amac_safe_nafmii_parsed_litigant.xlsx', 2)
    # litigant_to_db('./xlsx_file/litigant/pbc_sac_amac_safe_nafmii_parsed_litigant.xlsx', 3)
    # litigant_to_db('./xlsx_file/litigant/pbc_sac_amac_safe_nafmii_parsed_litigant.xlsx', 4)
