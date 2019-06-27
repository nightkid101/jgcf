from init import logger_init, config_init
from pymongo import MongoClient
from elasticsearch import Elasticsearch

logger = logger_init('数据统计')
config = config_init()
if config['mongodb']['dev_mongo'] == '1':
    db = MongoClient(config['mongodb']['ali_mongodb_url'], username=config['mongodb']['ali_mongodb_username'],
                     password=config['mongodb']['ali_mongodb_password'],
                     port=int(config['mongodb']['ali_mongodb_port']))[config['mongodb']['ali_mongodb_name']]

    dev_db = MongoClient(config['mongodb']['ali_mongodb_url'], username=config['mongodb']['ali_mongodb_username'],
                         password=config['mongodb']['ali_mongodb_password'],
                         port=int(config['mongodb']['ali_mongodb_port']))[
        config['mongodb']['dev_mongodb_db_name']]
else:
    db = MongoClient(
        host=config['mongodb']['mongodb_host'],
        port=int(config['mongodb']['mongodb_port']),
        username=None if config['mongodb']['mongodb_username'] == '' else config['mongodb']['mongodb_username'],
        password=None if config['mongodb']['mongodb_password'] == '' else config['mongodb']['mongodb_password'])[
        config['mongodb']['mongodb_db_name']]

    dev_db = MongoClient(
        host=config['mongodb']['dev_mongodb_host'],
        port=int(config['mongodb']['dev_mongodb_port']),
        username=None if config['mongodb']['dev_mongodb_username'] == '' else config['mongodb']['dev_mongodb_username'],
        password=None if config['mongodb']['dev_mongodb_password'] == '' else config['mongodb'][
            'dev_mongodb_password'])[
        config['mongodb']['dev_mongodb_db_name']]

city_list = [
    '北京', '天津', '河北', '山西',
    '内蒙古', '辽宁', '吉林', '黑龙江',
    '上海', '江苏', '浙江',
    '安徽',
    '福建', '江西', '山东', '河南',
    '湖北',
    '湖南',
    '广东',
    '广西',
    '海南',
    '重庆', '四川', '贵州',
    '云南',
    '西藏',
    '甘肃',
    '陕西',
    '青海',
    '宁夏', '新疆', '深圳',
    '大连', '宁波', '厦门', '青岛'
]

province_city_map = {
    '北京': [],
    '天津': ['北仓', '滨海'],
    '河北': ['石家庄', '沧州', '保定', '承德', '衡水', '秦皇岛', '唐山', '邢台', '张家口', '邯郸', '廊坊'],
    '山西': ['太原', '大同', '晋城', '晋中', '临汾', '吕梁', '朔州', '忻州', '阳泉', '运城', '长治'],
    '内蒙古': ['呼和浩特', '阿拉善', '巴彦淖尔', '包头', '赤峰', '鄂尔多斯', '呼伦贝尔', '通辽', '乌海', '乌兰察布', '兴安', '锡林郭勒'],
    '辽宁': ['沈阳', '鞍山', '本溪', '朝阳', '丹东', '抚顺', '阜新', '葫芦岛', '锦州', '辽阳', '盘锦', '铁岭', '营口'],
    '吉林': ['长春', '白城', '白山', '辽源', '四平', '松原', '延边', '通化'],
    '黑龙江': ['哈尔滨', '大庆', '大兴安岭', '鹤岗', '黑河', '鸡西', '佳木斯', '牡丹江', '七台河', '齐齐哈尔', '双鸭山', '绥化', '伊春'],
    '上海': [],
    '江苏': ['南京', '常州', '淮安', '连云港', '南通', '苏州', '泰州', '无锡', '宿迁', '盐城', '扬州', '镇江', '徐州'],
    '浙江': ['杭州', '湖州', '嘉兴', '金华', '丽水', '衢州', '绍兴', '台州', '温州', '舟山'],
    '安徽': ['合肥', '安庆', '蚌埠', '亳州', '巢湖', '池州', '滁州', '阜阳', '淮北', '淮南', '黄山', '六安', '马鞍山', '铜陵', '芜湖', '宿州', '宣城'],
    '福建': ['福州', '龙岩', '南平', '宁德', '莆田', '泉州', '三明', '漳州'],
    '江西': ['南昌', '抚州', '赣州', '吉安', '景德镇', '九江', '萍乡', '上饶', '新余', '宜春', '鹰潭'],
    '山东': ['济南', '滨州', '德州', '东营', '菏泽', '济宁', '莱芜', '聊城', '临沂', '日照', '泰安', '威海', '潍坊', '烟台', '枣庄', '中国滨州', '淄博'],
    '河南': ['郑州', '安阳', '鹤壁', '焦作', '开封', '洛阳', '漯河', '南阳', '平顶山', '濮阳', '三门峡', '商丘', '新乡', '信阳', '许昌', '周口', '驻马店'],
    '湖北': ['武汉', '鄂州', '恩施', '黄冈', '黄石', '荆门', '荆州', '十堰', '随州', '咸宁', '襄樊', '襄阳', '孝感', '宜昌'],
    '湖南': ['常德', '郴州', '衡阳', '怀化', '娄底', '邵阳', '湘潭', '湘潭', '湘西', '益阳', '永州', '岳阳', '张家界', '株洲', '长沙'],
    '广东': ['广州', '潮州', '东莞', '佛山', '河源', '惠州', '江门', '揭阳', '茂名', '梅州', '清远', '汕头', '汕尾', '韶关', '阳江', '云浮', '湛江', '肇庆',
           '中山', '增城', '珠海'],
    '广西': ['南宁', '百色', '北海', '崇左', '防城港', '贵港', '桂林', '河池', '贺州', '来宾', '柳州', '钦州', '梧州', '玉林'],
    '海南': ['海口', '三亚'],
    '重庆': ['巴南', '涪陵', '合川', '江津', '两江', '黔江', '万州', '永川'],
    '四川': ['成都', '阿坝', '巴中', '达州', '德阳', '甘孜', '广安', '广元', '乐山', '凉山', '泸州', '眉山', '绵阳', '南充', '内江', '攀枝花', '遂宁', '雅安',
           '宜宾', '资阳', '自贡'],
    '贵州': ['贵阳', '安顺', '毕节', '六盘水', '黔东南', '黔南', '黔西南', '铜仁', '遵义'],
    '云南': ['昆明', '保山', '楚雄', '大理', '德宏', '迪庆', '红河', '丽江', '临沧', '怒江', '普洱', '曲靖', '文山', '西双版纳', '玉溪', '昭通'],
    '甘肃': ['兰州', '白银', '定西', '甘南', '嘉峪关', '金昌', '酒泉', '临夏', '陇南', '平凉', '庆阳', '天水', '武威', '张掖'],
    '陕西': ['西安', '安康', '宝鸡', '汉中', '铜川', '渭南', '咸阳', '延安', '榆林', '商洛'],
    '青海': ['西宁', '果洛', '海北', '海东', '海西', '黄南', '玉树'],
    '宁夏': ['银川', '固原', '石嘴山', '吴忠', '中卫'],
    '新疆': ['乌鲁木齐', '阿克苏', '阿勒泰', '巴音郭楞', '博尔塔拉', '博州', '昌吉', '哈密', '和田', '喀什', '克拉玛依', '克孜勒苏', '石河子', '塔城', '吐鲁番',
           '伊犁'],
    '深圳': [],
    '大连': [],
    '宁波': [],
    '厦门': [],
    '青岛': [],
    '西藏': ['拉萨', '林芝']
}


# 证监会
def csrc_count():
    # 证监会行政处罚决定
    logger.info('证监会行政处罚决定 抓取数量：%d' % db.csrc_data.find({'origin': '证监会', 'type': '行政处罚决定'}).count())
    parsed_count = 0
    show_count = 0
    for each_aa in db.csrc_data.find({'origin': '证监会', 'type': '行政处罚决定', 'status': 'parsed'}):
        if db.parsed_data.find({'origin_url_id': each_aa['_id'], 'parsed': True}).count() > 0:
            parsed_count += 1
            parsed_data_id = db.parsed_data.find_one({'origin_url_id': each_aa['_id']})['_id']

            show_count += db.announcement.find({'oss_file_id': parsed_data_id, 'status': 'checked'}).count()
    logger.info('证监会行政处罚决定 解析数量：%d' % parsed_count)
    logger.info('证监会行政处罚决定 展示数量：%d' % show_count)

    # 证监会市场禁入决定
    logger.info('证监会市场禁入决定 抓取数量：%d' % db.csrc_data.find({'origin': '证监会', 'type': '市场禁入决定'}).count())
    parsed_count = 0
    show_count = 0
    for each_aa in db.csrc_data.find({'origin': '证监会', 'type': '市场禁入决定', 'status': 'parsed'}):
        if db.parsed_data.find({'origin_url_id': each_aa['_id'], 'parsed': True}).count() > 0:
            parsed_count += 1
            parsed_data_id = db.parsed_data.find_one({'origin_url_id': each_aa['_id']})['_id']

            show_count += db.announcement.find({'oss_file_id': parsed_data_id, 'status': 'checked'}).count()
    logger.info('证监会市场禁入决定 解析数量：%d' % parsed_count)
    logger.info('证监会市场禁入决定 展示数量：%d' % show_count)

    # 证监会责令整改通知
    logger.info('证监会责令整改通知 抓取数量：%d' % db.csrc_data.find({'origin': '证监会', 'type': '责令整改通知'}).count())
    parsed_count = 0
    show_count = 0
    for each_aa in db.csrc_data.find({'origin': '证监会', 'type': '责令整改通知', 'status': 'parsed'}):
        if db.parsed_data.find({'origin_url_id': each_aa['_id'], 'parsed': True}).count() > 0:
            parsed_count += 1
            parsed_data_id = db.parsed_data.find_one({'origin_url_id': each_aa['_id']})['_id']

            show_count += db.announcement.find({'oss_file_id': parsed_data_id, 'status': 'checked'}).count()
    logger.info('证监会责令整改通知 解析数量：%d' % parsed_count)
    logger.info('证监会责令整改通知 展示数量：%d' % show_count)

    # 证监会要闻
    logger.info('证监会要闻 抓取数量：%d' % db.csrc_data.find({'origin': '证监会', 'type': '要闻'}).count())
    parsed_count = 0
    show_count = 0
    for each_aa in db.csrc_data.find({'origin': '证监会', 'type': '要闻', 'status': 'parsed'}):
        if db.parsed_data.find({'origin_url_id': each_aa['_id'], 'parsed': True}).count() > 0:
            parsed_count += 1
            parsed_data_id = db.parsed_data.find_one({'origin_url_id': each_aa['_id']})['_id']

            show_count += db.announcement.find({'oss_file_id': parsed_data_id, 'status': 'checked'}).count()
    logger.info('证监会要闻 解析数量：%d' % parsed_count)
    logger.info('证监会要闻 展示数量：%d' % show_count)


# 地方证监局
def local_csrc_count(location):
    logger.info('%s行政处罚决定 抓取数量：%d' % (location, db.csrc_data.find({'origin': location, 'type': '行政处罚决定'}).count()))
    parsed_count = 0
    show_count = 0
    for each_aa in db.csrc_data.find({'origin': location, 'type': '行政处罚决定', 'status': 'parsed'}):
        if db.parsed_data.find({'origin_url_id': each_aa['_id'], 'parsed': True}).count() > 0:
            parsed_count += 1
            parsed_data_id = db.parsed_data.find_one({'origin_url_id': each_aa['_id']})['_id']
            csrc_mogo = db.announcement.find({'oss_file_id': parsed_data_id, 'status': 'checked'})
            if csrc_mogo.count() > 0:
                if csrc_mogo[0]['announcementOrg'] != location:
                    logger.info(csrc_mogo[0]['announcementOrg'])
                show_count += csrc_mogo.count()
            else:
                logger.warning('not showed %s' % str(parsed_data_id))
        else:
            logger.warning('not parsed %s' % str(each_aa['_id']))

    logger.info('%s行政处罚决定 解析数量：%d' % (location, parsed_count))
    logger.info('%s行政处罚决定 展示数量：%d' % (location, show_count))

    logger.info('%s监管措施 抓取数量：%d' % (location, db.csrc_data.find({'origin': location, 'type': '监管措施'}).count()))
    parsed_count = 0
    show_count = 0
    for each_aa in db.csrc_data.find({'origin': location, 'type': '监管措施', 'status': 'parsed'}):
        if db.parsed_data.find({'origin_url_id': each_aa['_id'], 'parsed': True}).count() > 0:
            parsed_count += 1
            parsed_data_id = db.parsed_data.find_one({'origin_url_id': each_aa['_id']})['_id']
            csrc_mogo = db.announcement.find({'oss_file_id': parsed_data_id, 'status': 'checked'})
            if csrc_mogo.count() > 0:
                if csrc_mogo[0]['announcementOrg'] != location:
                    logger.info(csrc_mogo[0]['announcementOrg'])
                show_count += csrc_mogo.count()
            else:
                logger.warning('not showed %s' % str(parsed_data_id))
        else:
            logger.warning('not parsed %s' % str(each_aa['_id']))

    logger.info('%s监管措施 解析数量：%d' % (location, parsed_count))
    logger.info('%s监管措施 展示数量：%d\n' % (location, show_count))


# 银监会
def cbrc_count():
    # 银监会行政处罚决定
    logger.info('银监会行政处罚决定 抓取数量：%d' % db.cbrc_data.find({'origin': '银监会', 'type': '行政处罚决定'}).count())
    parsed_count = 0
    show_count = 0
    for each_aa in db.cbrc_data.find({'origin': '银监会', 'type': '行政处罚决定', 'status': 'parsed'}):
        if db.parsed_data.find({'origin_url_id': each_aa['_id'], 'parsed': True}).count() > 0:
            parsed_count += 1
            parsed_data_id = db.parsed_data.find_one({'origin_url_id': each_aa['_id']})['_id']

            show_count += db.announcement.find({'oss_file_id': parsed_data_id, 'status': 'checked'}).count()
    logger.info('银监会行政处罚决定 解析数量：%d' % parsed_count)
    logger.info('银监会行政处罚决定 展示数量：%d' % show_count)


# 地方银监局
def local_cbrc_count(location):
    logger.info('%s行政处罚决定 抓取数量：%d' % (location, db.cbrc_data.find({'origin': location, 'type': '行政处罚决定'}).count()))
    parsed_count = 0
    show_count = 0
    for each_aa in db.cbrc_data.find({'origin': location, 'type': '行政处罚决定', 'status': 'parsed'}):
        parsed_data_mongo = db.parsed_data.find({'origin_url_id': each_aa['_id'], 'parsed': True})
        if parsed_data_mongo.count() > 0:
            parsed_count += 1
            parsed_data_id = parsed_data_mongo[0]['_id']
            cbrc_mogo = db.announcement.find({'oss_file_id': parsed_data_id, 'status': 'checked'})
            if cbrc_mogo.count() > 0:
                if cbrc_mogo[0]['announcementOrg'].replace('银监局', '').replace('银监分局', '') not in \
                        province_city_map[location.replace('银监局', '')] and \
                        cbrc_mogo[0]['announcementOrg'] != location:
                    logger.info(parsed_data_id)
                    logger.info(cbrc_mogo[0]['announcementOrg'])
                if cbrc_mogo.count() > 1:
                    logger.info(parsed_data_id)
                show_count += cbrc_mogo.count()
            else:
                logger.warning('not showed %s' % str(parsed_data_id))
        else:
            logger.warning('not parsed %s' % str(each_aa['_id']))

    logger.info('%s行政处罚决定 解析数量：%d' % (location, parsed_count))
    logger.info('%s行政处罚决定 展示数量：%d\n' % (location, show_count))


# 保监会
def circ_count():
    # 保监会行政处罚决定
    logger.info('保监会行政处罚决定 抓取数量：%d' % db.circ_data.find({'origin': '保监会', 'type': '行政处罚决定'}).count())
    parsed_count = 0
    show_count = 0
    for each_aa in db.circ_data.find({'origin': '保监会', 'type': '行政处罚决定', 'status': 'parsed'}):
        if db.parsed_data.find({'origin_url_id': each_aa['_id'], 'parsed': True}).count() > 0:
            parsed_count += 1
            parsed_data_id = db.parsed_data.find_one({'origin_url_id': each_aa['_id']})['_id']

            show_count += db.announcement.find({'oss_file_id': parsed_data_id, 'status': 'checked'}).count()
    logger.info('保监会行政处罚决定 解析数量：%d' % parsed_count)
    logger.info('保监会行政处罚决定 展示数量：%d' % show_count)

    # 保监会监管函
    logger.info('保监会监管函 抓取数量：%d' % db.circ_data.find({'origin': '保监会', 'type': '监管措施'}).count())
    parsed_count = 0
    show_count = 0
    for each_aa in db.circ_data.find({'origin': '保监会', 'type': '监管措施', 'status': 'parsed'}):
        if db.parsed_data.find({'origin_url_id': each_aa['_id'], 'parsed': True}).count() > 0:
            parsed_count += 1
            parsed_data_id = db.parsed_data.find_one({'origin_url_id': each_aa['_id']})['_id']

            show_count += db.announcement.find({'oss_file_id': parsed_data_id, 'status': 'checked'}).count()
    logger.info('保监会监管函 解析数量：%d' % parsed_count)
    logger.info('保监会监管函 展示数量：%d\n' % show_count)


# 地方保监局
def local_circ_count(location):
    logger.info('%s行政处罚决定 抓取数量：%d' % (location, db.circ_data.find({'origin': location, 'type': '行政处罚决定'}).count()))
    parsed_count = 0
    show_count = 0
    for each_aa in db.circ_data.find({'origin': location, 'type': '行政处罚决定', 'status': 'parsed'}):
        parsed_data_mongo = db.parsed_data.find({'origin_url_id': each_aa['_id'], 'parsed': True})
        if parsed_data_mongo.count() > 0:
            parsed_count += 1
            parsed_data_id = parsed_data_mongo[0]['_id']
            circ_mogo = db.announcement.find({'oss_file_id': parsed_data_id, 'status': 'checked'})
            if circ_mogo.count() > 0:
                if circ_mogo[0]['announcementOrg'].replace('保监局', '').replace('保监分局', '') not in \
                        province_city_map[location.replace('保监局', '')] and \
                        circ_mogo[0]['announcementOrg'] != location:
                    logger.info(parsed_data_id)
                    logger.info(circ_mogo[0]['announcementOrg'])
                if circ_mogo.count() > 1:
                    logger.info(parsed_data_id)
                show_count += circ_mogo.count()
            else:
                logger.warning('not showed %s' % str(parsed_data_id))
        else:
            logger.warning('not parsed %s' % str(each_aa['_id']))

    logger.info('%s行政处罚决定 解析数量：%d' % (location, parsed_count))
    logger.info('%s行政处罚决定 展示数量：%d\n' % (location, show_count))


if __name__ == "__main__":
    # 地方证监局
    # for each_city in city_list:
    #     local_csrc_count(each_city + '证监局')

    # 地方银监局
    # for each_city in city_list:
    #     local_cbrc_count(each_city + '银监局')

    # 地方保监局
    for each_city in city_list:
        local_circ_count(each_city + '保监局')
