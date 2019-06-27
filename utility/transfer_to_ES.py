from elasticsearch import Elasticsearch, exceptions
from elasticsearch.helpers import bulk
import re
from requests_html import HTML
from pymongo import MongoClient
from init import config_init, logger_init
from xlrd import open_workbook, xldate_as_tuple
import datetime
from bs4 import BeautifulSoup as bs
from bson import ObjectId

config = config_init()
logger = logger_init('迁移数据至 ES')

es = Elasticsearch([config['Aliyun_ES']['host']], timeout=30)

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

category_map = {
    'announcementTypesAll': ['行政处罚决定', '要闻', '责令整改通知', '市场禁入决定', '监管措施', '监管关注', '监管工作函',
                             '公开谴责', '公开谴责及公开认定', '纪律处分', '劝诫谈话', '其他'],
    'organizationsAll': ['财政部及地方财政', '人行及分支机构', '证监会及派出机构', '银保监会及派出机构', '银监会及派出机构（已撤销）',
                         '保监会及派出机构（已撤销）', '上交所', '深交所', '股转系统', '证券业协会', '基金业协会', '交易商协会',
                         '律师协会', '司法部门', '外管局'],
    'regionsAll': [
        '中央',
        '北京', '天津', '河北', '山西',
        '内蒙古', '辽宁', '吉林', '黑龙江',
        '上海', '江苏', '浙江', '安徽',
        '福建', '江西', '山东', '河南',
        '湖北', '湖南', '广东', '广西',
        '海南', '重庆', '四川', '贵州',
        '云南', '西藏', '甘肃', '陕西',
        '青海', '宁夏', '新疆', '深圳',
        '大连', '宁波', '厦门', '青岛'
    ]
}

redundance_list = ['当事人：', '当事人:',
                   '经查明，当事人存在如下违法事实：', '经查明,当事人存在如下违法事实：', '按照当事人的违法事实、性质、情节与社会危害程度，', '按照当事人的违法事实、性质、情节与社会危害程度,',
                   '按照有关规定，现对你公司提出以下监管要求：', '按照有关规定,现对你公司提出以下监管要求：', '存在不规范情况：', '存在以下情况：', '发现，你公司存在以下问题：',
                   '发现，你公司存在以下情况：', '发现,你公司存在以下问题：', '发现,你公司存在以下情况：', '发现公司存在以下问题：', '发现公司存在以下情况：', '当事人存在以下违法事实：',
                   '发现公司及其相关子公司存在以下问题：', '发现公司及其相关子公司存在以下情况：', '发现你部存在以下问题：', '发现你部存在以下情况：', '发现你公司存在以下情况：',
                   '发现你公司存在以下问题：', '发现你公司及相关子公司存在以下问题：', '发现你公司及相关子公司存在以下情况：', '发现以下情况：', '发现以下问题：', '该公司存在以下违法行为：',
                   '根据当事人的违法行为事实、性质、情节和社会危害程度，', '根据当事人的违法行为事实、性质、情节和社会危害程度,', '根据当事人的违法事实、性质、情节与社会危害程度，',
                   '根据当事人的违法事实、性质、情节与社会危害程度,', '根据当事人上述违法行为的事实、性质、情节与社会危害程度，', '根据当事人上述违法行为的事实、性质、情节与社会危害程度,',
                   '根据当事人违法行为的事实、性质、获利情况与社会危害程度等情节，', '根据当事人违法行为的事实、性质、获利情况与社会危害程度等情节,', '根据当事人违法行为的事实、性质、情节、社会危害程度，',
                   '根据当事人违法行为的事实、性质、情节、社会危害程度,', '根据当事人违法行为的事实、性质、情节、社会危害程度及违法行为发生后的态度，',
                   '根据当事人违法行为的事实、性质、情节、社会危害程度及违法行为发生后的态度,', '根据当事人违法行为的事实、性质、情节和社会危害程度，', '根据当事人违法行为的事实、性质、情节和社会危害程度,',
                   '根据当事人违法行为的事实、性质、情节和社会危害程度等，', '根据当事人违法行为的事实、性质、情节和社会危害程度等,', '根据当事人违法行为的事实、性质、情节以及社会危害程度，',
                   '根据当事人违法行为的事实、性质、情节以及社会危害程度,', '根据当事人违法行为的事实、性质、情节与社会危害程度，', '根据当事人违法行为的事实、性质、情节与社会危害程度,',
                   '根据当事人违法行为的事实、性质、情与社会危害程度，', '根据当事人违法行为的事实、性质、情与社会危害程度,', '根据当事人违法行为的事实、性质和情节，',
                   '根据当事人违法行为的事实、性质和情节,', '根据当事人违规行为的事实、性质、情节与社会危害程度，', '根据当事人违规行为的事实、性质、情节与社会危害程度,', '根据上述法律规定，',
                   '根据上述法律规定,', '根据上述法律规定，本会决定：', '根据上述法律规定,本会决定：', '根据上述法律规定，经研究，', '根据上述法律规定,经研究,', '根据上述法律规定，经研究决定，',
                   '根据上述法律规定,经研究决定,', '根据上述法律规定，经研究决定：', '根据上述法律规定,经研究决定：', '根据上述违法行为的事实、性质、情节与社会危害程度，',
                   '根据上述违法行为的事实、性质、情节与社会危害程度,', '根据上述违法违规行为及当事人的违法事实、性质、情节与社会危害程度，',
                   '根据上述违法违规行为及当事人的违法事实、性质、情节与社会危害程度,', '根据上述违法违规行为及当事人违法行为的事实、性质、情节与社会危害程度，',
                   '根据上述违法违规行为及当事人违法行为的事实、性质、情节与社会危害程度,', '根据上述违法违规行为及相关当事人的违法事实、性质、情节与社会危害程度，',
                   '根据上述违法违规行为及相关当事人的违法事实、性质、情节与社会危害程度,', '根据上述违法违规行为及有关人员的违法事实、性质、情节与社会危害程度，',
                   '根据上述违法违规行为及有关人员的违法事实、性质、情节与社会危害程度,', '根据违法行为的事实、性质、情节与社会危害程度，', '根据违法行为的事实、性质、情节与社会危害程度,',
                   '根据违法行为的事实、性质、情节与社会危害程度，我局决定：', '根据违法行为的事实、性质、情节与社会危害程度,我局决定：', '根据违法事实、性质、情节与社会危害程度，',
                   '根据违法事实、性质、情节与社会危害程度,', '基于上述理由，我会认为，', '基于上述理由,我会认为,', '基于上述情况，根据当事人违法行为的事实、性质、情节与社会危害程度，',
                   '基于上述情况,根据当事人违法行为的事实、性质、情节与社会危害程度,', '基于上述事实和理由，', '基于上述事实和理由,',
                   '基于上述事实和理由，根据当事人违法行为的事实、性质、情节与社会危害程度，', '基于上述事实和理由,根据当事人违法行为的事实、性质、情节与社会危害程度,', '基于上述事实和情节，',
                   '基于上述事实和情节,', '基于上述事实和情节,经本所纪律处分委员会审核通过,', '基于上述事实和情节，经本所纪律处分委员会审核通过，', '基于上述违规事实和情节，',
                   '基于上述违规事实和情节,', '基于以上事实，', '基于以上事实,', '基于以上违法事实，', '基于以上违法事实,', '检查发现，该公司存在以下违法行为：',
                   '检查发现,该公司存在以下违法行为：', '检查发现，公司存在以下问题：', '检查发现,公司存在以下问题：', '检查发现，你公司存在以下问题：', '检查发现,你公司存在以下问题：',
                   '检查发现你公司存在以下主要问题：', '检查中，发现以下问题：', '检查中,发现以下问题：', '鉴于行为的性质和情节', '鉴于你公司上述行为的性质及情节，',
                   '鉴于你公司上述行为的性质及情节,', '鉴于前述事实,', '鉴于前述事实，', '鉴于前述违规事实和情节,', '鉴于前述违规事实和情节，', '鉴于上述行为的性质和情节，',
                   '鉴于上述行为的性质和情节,', '鉴于上述行为和情节，', '鉴于上述行为和情节,', '鉴于上述情况，', '鉴于上述情况,', '鉴于上述事实和理由,', '鉴于上述事实和理由，',
                   '鉴于上述事实和情节，', '鉴于上述事实和情节,', '鉴于上述事实和情形,经本所纪律处分委员会审核通过,', '鉴于上述事实和情形，经本所纪律处分委员会审核通过，',
                   '鉴于上述违法、违规事实和情节,', '鉴于上述违法、违规事实和情节，', '鉴于上述违规行为,', '鉴于上述违规行为，', '鉴于上述违规行为的性质和情节', '鉴于上述违规行为的性质和情节，',
                   '鉴于上述违规行为的性质和情节,', '鉴于上述违规事实，', '鉴于上述违规事实,', '鉴于上述违规事实，经上海证券交易所（以下简称“本所”）纪律处分委员会审核通过，',
                   '鉴于上述违规事实,经上海证券交易所（以下简称“本所”）纪律处分委员会审核通过,', '鉴于上述违规事实和情节,', '鉴于上述违规事实和情节，',
                   '鉴于上述违规事实和情节,经上海证券交易所(以下简称“本所”)纪律处分委员会审核通过,', '鉴于上述违规事实和情节，经上海证券交易所(以下简称“本所”)纪律处分委员会审核通过，',
                   '鉴于上述违规事实和情节，经上海证券交易所纪律处分委员会审核通过，', '鉴于上述违规事实和情节,经上海证券交易所纪律处分委员会审核通过,', '鉴于上述违规事实和情形，',
                   '鉴于上述违规事实和情形,', '鉴于违规行为的性质和情节，', '鉴于违规行为的性质和情节,', '鉴于以上违规事实,', '鉴于以上违规事实，', '经查，', '经查,',
                   '经查，发现该公司存在如下问题：', '经查,发现该公司存在如下问题：', '经查，发现你公司存在以下问题：', '经查,发现你公司存在以下问题：', '经查，发现你司存在以下问题：',
                   '经查,发现你司存在以下问题：', '经查，你存在下列违法行为：', '经查,你存在下列违法行为：', '经查，你分公司存在以下违规行为：', '经查,你分公司存在以下违规行为：',
                   '经查，你公司存在下列违法行为：', '经查,你公司存在下列违法行为：', '经查，你公司存在以下行为：', '经查,你公司存在以下行为：', '经查，你公司存在以下违规行为：',
                   '经查,你公司存在以下违规行为：', '经查，你机构存在下列违法行为：', '经查,你机构存在下列违法行为：', '经查，你司存在以下问题：', '经查,你司存在以下问题：',
                   '经查，你支公司存在下列违法行为：', '经查,你支公司存在下列违法行为：', '经查，我局发现你公司存在以下违规行为：', '经查,我局发现你公司存在以下违规行为：',
                   '经查，我局发现你公司存在以下问题：', '经查,我局发现你公司存在以下问题：', '经查，我局发现你公司存在以下情况：', '经查,我局发现你公司存在以下情况：',
                   '经查，我局发现你们存在以下情况。', '经查,我局发现你们存在以下情况。', '经查，我局发现你司存在以下情况。', '经查,我局发现你司存在以下情况。', '经查，我局发现你们存在以下问题。',
                   '经查,我局发现你们存在以下问题。', '经查，我局发现你司存在以下问题。', '经查,我局发现你司存在以下问题。', '经查，我局发现你司以下问题。', '经查,我局发现你司以下问题。',
                   '经查：', '经查明。', '经查明，', '经查明,', '经查明，当事人存在如下违法行为：', '经查明,当事人存在如下违法行为：', '经查明，当事人存在下述违法行为：',
                   '经查明,当事人存在下述违法行为：', '经查明，当事人存在下述违法事实：', '经查明,当事人存在下述违法事实：', '经查明，当事人存在以下违法事实：', '经查明,当事人存在以下违法事实：',
                   '经查明,当事人存在以下违规行为：', '经查明，当事人存在以下违规行为：', '经查明，当事人具有以下违法事实：', '经查明,当事人具有以下违法事实：', '经查明，当事人有以下违法事实：',
                   '经查明,当事人有以下违法事实：', '经查明，你存在以下违法行为：', '经查明,你存在以下违法行为：', '经查明,你公司存在如下违规行为：', '经查明，你公司存在如下违规行为：',
                   '经查明，你公司存在以下违法行为：', '经查明,你公司存在以下违法行为：', '经查明，你公司存在以下违规事实：', '经查明,你公司存在以下违规事实：',
                   '经查明，你公司及相关责任主体存在以下违规事实：', '经查明,你公司及相关责任主体存在以下违规事实：', '经查明,你公司及相关责任主体有以下违规事实：',
                   '经查明，你公司及相关责任主体有以下违规事实：', '经查明，你公司有以下违规事实：', '经查明,你公司有以下违规事实：', '经查明，上述当事人存在以下违法事实：',
                   '经查明,上述当事人存在以下违法事实：', '经查明，上述当事人分别存在如下违法行为：', '经查明,上述当事人分别存在如下违法行为：', '经查明,相关违规事实如下：',
                   '经查明，相关违规事实如下：', '经查明：', '经查你公司存在以下违法违规行为：', '经复核，我会认为', '经复核,我会认为', '经复核，我会认为，', '经复核,我会认为,',
                   '经复核，我会认为：', '经复核,我会认为：', '经复核，我局认为', '经复核,我局认为', '经复核，我局认为，', '经复核,我局认为,', '经复核，我局认为：', '经复核,我局认为：',
                   '经核实，', '经核实,', '经研究，', '经研究,', '经研究决定，', '经研究决定,', '据此，', '据此,', '据此，决定给予你公司以下行政处罚：',
                   '据此,决定给予你公司以下行政处罚：', '决定作出如下处罚：', '你公司存在如下违法违规行为：', '你机构存在下列违法行为：', '受处罚人名称：', '我部发现,', '我部发现，',
                   '我部关注到,', '我部关注到，', '我部在监管中发现,', '我部在监管中发现，', '我会认为，', '我会认为,', '我局决定对你公司做出以下处罚：', '我局决定作出如下处罚：',
                   '我局决定作出如下行政处罚：', '我局作出如下处罚：', '现查明，', '现查明,', '现查明：', '综合当事人违法行为的事实、性质、情节以及危害后果，',
                   '综合当事人违法行为的事实、性质、情节以及危害后果,', '综合考量上述因素，', '综合考量上述因素,', '综合考虑本案证据，根据冯泽良违法行为的事实、性质、情节与社会危害程度，',
                   '综合考虑本案证据,根据冯泽良违法行为的事实、性质、情节与社会危害程度,', '综合考虑整个事件的性质、影响和情节等情况，', '综合考虑整个事件的性质、影响和情节等情况,',
                   '综合上述情况，根据当事人违法行为的事实、性质、情节与社会危害程度，', '综合上述情况,根据当事人违法行为的事实、性质、情节与社会危害程度,', '综合上述情况，审理认为，',
                   '综合上述情况,审理认为,', '综合整个事件的性质、影响和情节，', '综合整个事件的性质、影响和情节,', '综上，', '综上,',
                   '综上，根据当事人违法行为的事实、性质、情节与社会危害程度，', '综上,根据当事人违法行为的事实、性质、情节与社会危害程度,', '综上，我会决定：', '综上,我会决定：',
                   '综上，我会决定如下：', '综上,我会决定如下：', '综上，我会决定作出如下处罚：', '综上,我会决定作出如下处罚：', '综上，我会作出如下处罚：', '综上,我会作出如下处罚：',
                   '综上，我局决定作出如下处罚：', '综上,我局决定作出如下处罚：']


# 获取法律信息
def get_law():
    excel_data = open_workbook('./xlsx_file/laws/laws.xls')
    sheet = excel_data.sheets()[1]
    left_value = sheet.cell(0, 0).value
    laws_result_map = {}
    result_list = []
    for i in range(sheet.nrows):
        if sheet.cell(i, 0).value != '':
            laws_result_map[left_value] = result_list
            left_value = sheet.cell(i, 0).value
            result_list = []
        result_list.append(sheet.cell(i, 1).value)

    sheet = excel_data.sheets()[2]
    left_value = sheet.cell(0, 0).value
    laws_url_map = {}
    result_list = []
    for i in range(sheet.nrows):
        if sheet.cell(i, 2).value == '' and sheet.cell(i, 3).value == '':
            continue
        if sheet.cell(i, 3).ctype == 3 or sheet.cell(i, 3).ctype == 2:
            publish_date = xldate_as_tuple(sheet.cell_value(i, 3), excel_data.datemode)
            law_datetime = datetime.date(int(publish_date[0]), int(publish_date[1]), int(publish_date[2]))
        else:
            date_str_list = re.split(r'[-/]', sheet.cell(i, 3).value)
            law_datetime = datetime.date(int(date_str_list[0]), int(date_str_list[1]), int(date_str_list[2]))

        if sheet.cell(i, 0).value != '':
            result_list = sorted(result_list, key=lambda x: x['date'])
            laws_url_map[left_value] = result_list
            left_value = sheet.cell(i, 0).value
            result_list = []
        result_list.append({'url': sheet.cell(i, 2).value.replace('/#', ''), 'date': law_datetime})

    laws_final_map = {}
    for each_law in laws_result_map:
        for each_origin_law in laws_result_map[each_law]:
            laws_final_map[each_origin_law] = laws_url_map[each_law]
    return laws_final_map


# 获取地区和机构
def get_region_and_org(real_org, origin_url):
    if real_org in ['上交所', '深交所', '股转系统', '证券业协会', '基金业协会', '交易商协会']:
        announcement_region = '中央'
        org_cate = real_org
    else:
        if 'pbc.gov.cn' in origin_url:
            org_cate = '人行及分支机构'
            if real_org == '人民银行':
                announcement_region = '中央'
            else:
                announcement_region = real_org.replace('市中心支行', '').replace('中心支行', '').replace('中国人民银行', '') \
                    .replace('人民银行', ''). \
                    replace('营业管理部', '').replace('（北京）', '北京').replace('分行', '').replace('总部', '')
        elif 'csrc.gov.cn' in origin_url:
            org_cate = '证监会及派出机构'
            if real_org == '证监会':
                announcement_region = '中央'
            else:
                announcement_region = re.split('证监', real_org)[0]
        elif 'cbrc.gov.cn' in origin_url:
            if '银保监' in real_org or '中国银行保险监督管理委员会' in real_org:
                org_cate = '银保监会及派出机构'
            else:
                org_cate = '银监会及派出机构'
            real_org = real_org.replace('中国银保监会', '').replace('中国银行保险监督管理委员会', '')
            if real_org == '银监会' or real_org == '银保监会':
                announcement_region = '中央'
            else:
                announcement_region = re.split('银保监分局|银保监局|银监|监管局|监管分局', real_org)[0]
        elif 'circ.gov.cn' in origin_url:
            if '银保监' in real_org:
                org_cate = '银保监会及派出机构'
            else:
                org_cate = '保监会及派出机构'
            real_org = real_org.replace('中国银保监会', '')
            if real_org == '保监会' or real_org == '银保监会':
                announcement_region = '中央'
            else:
                announcement_region = re.split('银保监分局|银保监局|保监|监管局|监管分局', real_org)[0]
        elif '律师协会' in real_org:
            org_cate = '律师协会'
            announcement_region = real_org.replace('市律师协会', '').replace('省律师协会', '').replace('州律师协会', '') \
                .replace('律师协会', '')
        elif '外汇' in real_org:
            org_cate = '外管局'
            announcement_region = real_org.replace('国家外汇管理局', '').replace('国家外汇管理', '') \
                .replace('壮族自治区分局', '').replace('省分局', '').replace('市中心支局', '') \
                .replace('外汇管理部', '').replace('市分局', '').replace('分局', '').replace('支局', '')
        elif '司法' in real_org:
            org_cate = '司法部门'
            announcement_region = real_org.replace('黑龙江省', '').replace('市司法局', '').replace('省司法厅', '')
        elif '财政' in real_org:
            org_cate = '财政部及地方财政'
            announcement_region = real_org.replace('财政局', '').replace('财政厅', '')
            pass
        else:
            if origin_url == '':
                if '证监' in real_org:
                    org_cate = '证监会及派出机构'
                    if real_org == '证监会':
                        announcement_region = '中央'
                    else:
                        announcement_region = re.split('证监', real_org)[0]
                else:
                    logger.error('新地区！！！')
                    announcement_region = org_cate = ''
            elif real_org == '全国贸联会':
                announcement_region = '中央'
                org_cate = '贸联会'
            else:
                logger.error('新地区！！！')
                announcement_region = org_cate = ''
        announcement_region = announcement_region.strip()

    for each_pr in province_city_map:
        if announcement_region in province_city_map[each_pr]:
            announcement_region = each_pr
    if announcement_region not in category_map['regionsAll']:
        logger.info(org_cate + ' ' + announcement_region)
        logger.error('地区信息错误！！！')
        return '', ''

    logger.info(org_cate + ' ' + announcement_region)
    return org_cate, announcement_region


# 数据导入到ES里
def transfer(org_info):
    es_action_list = []
    for each_punishAnnouncement in db.announcement.find(
            {'status': 'checked',
             'es_status': {'$nin': ['inserted']},
             # '_id': ObjectId("5c7e0b01c663849a6fd9752f"),
             'announcementOrg': {'$regex': org_info}}, no_cursor_timeout=True):
        try:
            logger.info(str(each_punishAnnouncement['_id']))
            res = es.get(index=str(config['Aliyun_ES']['dev_data_index_name']).strip(),
                         doc_type=str(config['Aliyun_ES']['dev_data_doc_type']).strip(),
                         id=str(each_punishAnnouncement['_id']))
            if res['found']:
                logger.info('exists')
                db.announcement.update_one({'_id': ObjectId(each_punishAnnouncement['_id'])},
                                           {'$set': {'es_status': 'inserted'}})
                logger.info('Update existed announcement es_status success')
                continue
        except exceptions.NotFoundError:
            logger.info(str(each_punishAnnouncement['_id']))
        punishment_type = each_punishAnnouncement['type']
        if each_punishAnnouncement['oss_file_id'] != '':
            oss_file = db.parsed_data.find_one({'_id': each_punishAnnouncement['oss_file_id']})
            html_content = oss_file['oss_file_content']
            oss_file_type = oss_file['oss_file_type']
            oss_file_name = oss_file['oss_file_name']
            origin_url = oss_file['origin_url']
            real_org = each_punishAnnouncement['announcementOrg']
            org_cate, announcement_region = get_region_and_org(real_org, origin_url)
        else:
            oss_file = {}
            html_content = ''
            oss_file_type = ''
            oss_file_name = ''
            origin_url = ''
            real_org = each_punishAnnouncement['announcementOrg']
            org_cate, announcement_region = get_region_and_org(real_org, origin_url)

        if org_cate == '' and announcement_region == '':
            continue

        content = ''
        if oss_file_type == 'html' or oss_file_type == 'shtml':
            html = HTML(html=html_content)
            if 'content_id_name' in each_punishAnnouncement.keys():
                content = html.find('#' + each_punishAnnouncement['content_id_name'])[0].html
            elif 'content_class_name' in each_punishAnnouncement.keys():
                content = html.find('.' + each_punishAnnouncement['content_class_name'])[0].html
            elif 'content_id_name' in oss_file.keys():
                content = html.find('#' + oss_file['content_id_name'])[0].html
            elif 'content_class_name' in oss_file.keys():
                if each_punishAnnouncement['announcementOrg'] == '山东律师协会' and \
                        'http://www.sdlawyer.org.cn/003/002/201214631225.htm' in oss_file['origin_url']:
                    content = str(html)
                else:
                    content = html.find('.' + oss_file['content_class_name'])[0].html
            else:
                if len(html.find('.in_main')) > 0:
                    content = html.find('.content')[0].html
                else:
                    if len(html.find('.main')) > 0:
                        content = html.find('.headInfo')[0].html + \
                                  '<p align="center" class="title">' + \
                                  each_punishAnnouncement['announcementTitle'] + \
                                  '</p>' + \
                                  html.find('#ContentRegion')[0].html
                    else:
                        if len(html.find('.er_main')) > 0:
                            content = html.find('.er_main')[0].html
                            logger.info('er_main')
                        else:
                            if len(html.find('#zwgk_pre')) > 0:
                                content = html.find('#zwgk_pre')[0].html
                                logger.info('zwgk_pre')
                            else:
                                if len(html.find('.f12c')) > 0:
                                    content = html.find('.f12c')[0].html.replace('margin-left:-25.1500pt;', '').replace(
                                        '/chinese/home/img/mz2.jpg', '')
                                    logger.info('f12c')
                                else:
                                    if len(html.find('.xl_cen')) > 0:
                                        content = html.find('.xl_cen')[0].html
                                        logger.info('xl_cen')
                                    else:
                                        if len(html.find('.iRight')) > 0:
                                            content = html.find('.iRight')[0].html
                                            logger.info('iRight')
                                        else:
                                            if len(html.find('.TRS_Editor')) > 0:
                                                content = html.find('.TRS_Editor')[0].html
                                                logger.info('TRS_Editor')
                                            else:
                                                if len(html.find('#tab_content')) > 0:
                                                    content = '<table width="100%" cellspacing="1" cellpadding="3" ' \
                                                              'border="0" align="center" class="normal" ' \
                                                              'id="tab_content"><tbody>' + \
                                                              html.find('#tab_content')[0].find('tr')[0].html + \
                                                              html.find('#tab_content')[0].find('tr')[3].html + \
                                                              '</table>'
                                                    content = content.replace('#08318d', 'red')
                                                    logger.info('tab_content')
                                                else:
                                                    if len(html.find('.hei14jj')) > 0:
                                                        content = html.find('.hei14jj')[0].find('table')[0].html
                                                        logger.info('hei14jj')
                                                    else:
                                                        if len(html.find('.article-infor')) > 0:
                                                            content = html.find('.article-infor')[0].html
                                                            logger.info('article-infor')
                                                        else:
                                                            if len(html.find('.Section1')) > 0:
                                                                content = html.find('.Section1')[0].html
                                                                logger.info('Section1')
                                                            else:
                                                                logger.error('content not exists')
                                                                continue
        else:
            content = ''
        if content != '':
            soup = bs(content, 'lxml')
            for div in soup.find_all("a"):
                div.decompose()
            content = str(soup.html)

        publish_date_list = re.split('[年月日]', each_punishAnnouncement['announcementDate'].replace('\xa0', ''))
        publish_date_text = publish_date_list[0] + (
            '0' + publish_date_list[1] if len(publish_date_list[1]) == 1 else publish_date_list[1]) + (
                                '0' + publish_date_list[2] if len(publish_date_list[2]) == 1 else publish_date_list[2])
        punish_datetime = datetime.date(int(publish_date_list[0]), int(publish_date_list[1]), int(publish_date_list[2]))

        punishment_decision = each_punishAnnouncement['punishmentDecision'].strip()
        law_list = re.findall('(《.*?》(（.*?）)?)', punishment_decision)
        laws_final_map = get_law()
        for each_law in law_list:
            if each_law[0] in laws_final_map.keys():
                for each_date in laws_final_map[each_law[0]]:
                    if punish_datetime > each_date['date']:
                        punishment_decision = \
                            punishment_decision.replace(
                                each_law[0],
                                '<a target="_blank" href="' + '/app/lar/' + str(each_date['url'])
                                + '">' + each_law[0] + '</a>'
                            )

        # 去除开头冗余
        facts = each_punishAnnouncement['facts']
        litigant = each_punishAnnouncement['litigant'].replace(',', '，').replace('(', '（').replace(')', '）').replace(
            ';',
            '；')
        defense = each_punishAnnouncement['defenseOpinion']
        defense_response = each_punishAnnouncement['defenseResponse']

        for each_redundance in redundance_list:
            facts = re.sub('^' + each_redundance + '[，,。：:]?', '', facts)

            litigant = re.sub('^' + each_redundance + '[，,。：:]?', '', litigant)

            defense = re.sub('^' + each_redundance + '[，,。：:]?', '', defense)

            defense_response = re.sub('^' + each_redundance + '[，,。：:]?', '', defense_response)

            punishment_decision = re.sub('^' + each_redundance + '[，,。：:]?', '', punishment_decision)

        doc = {
            'title': each_punishAnnouncement['announcementTitle'],
            'document_code': each_punishAnnouncement['announcementCode'],
            'publish_date': each_punishAnnouncement['announcementDate'].replace('年0', '年').replace('月0', '月'),
            'publish_date_text': int(publish_date_text),
            'litigant_origin_text': litigant,
            'litigant': '<p>' + '</p><p>'.join(litigant.strip().split('\n')) + '</p>',
            'fact_origin_text': facts.strip(),
            'fact': '<p>' + '</p><p>'.join(facts.strip().split('\n')) + '</p>',
            'defense': '<p>' + '</p><p>'.join(defense.strip().split('\n')) + '</p>',
            'defense_response': '<p>' + '</p><p>'.join(
                defense_response.strip().split('\n')) + '</p>',
            'punishment_basis': '<p>' + '</p><p>'.join(
                each_punishAnnouncement['punishmentBasement'].strip().split('\n')) + '</p>',
            'punishment_decision': '<p>' + '</p><p>'.join(
                punishment_decision.strip().split('\n')) + '</p>',
            'punishment_org_cate': org_cate,
            'punishment_organization': each_punishAnnouncement['announcementOrg'],
            'punishment_region': announcement_region,
            'punishment_type': punishment_type,
            'content_text': '\n'.join(
                [each_punishAnnouncement['announcementCode'], litigant,
                 facts, defense, defense_response, each_punishAnnouncement['punishmentBasement'],
                 punishment_decision]),
            'html_content': content,
            'oss_file_type': oss_file_type,
            'oss_file_id': str(each_punishAnnouncement['oss_file_id']),
            'oss_file_name': oss_file_name
        }

        es_action_list.append({
            '_index': str(config['Aliyun_ES']['dev_data_index_name']).strip(),
            '_type': str(config['Aliyun_ES']['dev_data_doc_type']).strip(),
            '_id': str(each_punishAnnouncement['_id']),
            '_source': doc
        })
        logger.info('one document add to action list\n')
        if len(es_action_list) == 50:
            bulk(es, es_action_list, raise_on_error=False)
            logger.info('Inserted into ES 50 documents!!')
            for each_es_action in es_action_list:
                db.announcement.update_one({'_id': ObjectId(each_es_action['_id'])},
                                           {'$set': {'es_status': 'inserted'}})
            logger.info('Update mongodb es_status success')
            es_action_list = []

    if len(es_action_list) > 0:
        bulk(es, es_action_list, raise_on_error=False)
        logger.info('Inserted into ES %d documents!!' % len(es_action_list))
        for each_es_action in es_action_list:
            db.announcement.update_one({'_id': ObjectId(each_es_action['_id'])}, {'$set': {'es_status': 'inserted'}})
        logger.info('Update mongodb es_status success')


if __name__ == "__main__":
    transfer('.*.*')
