import re
import os
import oss2
from init import logger_init, config_init

logger = logger_init('ALI_OSS_IO')
config = config_init()
localDirName = 'test'


def init_ali_oss(key='ALI_OSS_Storage_Config'):
    # https://signin.aliyun.com/login.htm
    # https://help.aliyun.com/document_detail/32032.html
    access_key_id = config[key]['access_key_ID']
    access_key_secret = config[key]['access_key_Secret']
    bucket_name = config[key]['bucket_name']
    oss_endpoint = config[key]['oss_endpoint']
    auth = oss2.Auth(access_key_id, access_key_secret)
    bucket = oss2.Bucket(auth, oss_endpoint, bucket_name)
    return bucket


def oss_add_file(bucket, file_name, file_content, key='ALI_OSS_Storage_Config'):
    # 尽量少使用调取操作
    try:
        if int(config['mongodb']['dev_mongo']):
            file_name = os.path.join(config[key]['prefix'], file_name)
            if file_name.endswith('.shtml'):
                bucket.put_object(file_name, file_content, headers={'Content-Type': 'text/html'})
            else:
                bucket.put_object(file_name, file_content)
            logger.info('Save File to Ali OSS Success')
        else:
            save_local_file(file_name, file_content)
        return True
    except Exception as e:
        logger.warning('Cannot Save File ' + file_name + ' To Ali OSS')
        logger.warning(e)
        return False


def oss_get_file(bucket, file_name, key='ALI_OSS_Storage_Config'):
    try:
        file_name = os.path.join(config[key]['prefix'], file_name)
        file_content = bucket.get_object(file_name)
        return file_content
    except Exception as e:
        logger.warning('Cannot Get File ' + file_name + ' From Ali OSS')
        logger.warning(e)
        return None


def save_local_file(file_name, file_content):
    suffix = re.compile(r".*\.(.*)").findall(file_name)[0].upper()
    op = 'w' if suffix in ['TXT', 'HTML', 'SHTML'] else 'wb'
    if not os.path.isdir(localDirName):
        os.makedirs(localDirName)
    f_name = os.path.join(localDirName, file_name)
    if not os.path.isdir('/'.join(f_name.split('/')[:-1])):
        os.makedirs('/'.join(f_name.split('/')[:-1]))
    with open(f_name, op) as f:
        f.write(file_content)
    logger.info('Save File to localhost Success')
    return f_name
