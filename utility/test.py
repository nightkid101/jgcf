import re

def search():
    content_text = '责令北京飞行博达电子有限公司改正违法行为决定书（[2018]Y7号）'
    announcement_code_compiler = re.compile(r'((京环.*?号)|(\[\d+\][\s\S]*?号))')
    if announcement_code_compiler.search(content_text):
        announcement_code = announcement_code_compiler.search(content_text).group(1).strip()
    else:
        announcement_code = ''
    print(announcement_code)


if __name__=='__main__':
    search()
