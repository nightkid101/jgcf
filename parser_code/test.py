import re

def text():
    content_text = '津市环罚字[2019]1号'
    announcement_code_compiler = re.compile(r'(津市[\s\S]*?号)')
    announcement_code_list = announcement_code_compiler.search(content_text).group(1).split(' ')
    if len(announcement_code_list) >= 2:
        announcement_code = announcement_code_list[1]
    else:
        announcement_code = announcement_code_list[0]
    print(announcement_code)

if __name__=="__main__":
    text()