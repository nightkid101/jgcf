from pdf2html import pdf_ocr_to_text

result_text, ocr_flag = pdf_ocr_to_text('/home/wangl/jgcf/test/tmp.pdf')
print(result_text)