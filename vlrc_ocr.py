from PIL import Image
import pytesseract
from pdf2image import convert_from_path
import fitz


file = """C:\\Users\\KarthickK\\Box\\VLRC Report\\2021Q3\\00ab3cbbf683789c.pdf"""
doc = fitz.open(file)
print(doc.page_count)
for i in range(2):
    page = doc.loadPage(i-1)  # number of page
    pix = page.get_pixmap()
    output = "c:\\garbage\\" + str(i) + ".png"
    pix.save(output)

pytesseract.pytesseract.tesseract_cmd = "C:\\Program Files\\Tesseract-OCR\\tesseract.exe"
text = str(((pytesseract.image_to_string(Image.open("c:\\garbage\\1.png")))))
print(text)



