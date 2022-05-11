import urllib.request
import requests as req
import PyPDF2 as pdf

# pdf_path = "https://www.ibm.com/downloads/cas/G5DPO1J6"


def download_file(download_url, filename):
    response = urllib.request.urlopen(download_url)
    file = open(filename + ".pdf", 'wb')
    file.write(response.read())
    file.close()

# def read_text(read_url):
#     txt = req.get(read_url).text
#     print(txt)
#


# download_file(pdf_path, "c:\\garbage\\Test1")
# file1 = open("c:\\garbage\\Test.pdf","rb")
# reader = pdf.PdfFileReader(file1)
# text = reader.getPage(0).extractText()
# print(text)
#
# read_text("https://www.forrester.com/report/The+Forrester+Wave+RiskBased+Authentication+Q2+2020/-/E-RES157259#")