import pdfkit

locs = "https://www.ibm.com/Security/digital-assets/data-breach-cost-calculator"

config = pdfkit.configuration(wkhtmltopdf="C:\\Program Files\\wkhtmltopdf\\bin\\wkhtmltopdf.exe")
pdfkit.from_url(locs,"c:\\garbage\\optf1.pdf",configuration=config)