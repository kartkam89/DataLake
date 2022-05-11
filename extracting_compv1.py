import pandas as pd
import downloading_file as dowf
from datetime import datetime as dt
import pdfplumber as pp

class extracting_comp:
    def __init__(self,op_loc,ip_file):

        self.op_loc = op_loc
        self.ip_file = ip_file
        self.ip_df = pd.read_excel(self.ip_file)
        self.df = pd.DataFrame()


    def reading_text(self,file):
        with pp.open(file) as pdf:
            fp = pdf.pages[0].extract_text()
            lp = pdf.pages[-1].extract_text()
            if "commissioned" in fp.lower() or "commissioned" in lp.lower() \
                    or "sponsored" in fp.lower() or "sponsored" in lp.lower() \
                    or "in association with" in fp.lower() or "in association with" in lp.lower() \
                    or "in partnership with" in fp.lower() or "in partnership with" in lp.lower():
                comm = "Y"
            else:
                comm = "N"
        return [fp,lp,comm]

    def downloading_pdf(self):
        print("There are "+ str(len(self.ip_df)) + " records and going through one by one")
        for i in range(len(self.ip_df)):
        #for i in range(25):
            try:
                if ".pdf" in self.ip_df.iloc[i]['ASSET_URL'].lower() or "download" in self.ip_df.iloc[i]['ASSET_URL'].lower():
                    print("working on ", i)
                    dowf.download_file(self.ip_df.iloc[i]['ASSET_URL'],self.op_loc+self.ip_df.iloc[i]['ASSET_CODE'])
                    self.df = self.df.append([[self.ip_df.iloc[i]['ASSET_CODE'],
                                               '=Hyperlink("' + self.op_loc+self.ip_df.iloc[i]['ASSET_CODE']+'.pdf")',
                                               self.reading_text(self.op_loc+self.ip_df.iloc[i]['ASSET_CODE']+'.pdf')[0],
                                               self.reading_text(self.op_loc + self.ip_df.iloc[i]['ASSET_CODE'] + '.pdf')[1],
                                               self.reading_text(self.op_loc + self.ip_df.iloc[i]['ASSET_CODE'] + '.pdf')[2]
                                               ]])
                else:
                    print("working on ",i,"Non Pdf Page, skipping it","Non Pdf Page, skipping it","Non Pdf Page, skipping it","Non Pdf Page, skipping it")
                    self.df = self.df.append([[self.ip_df.iloc[i]['ASSET_CODE'], "non pdf page", "non pdf page", "non pdf page", "non pdf page"]])
            except:
                print(self.ip_df.iloc[i]['ASSET_URL'])
                self.df = self.df.append([[self.ip_df.iloc[i]['ASSET_CODE'], "not working", "not working", "not working", "not working"]])
        return self

    def pen_it(self):
        self.df.columns = ['asset_name','asset location','first page text','last page text','commissioned?']
        self.df.to_excel("c:\\garbage\\AR\\output.xlsx",index=False)



if __name__ == "__main__":
    print("starting the work", dt.now())
    extracting_comp("c:\\garbage\\AR\\","c:\\garbage\\input_file.xlsx").downloading_pdf().pen_it()
    print("Ending the work", dt.now())




    # import pdfplumber as pp
    #
    # loc = "c:\garbage\AR\ov77738.pdf"
    #
    # with pp.open(loc) as pdf:
    #     fp = pdf.pages[0].extract_text()
    #     lp = pdf.pages[-1].extract_text()
    #     if "commissioned" in fp.lower() or "commissioned" in lp.lower():
    #         comm = "Y"
    #     else:
    #         comm = "N"
    #
    #     print("First pages",fp.extract_text())
    #     print("Last Pages",lp.extract_text())
    #


