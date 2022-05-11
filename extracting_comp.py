import pandas as pd
import downloading_file as dowf
from datetime import datetime as dt


class extracting_comp:
    def __init__(self,op_loc,ip_file):

        self.op_loc = op_loc
        self.ip_file = ip_file
        self.ip_df = pd.read_excel(self.ip_file)
        self.df = pd.DataFrame()


    def downloading_pdf(self):
        print("There are "+ str(len(self.ip_df)) + " records and going through one by one")
        #for i in range(len(self.ip_df)):
        for i in range(10):
            try:
                if ".pdf" in self.ip_df.iloc[i]['ASSET_URL'].lower() or "download" in self.ip_df.iloc[i]['ASSET_URL'].lower():
                    print("working on ", i)
                    dowf.download_file(self.ip_df.iloc[i]['ASSET_URL'],self.op_loc+self.ip_df.iloc[i]['ASSET_CODE'])
                    self.df = self.df.append([[self.ip_df.iloc[i]['ASSET_CODE'],'=Hyperlink("' + self.op_loc+self.ip_df.iloc[i]['ASSET_CODE']+'.pdf")']])
                else:
                    print("working on ",i,"Non Pdf Page, skipping it")
                    self.df = self.df.append([[self.ip_df.iloc[i]['ASSET_CODE'], "non pdf page"]])
            except:
                print(self.ip_df.iloc[i]['ASSET_URL'])
                self.df = self.df.append([[self.ip_df.iloc[i]['ASSET_CODE'], "not working"]])
        return self

    def pen_it(self):
        self.df.to_excel("c:\\garbage\\AR\\output.xlsx",index=False)



if __name__ == "__main__":
    print("starting the work", dt.now())
    extracting_comp("c:\\garbage\\AR\\","c:\\garbage\\input_file.xlsx").downloading_pdf().pen_it()
    print("Ending the work", dt.now())