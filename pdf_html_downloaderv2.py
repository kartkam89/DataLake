import pandas as pd
import downloading_file as dowf
from datetime import datetime as dt
import pdfplumber as pp
import pdfkit
from collections import Counter

class file_download:
    def __init__(self,op_loc,df):

        self.op_loc = op_loc
        self.ip_df = df
        self.df = pd.DataFrame()
        print("length of dataframe is ",len(df))
        if len(df) != 0:
            self.downloading_pdf()
            self.pen_it()
        # self.comps = ['451 research',
        #                 'aberdeen',
        #                 'acoustics',
        #                 'appledore research',
        #                 'barc',
        #                 'biz tech insights',
        #                 'bloor',
        #                 'cabot partners group',
        #                 'cefro',
        #                 'celent',
        #                 'chartis',
        #                 'clabby analytics',
        #                 'creative',
        #                 'edison group',
        #                 'ema',
        #                 'esg',
        #                 'evaluator group',
        #                 'everest',
        #                 'forbes',
        #                 'forrester',
        #                 'frost & sullivan',
        #                 'futurum',
        #                 'g2',
        #                 'gartner',
        #                 'hardenstance',
        #                 'hfs',
        #                 'hfs research',
        #                 'hurwitz',
        #                 'ibm',
        #                 'ibv',
        #                 'idc',
        #                 'idg',
        #                 'ihl group',
        #                 'incisiv',
        #                 'intelligent business strategies',
        #                 'intelligent solutions',
        #                 'isg',
        #                 'isg insights',
        #                 'isg provider lens',
        #                 'it',
        #                 'it central station',
        #                 'itic',
        #                 'javelin',
        #                 'kuppinger',
        #                 'kuppingercole',
        #                 'nelsonhall',
        #                 'nucleus',
        #                 "o'reilly",
        #                 'other',
        #                 'ovum',
        #                 'oxford economics',
        #                 'ponemon',
        #                 'principled',
        #                 'pund-it',
        #                 'quark lepton',
        #                 'rfg',
        #                 'robert frances group',
        #                 'rti insights',
        #                 'rtinsights',
        #                 'sans',
        #                 'sc',
        #                 'sdx central',
        #                 'silverton',
        #                 'siverton consulting',
        #                 'solitaire',
        #                 'technology business research',
        #                 'techotherclarity',
        #                 'techtarget',
        #                 'tirias research',
        #                 'ventana',
        #                 'verdantix',
        #                 'ziff davis']


    def reading_text(self,file):
        with pp.open(file) as pdf:
            fp = pdf.pages[0].extract_text()
            lp = pdf.pages[-1].extract_text()
            keywords = ['commissioned','sponsored','in association with','in partnership with',
                        'report for','commissionato da','auftrag von','excerpt for','in zusammenarbeit mit',
                        'encargado por ','commandée par ','sponsor','solicitado por ','sponsorisé par',
                        'patrocinado por']


            for j in keywords:
                if j in fp.lower() or j in lp.lower():
                    comm = "Y"
                    break
                else:
                    comm = "N"
            #comp_name = self.comp_name([fp.lower(),lp.lower()])
            comp_name = ""
        return [fp,lp,comm,comp_name]


    # def comp_name(self,vals):
    #     # for k in self.comps:
    #     #     if vals[0].count(k) > 0 or vals[1].count(k) > 0:
    #     #         if k != "ibm":
    #     #             comp_name_ = k
    #     #         else:
    #     #
    #     #         break
    #     #     else:
    #     #         comp_name_ = "Others"
    #     comp_fp = Counter({k:vals[0].count(" " + k + " ") for k in self.comps})
    #     comp_lp = Counter({k:vals[1].count(" " + k + " ") for k in self.comps})
    #     comp_dict = dict(comp_fp + comp_lp)
    #     max_freq = list(comp_dict.keys())[list(comp_dict.values()).index(max(comp_dict.values()))]
    #     if max_freq == 'ibm':
    #         del comp_dict['ibm']
    #         max_freq = list(comp_dict.keys())[list(comp_dict.values()).index(max(comp_dict.values()))]
    #     comp_name_ = max_freq
    #     return comp_name_

    def downloading_pdf(self):
        print("There are "+ str(len(self.ip_df)) + " records and going through one by one")
        for i in range(len(self.ip_df)):
        #for i in range(150):
            try:
                if ".pdf" in self.ip_df.iloc[i]['ASSET_URL'].lower() or "download" in self.ip_df.iloc[i]['ASSET_URL'].lower():
                    print("working on ", i)
                    dowf.download_file(self.ip_df.iloc[i]['ASSET_URL'],self.op_loc+self.ip_df.iloc[i]['ASSET_CODE'])
                    self.df = self.df.append([[self.ip_df.iloc[i]['ASSET_CODE'],self.ip_df.iloc[i]['ASSET_URL'],
                                               self.ip_df.iloc[i]['ASSET_NAME'],

                                               '=Hyperlink("' + self.op_loc+self.ip_df.iloc[i]['ASSET_CODE']+'.pdf")',
                                               self.reading_text(self.op_loc+self.ip_df.iloc[i]['ASSET_CODE']+'.pdf')[0],
                                               self.reading_text(self.op_loc + self.ip_df.iloc[i]['ASSET_CODE'] + '.pdf')[1],
                                               self.reading_text(self.op_loc + self.ip_df.iloc[i]['ASSET_CODE'] + '.pdf')[2],
                                               self.reading_text(self.op_loc + self.ip_df.iloc[i]['ASSET_CODE'] + '.pdf')[3]
                                               ]])
                else:
                    print("working on ",i,", its a html page")
                    try:
                        config = pdfkit.configuration(wkhtmltopdf="C:\\Program Files\\wkhtmltopdf\\bin\\wkhtmltopdf.exe")
                        pdfkit.from_url(self.ip_df.iloc[i]['ASSET_URL'], self.op_loc+self.ip_df.iloc[i]['ASSET_CODE'] + ".pdf", configuration=config)
                        self.df = self.df.append([[self.ip_df.iloc[i]['ASSET_CODE'],
                                                   self.ip_df.iloc[i]['ASSET_URL'],
                                                   self.ip_df.iloc[i]['ASSET_NAME'],
                                                   '=Hyperlink("' + self.op_loc+self.ip_df.iloc[i]['ASSET_CODE']+'.pdf")',
                                                   self.reading_text(self.op_loc+self.ip_df.iloc[i]['ASSET_CODE']+'.pdf')[0],
                                                   self.reading_text(self.op_loc + self.ip_df.iloc[i]['ASSET_CODE'] + '.pdf')[1],
                                                   self.reading_text(self.op_loc + self.ip_df.iloc[i]['ASSET_CODE'] + '.pdf')[2],
                                                   self.reading_text(self.op_loc + self.ip_df.iloc[i]['ASSET_CODE'] + '.pdf')[3]]])

                    except:
                        print(self.ip_df.iloc[i]['ASSET_URL'])
                        self.df = self.df.append([[self.ip_df.iloc[i]['ASSET_CODE'], self.ip_df.iloc[i]['ASSET_URL'],self.ip_df.iloc[i]['ASSET_NAME'],"not working", "not working",
                                                   "not working", "not working","not working"]])

            except:
                print(self.ip_df.iloc[i]['ASSET_URL'])
                self.df = self.df.append([[self.ip_df.iloc[i]['ASSET_CODE'], self.ip_df.iloc[i]['ASSET_URL'],self.ip_df.iloc[i]['ASSET_NAME'],"not working", "not working", "not working", "not working","not working"]])
        return self

    def pen_it(self):
        #self.df.columns = ['asset_code','asset url','asset name','asset location','first page text','last page text','commissioned?',"comp_name"]
        self.df.to_excel("c:\\garbage\\AR\\output.xlsx",index=False)



if __name__ == "__main__":
    print("starting the work", dt.now())
    df = pd.read_excel("c:\\garbage\\new_file.xlsx")
    file_download("C:\\Users\\KarthickK\\Box\\VLRC Report\\2021Q3\\",df).downloading_pdf().pen_it()
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


