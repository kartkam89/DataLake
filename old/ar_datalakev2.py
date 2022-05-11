import pandas as pd
import numpy as np

class create_new_mq_data:
    def __init__(self,df):
        self.df = df
        self.columns = ['Firm', 'Original Title', 'Architect Project ID', 'AR Pro', 'Business Unit',
                        'Title', 'Title on Chart', 'plot x', 'Plot y', 'Leader?', 'Movement', 'Positive',
                        'Negative', 'New?', 'Ov Code', 'VLRC deal size', 'WR deal size', 'Title updated',
                        '$K VLRC deal size', '$K WR deal size', 'Key Reports for Hybrid Cloud', 'Key AI Story Report',
                        'Rankings', 'Growth Program Name', 'Growth Program Leader', 'Growth Program',
                        'SWG related', 'YTD?', 'Status', 'End Date']
    def preprocess(self):
        self.text_abbreviations = {'Platform': 'plat.',
                                   'Management': 'Mgmnt',
                                   'Application': 'App',
                                   'Performance': 'Perf.',
                                   'Service': 'Svc',
                                   'Solution': 'Soln.',
                                   'Financial': 'Fin.',
                                   'Infrastructure': 'Infra',
                                   'Machine Learning': 'ML',
                                   'Data Center Outsourcing': 'DCO',
                                   'Enterprise Asset Management': 'EAM',
                                   'Asia/Pacific': 'AP',
                                   'Asia Pacific':'AP',
                                   'Worldwide':'WW',
                                   'North America': 'NA',
                                   'Americas':'AM',
                                   'As a Service':'aaS'}

        self.df = self.df[(self.df['In suppression table']==0) & (self.df['Active report']=="Yes")]
        self.df['Title on Chart'] = [i.replace("Magic Quadrant for ","") for i in self.df['Title']]
        self.df['Title on Chart'] = list(map(self.abbreviation,self.df['Title on Chart']))
        self.df['Leader?'] = list(map(lambda x: 1 if x=="Leader" else 0, self.df['Leader?']))
        self.df['YTD?'] = list(map(lambda x: "Yes" if x == 2021 else "No", self.df['year']))

        self.df['Movement'] = list(map(lambda nx, ny, n_1x, n_1y: "No Coordinates" if np.isnan(nx) and np.isnan(ny) else
                                                                 ("New" if np.isnan(n_1x) and np.isnan(n_1y) else
                                                                 ("Positive" if ((1-nx)**2 + (1-ny)**2)**0.5 < ((1-n_1x)**2 + (1-n_1y)**2)**0.5
                                                            else ("Negative" if ((1-nx)**2 + (1-ny)**2)**0.5 > ((1-n_1x)**2 + (1-n_1y)**2)**0.5 else "Neutral"))),
                                                            self.df['x'],self.df['y'],self.df['n-1 x'],self.df['n-1 y']))
        return self



    def abbreviation(self, text):
        for i in self.text_abbreviations:
            text = text.replace(i, self.text_abbreviations[i])
        return text

    def output(self):
        columns_needed = ['Title','Document Code','Publish Date','Vendor Quadrant','Business Unit','AR Lead','Report Type','Interaction Count'
                          ,'x','y','n-1 x','n-1 y','Leader?','YTD?','Movement','Title on Chart']
        self.df = self.df[columns_needed]
        self.df.columns = ['Original Title','Architect Project ID','End Date','Ranking',	'Business Unit','AR Pro','Report Type','Interaction Count',
                          'plot x','Plot y','n-1 x','n-1 y','Leader?','YTD?','Movement','Title on Chart']
        self.df['Firm'] = list(map(lambda x: "Gartner" if x=="Magic Quadrant" else ("IDC" if x=="IDC Marketscape" else "Forrester"),self.df['Report Type']))
        self.df['Architect Project ID'] = ""
        self.df['plot x'] = self.df['plot x'].fillna(0)
        self.df['Plot y'] = self.df['Plot y'].fillna(0)


        return self

    def merge_with_architect(self):
        required_fields = ['Original Title','End Date_arch','mod_ranking','Business Unit','AR Pro','Report Type','Project Type','Publishing year',
                           'Interaction Count','plot x','Plot y','Leader?','Title on Chart','pred_firm','Project Id','non_eval_reports']
        architect_data = pd.read_excel("c:\\garbage\\merged_file.xlsx")[required_fields]
        architect_data = architect_data.rename({'End Date_arch':'End Date',
                                                'mod_ranking':'Ranking',
                                                'pred_firm':'Firm',
                                                'Project Id':'Architect Project ID'},axis=1)
        architect_data['YTD?'] = list(map(lambda x: "Yes" if '2021' in x else "No", architect_data['End Date']))
        architect_data['data_source'] = "Architect"
        self.df['data_source'] = 'VoT'
        self.df['non_eval_reports'] = 0
        self.df['Publishing year'] = ""
        architect_data['n-1 x'] = ""
        architect_data['n-1 y'] = ""
        architect_data['Movement'] = ''
        architect_data = architect_data[~architect_data['Firm'].isin(['Gartner','Forrester Research','IDC'])].append(self.df)
        totaldata = architect_data.rename({'End Date':'End Date_arch',
                                                'Firm':'pred_firm',
                                                'Architect Project ID':'Project Id'},axis=1)
        totaldata['Created By'] = totaldata['AR Pro']
        totaldata['mod_ranking'] = totaldata['Ranking']
        architect_data.to_excel("c:\\garbage\\archi.xlsx")
        totaldata.to_excel("c:\\garbage\\for_othercharts.xlsx")

        return self


if __name__ == "__main__":
    x = create_new_mq_data(pd.read_excel("C:\\Users\\KarthickK\\Downloads\\Datalake test build6.xlsm",sheet_name="Data combine").iloc[:,1:]).preprocess().output().merge_with_architect()
    x.df.to_excel("c:\\garbage\\patrick.xlsx",sheet_name="Gartner Master Data")

    "https://www.gartner.com/en/research/magic-quadrant"