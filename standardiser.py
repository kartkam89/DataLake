import pandas as pd
import numpy as np
from text_best_match import best_match
from datetime import datetime as dt
class standardiser:
    def __init__(self,ip_df,op_df):
        print(dt.now())
        self.input_data = ip_df
        self.op_df = op_df
        self.text_abbreviations = {'Platform': 'plat',
                                   'Management': 'Mgmnt',
                                   'Application': 'App',
                                   'Performance': 'Perf',
                                   'Service': 'Svc',
                                   'Solution': 'Soln',
                                   'Financial': 'Fin',
                                   'Infrastructure': 'Infra',
                                   'Machine Learning': 'ML',
                                   'Data Center Outsourcing': 'DCO',
                                   'Enterprise Asset Management': 'EAM',
                                   'Asia/Pacific': 'AP',
                                   'North America': 'NA'}

        self.gartner_words = ['MQ', 'CC for', 'CCs:', 'Magic Quadrant for',
                              ' Magic Quadrant + Critical Capabilities for',
                              'Magic Quadrant and Critical Capabilities for', '2019', '2020', '2021', '2022',
                              'Gartner', '/', ':', '-']
        self.input_data['Original Title'] = self.input_data['Title']
        self.processing()

    def processing(self):
        self.abbreviate_firm()
        self.gartner_title()
        self.abbreviate_title()
        self.abbreviate_ranking()
        self.clean()
        self.gartner_title()
        self.penit()

    def abbreviate_firm(self):
        firms_abb = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\firm.xlsx")
        firms_abb = {firms_abb.iloc[i]['Firms']: firms_abb.iloc[i]['New_Firms'] for i in range(len(firms_abb)) if
                     i in firms_abb['Firms']}
        print(firms_abb.keys())
        self.input_data['pred_firm'] = list(
            map(lambda x: firms_abb[x] if x in firms_abb.keys() else x, self.input_data['pred_firm']))
        return self

    def abbreviate_ranking(self):
        ranking_abb = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\rankingv1.xlsx")
        ranking_abb = {ranking_abb.iloc[i]['Ranking']: ranking_abb.iloc[i]['New_Ranking'] for i in
                       range(len(ranking_abb)) if i in ranking_abb['Ranking']}
        self.input_data['mod_ranking'] = list(
            map(lambda x: ranking_abb[x] if x in ranking_abb.keys() else x, self.input_data['mod_ranking']))
        return self



    def clean(self):
        self.input_data = self.input_data.drop(
            ["concat", 'tbd_records', 'other_reports', 'ranking_check', 'more_than_one_comp',
             'pub_yr_end_yr', 'pub_yr_title_yr', 'end_yr_title_yr', 'pub_yr_now_yr',
             'title_duplication_count', 'project_duplication_count', 'non_alpha_num_desc'], axis=1)
        x = self.input_data.columns
        x = list(x)
        x[0] = 'Project Id'
        x[1] = 'Title'
        self.input_data.columns = x
        x.remove("pred_firm")
        x.remove("mod_Description")
        x.remove("Title on Chart")
        x.insert(x.index("Firms") + 1, "pred_firm")
        x.insert(x.index('Description') + 1, "mod_Description")
        x.insert(x.index("Title") + 1, "Title on Chart")
        self.input_data = self.input_data[x]
        return self

    def abbreviate_title(self):
        # self.input_data['Title on Chart'] = list(map(self.abbreviation,self.input_data['index']))
        self.input_data['Title on Chart'] = list(map(self.abbreviation, self.input_data['gartner_title']))
        return self

    def abbreviation(self, text):
        for i in self.text_abbreviations:
            text = text.replace(i, self.text_abbreviations[i])
            for j in self.gartner_words:
                text = text.replace(j, "").strip(" ").strip(",").strip(":")
        return text

    def gartner_title(self):
        self.match_df = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\MQs & CCs.xlsx",
                                 sheet_name="List of MQs")
        gartner_words = ['MQ', 'CC for', 'CCs:', 'Magic Quadrant for', ' Magic Quadrant + Critical Capabilities for',
                         'Worldwide',
                         'Magic Quadrant and Critical Capabilities for ', 'Market Guide for ',
                         'Critical Capabilities for ',
                         'Magic Quadrant and Critical Capabilities for', '2019', '2020', '2021', '2022',
                         'Gartner', '/', ':', '-', 'Market Guide', 'Critical Capabilities']
        self.input_data['Title'] = self.input_data['Title'].astype("str")
        #self.input_data['Original Title'] = self.input_data['Title']
        for i in range(len(self.input_data['Title'])):
            for j in gartner_words:
                self.input_data['Title'][i] = self.input_data.iloc[i]['Title'].replace(j, " ").strip(" ").strip(",").strip(":").replace(
                    "Worldwide", "")

        x = [best_match(str(i), self.match_df['Name']).output
             if "".join([j[0].lower() for j in i.lower().split(" ") if len(j) > 0]) != i.lower() and
                not str(i) in self.match_df['Name'] else i for i in self.input_data['Title'].astype("str")]

        self.input_data['gartner_title'] = x
        return self

    def penit(self):
        self.input_data.to_excel(self.op_df)
        print(dt.now())



if __name__ == "__main__":
    standardiser(pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\QC\\QC_output.xlsx"),"C:\\Users\\KarthickK\\Downloads\\std_output_Projects_YTD.xlsx")


