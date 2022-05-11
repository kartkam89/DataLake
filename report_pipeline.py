import pandas as pd
import numpy as np


class report_train:
    def __init__(self,data):
        self.data = pd.read_excel(data).drop("Column1",axis=1)

    def remove_years(self):
        with pd.ExcelWriter("c:\\garbage\\report_train.xlsx") as writer:
            pd.pivot_table(index="Category",columns=["Year","Ranking"],data=self.data).to_excel(writer,"view1")
            self.data['new_ranking'] = self.data['Ranking'].replace({"Contender":0,"Strong Performer":1,"Leader":2})
            pd.pivot_table(index="Category",columns=["Year"],data=self.data).to_excel(writer,"view2")
            #self.data[['Category','Year','Ranking']].groupby([])


class idc_report_train:
    def __init__(self,data):
        self.data = pd.read_excel(data,sheet_name="2019 to 2021")

    def remove_years(self):
        self.data['20xx vendor assessment'] = self.data['MarketScape Title'].str.contains("20.*–20.* Vendor Assessment")
        self.data['new_title'] = list(map(lambda x,y: y[:y.find("Vendor Assessment")-10].strip()  if x else y[:y.find("Vendor Assessment")-5].strip() ,self.data['20xx vendor assessment'],self.data['MarketScape Title']))
        self.data['new_ranking'] = self.data['Ranking'].replace({"Contenders":0,"Major Players":1,"Leaders":2})
        pivoted = pd.pivot_table(index="new_title", columns=["Year"],
                                 data=self.data)
        pivoted.to_excel("c:\\garbage\\report_train_idc.xlsx")

        for i in range(len(pivoted)):
            for j in pivoted['new_ranking'].columns:
                if np.isnan(pivoted.iloc[i]['new_ranking',j])==False:
                    title = pivoted.index[i]
                    print(title)
                    title_df = self.data[self.data['new_title']==title]
                    year_df = title_df[title_df['Year']==j]
                    pivoted.iat[i,pivoted.columns.get_loc(j)] = year_df['Document Number']


        pivoted.to_excel("c:\\garbage\\new_idc_report.xlsx")



class gartner_report_train:
    def __init__(self,data):
        self.data = pd.read_excel(data, sheet_name="IBM 2020 - 2021")

    def remove_years(self):
        with pd.ExcelWriter("c:\\garbage\\gartner_report_train.xlsx") as writer:
            pd.pivot(index="Title - net",columns=["Report Year","Vendor Quadrant"],data=self.data).to_excel(writer,"view1")
            self.data['new_ranking'] = self.data['Vendor Quadrant'].replace({'Leaders':2,'Challengers':1,'Visionaries':0,'Niche Players':-1})
            #pd.pivot(index="Title - net",columns=["Report Year"],data=self.data[['new_ranking','Title - net','Report Year']]).to_excel(writer,"view2")



if __name__ == "__main__":
    #report_train("C:\\Users\\KarthickK\\Downloads\\Forrester Waves for IBM_updated_Jan 2019-Jan 2022 (003).xlsx").remove_years()
    idc_report_train("C:\\Users\\KarthickK\\Downloads\\IDC MarketScapes 2019-2021.xlsx").remove_years()
    #gartner_report_train("C:\\Users\\KarthickK\\Downloads\\Gartner Vault of Truth -- November 2021 v1.xlsm").remove_years()
