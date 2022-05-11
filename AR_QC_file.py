import pandas as pd
import numpy as np
from predict_firm import predict_firm as pf
from datetime import datetime as dt
from fuzzywuzzy import process as pro

class qc_class:
    def __init__(self,input_file, output_file,st_date,end_date):
        print(dt.now())
        self.input_data = pd.read_excel(input_file)
        self.input_data['Start Date'] = pd.to_datetime(self.input_data['Start Date'])
        self.input_data['End Date'] = pd.to_datetime(self.input_data['End Date'])
        self.input_data = self.input_data[(self.input_data['End Date']>=st_date)&(self.input_data['End Date']<=end_date)]
        self.input_data['Start Date'] = self.input_data['Start Date'].dt.strftime("%m/%d/%Y")
        self.input_data['End Date'] = self.input_data['End Date'].dt.strftime("%m/%d/%Y")
        self.input_data.rename(columns={'Leader': 'AR Pro'}, inplace=True)

        #self.input_data['End Date'] = self.input_data['End Date'].dt.strftime('%d/%m/%Y')
        self.output_file = output_file
        self.text_abbreviations = {'Platform':'plat',
                                   'Management':'Mgmnt',
                                   'Application':'App',
                                   'Performance':'Perf',
                                   'Service':'Svc',
                                   'Solution':'Soln',
                                   'Financial':'Fin',
                                   'Infrastructure':'Infra',
                                   'Machine Learning':'ML',
                                   'Data Center Outsourcing':'DCO',
                                   'Enterprise Asset Management':'EAM',
                                   'Asia/Pacific':'AP',
                                   'North America':'NA'}

        self.gartner_words = ['MQ','CC for','CCs:','Magic Quadrant for',' Magic Quadrant + Critical Capabilities for',
                              'Magic Quadrant and Critical Capabilities for','2019','2020','2021','2022',
                              'Gartner','/',':','-']



        self.processing()
    def processing(self):
        self.identify_leaders()
        self.identify_issues()
        self.morethanonefirm()
        self.publish_year_mismatch()
        self.end_year_title_year()
        self.publish_yr_now_yr()
        self.identify_title_duplicates()
        self.identify_project_duplicates()
        self.identify_non_alpha_num_description()
        self.add_firms()
        self.leaders_exception()
        self.identify_ranking()
        self.add_comments()
        self.issue_count()
        self.comments()
        #self.abbreviate_title()
        #self.abbreviate_firm()
        #self.abbreviate_ranking()
        #self.clean()
        self.add_projects_flag_file()
        self.output_it()

    def identify_leaders(self):
        self.input_data['Ranking'] = self.input_data['Ranking'].fillna("Blanks")
        self.input_data['Leader'] = list(map(lambda x: 1 if "Leader" in x else 0, self.input_data['Ranking']))

    def leaders_exception(self):
        self.input_data['Leader'] = list(map(lambda x, y, z: 0 if y  in ['KuppingerCole Analysts AG','HFS Research',"Ventana Research"] else z, self.input_data['Ranking'], self.input_data['pred_firm'],
                                                 self.input_data['Leader']))

        self.input_data['Leader'] = list(map(lambda x,y,z: 1 if y == 'KuppingerCole Analysts AG' and "Overall Leader" in x
                                                else (1 if y=='HFS Research' and  x =="HfS Research Top 10 -- #1 to #5"
                                                else(1 if y=="Ventana Research" and x=="Ventana Value Index - Exemplary" else z)),self.input_data['Ranking'],self.input_data['pred_firm'],self.input_data['Leader']))



    def identify_issues(self):
        self.input_data['tbd_records'] = list(map(lambda x: 1 if x == 'Other / TBD' else 0,self.input_data['Ranking']))
        self.input_data['other_reports'] = list(map(lambda x: 1 if x == 'Other' else 0,self.input_data['Report Type']))
        non_eval = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\Report Types.xlsx")
        non_eval = list(non_eval[non_eval['Comparative_report?'] == 0]['Report Type'].unique())
        self.input_data['non_eval_reports'] = list(map(lambda y: 1 if y in non_eval else 0,self.input_data['Report Type']))
        self.input_data['timestamp'] = dt.now()

    def morethanonefirm(self):
        #firms end were checked as of Oct 27, 2021
        self.input_data['Firms'] = self.input_data['Firms'].fillna("Blank")
        self.firms_end = [", Inc.",", Ltd.",", LLC",", Publications & Consulting",", Consultants",", LC",
                     ", a MinterEllison Company", ", Consultant",", L.L.C.",", Chinese Academy of Sciences"]
        self.input_data["more_than_one_comp"] = list(map(self.replace_commas, self.input_data["Firms"]))
        self.input_data['pub_yr_end_yr'] = list(map(lambda x,y: 1 if str(x) != str(y)[-4:] else 0,self.input_data['Publishing year'],self.input_data['End Date']))

    def replace_commas(self,firm):
        for j in self.firms_end:
            firm = firm.replace(j,"")
        return len(firm.split(",")) > 1

    def publish_year_mismatch(self):
        self.input_data['pub_yr_title_yr'] = list(map(lambda x,y: 1 if str(int(y)-1) in x else
                                                                 (1 if str(int(y)-2) in x else
                                                                 (1 if str(int(y)-3) in x else
                                                                 (1 if " " + str(int(y)-2001) + " " in x  else
                                                                 (1 if " " + str(int(y) - 2002)+ " " in x else
                                                                 (1 if " " + str(int(y) - 2003)+ " " in x else
                                                                  0))))),self.input_data['Title'],self.input_data['Publishing year']))

    def publish_yr_now_yr(self):
        self.input_data['pub_yr_now_yr'] = list(map(lambda x: 1 if x == '2021' else 0, self.input_data['Publishing year']))
        return self


    def identify_ranking(self):
        ranking = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\Report type by ranking mappingv2.xlsx",sheet_name="data")
        self.input_data['ranking_check'] = list(map(lambda x,y : 0 if y in list(ranking[ranking['Report Type'] == x]['Ranking'].unique()) else 1,self.input_data['Report Type'],self.input_data['Ranking']))
        x = []
        counter = 0
        for i,j in zip(self.input_data['Ranking'],self.input_data['Report Type']):
            print(i,j)

            df = list(ranking[ranking['Report Type']==j]['Ranking'].unique())

            if j!= 'Other' and not i in ['Other / TBD','TBD']:
                print(i)
                print(df)
                x.append(pro.extractOne(i,df)[0])
            else:
                x.append("check")
            print(counter)
            counter = counter + 1
        self.input_data['mod_ranking'] = x
            #list(map(lambda x,y: pro.extractOne(x,list(ranking[ranking['Report Type']==y]['Ranking'].unique())) if y != 'Other' else x,self.input_data['Ranking'],self.input_data['Report Type']))
        return self

    def end_year_title_year(self):
        self.input_data['end_yr_title_yr'] = list(map(lambda x,y: 1 if str(int(y[-4:])-1) in x else
                                                                 (1 if str(int(y[-4:])-2) in x else
                                                                 (1 if str(int(y[-4:])-3) in x else
                                                                 (1 if " " + str(int(y[-4:])- 2001) + " " in x else
                                                                 (1 if " " + str(int(y[-4:]) - 2002)+ " " in x else
                                                                 (1 if " " + str(int(y[-4:]) - 2003)+ " " in x else
                                                                  0))))),self.input_data['Title'],self.input_data['End Date'].astype("str")))

    def identify_non_alpha_num_description(self):
        self.input_data['non_alpha_num_desc'] = list(map(lambda x: 1 if x[0].isalnum() == False else 0,self.input_data['Description']))
        self.input_data['mod_Description'] = list(map(lambda x,y: y.replace(y[0],"") if x ==1 else y,self.input_data['non_alpha_num_desc'],self.input_data['Description']))


    def identify_title_duplicates(self):
        duplicate_records = pd.DataFrame(self.input_data['Title'].value_counts()).reset_index()
        duplicate_records.columns = ['Title_', 'title_duplication_count']
        self.input_data = self.input_data.set_index('Title').join(duplicate_records.set_index("Title_")).reset_index()
        #self.input_data = self.input_data.drop("Title_", axis=1)

    def identify_project_duplicates(self):
        duplicate_records = pd.DataFrame(self.input_data['Project Id'].value_counts()).reset_index()
        duplicate_records.columns = ['Project Id_', 'project_duplication_count']
        self.input_data = self.input_data.set_index('Project Id').join(
            duplicate_records.set_index("Project Id_")).reset_index()
        #self.input_data = self.input_data.drop("Project Id_", axis=1)

    def add_comments(self):
        self.input_data['issues'] = list(map(self.comments_logic,
                                        self.input_data['tbd_records'],
                                        self.input_data['other_reports'],
                                        self.input_data['non_eval_reports'],
                                        self.input_data['ranking_check'],
                                        self.input_data['more_than_one_comp'],
                                        self.input_data['pub_yr_title_yr'],
                                        self.input_data['pub_yr_end_yr'],
                                        self.input_data['end_yr_title_yr'],
                                        self.input_data['pub_yr_now_yr'],
                                        self.input_data['title_duplication_count'],
                                        self.input_data['project_duplication_count'],
                                        self.input_data['non_alpha_num_desc']))


    def comments_logic(self,tbd_record,other_reports,non_eval_reports,ranking_check,more_than_one_comp,pub_yr_title_yr,
                       pub_yr_end_yr, end_yr_title_yr, pub_yr_now_yr,title_duplication_count, project_duplication_count,
                       non_alpha_num_desc):
        tbd_record_comment = "Ranking: TBD Record"
        other_reports_comment = "Report Type: Other"
        non_eval_reports_comment = "Non Evaluative Report"
        ranking_check_comment = "Ranking possibly incorrect"
        more_than_one_comp_comment = "More than one Firm"
        pub_yr_title_yr_comment = "Year in Title and Publishing Year does not match"
        pub_yr_end_yr_comment = "Year in End Date does not match with Publishing Year"
        end_yr_title_yr_comment = "Year in End Date does not match year in Title"
        pub_yr_now_yr_comment = "Publishing year is not current year"
        title_duplication_count_comment = "Title mentioned in this record is same as another record"
        project_duplication_count_comment = "Project Id mentioned in this record is same as another record"
        non_alpha_num_desc_comment = "Description starts with non alpha numeric character"


        final_comment = ""
        if non_eval_reports == 1:
            final_comment = final_comment + non_eval_reports_comment

        if tbd_record == 1:
            final_comment= final_comment + "|" + tbd_record_comment

        if other_reports == 1:
            final_comment = final_comment + "|" +other_reports_comment

        if ranking_check == 1:
            final_comment = final_comment + "|" +ranking_check_comment

        # if more_than_one_comp == True:
        #     final_comment = final_comment + "|" + more_than_one_comp_comment

        if pub_yr_title_yr == 1:
            final_comment = final_comment + "|" + pub_yr_title_yr_comment

        if pub_yr_end_yr == 1:
            final_comment = final_comment + "|" + pub_yr_end_yr_comment

        if end_yr_title_yr == 1:
            final_comment = final_comment + "|" + end_yr_title_yr_comment

        if pub_yr_now_yr == 1:
            final_comment = final_comment + "|" + pub_yr_now_yr_comment

        if title_duplication_count > 1:
            final_comment = final_comment + "|" + title_duplication_count_comment

        if project_duplication_count > 1:
            final_comment = final_comment + "|" + project_duplication_count_comment

        if non_alpha_num_desc == 1:
            final_comment= final_comment + "|" + non_alpha_num_desc_comment




        return final_comment.lstrip("|").rstrip("|")

    # def add_firms(self):
    #     gr_thn_1_firm = self.input_data[self.input_data['more_than_one_comp']==True][['index','Report Type','Ranking']]
    #     gr_thn_1_firm['concat'] = gr_thn_1_firm['index'].astype("str") + " " + gr_thn_1_firm['Report Type'].astype("str") + " " + gr_thn_1_firm['Ranking'].astype("str")
    #     gr_thn_1_firm['pred_firm'] = list(map(lambda x: pf(x).processing().output,gr_thn_1_firm['concat']))
    #     self.input_data = self.input_data.join(gr_thn_1_firm['pred_firm'])

    def add_firms(self):
        #self.input_data['concat'] = self.input_data['index'].astype("str") + " " + self.input_data['Report Type'].astype("str") + " " + self.input_data['Ranking'].astype("str")
        self.input_data['concat'] = self.input_data['Title'].astype("str") + " " + self.input_data[
            'Report Type'].astype("str") + " " + self.input_data['Ranking'].astype("str")
        self.input_data['pred_firm'] = list(map(lambda x,y,z: z if x == False and z != 'Analysts In-Transit' else pf(y).processing().output,self.input_data['more_than_one_comp'],self.input_data['concat'],self.input_data['Firms']))
        return self

    def abbreviate_firm(self):
        firms_abb = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\firm.xlsx")
        firms_abb = {firms_abb.iloc[i]['Firms']:firms_abb.iloc[i]['New_Firms'] for i in range(len(firms_abb)) if i in firms_abb['Firms']}
        self.input_data['pred_firm'] = list(map(lambda x: firms_abb[x] if x in firms_abb.keys() else x,self.input_data['pred_firm']))
        return self

    def abbreviate_ranking(self):
        ranking_abb = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\rankingv1.xlsx")
        ranking_abb = {ranking_abb.iloc[i]['Ranking']: ranking_abb.iloc[i]['New_Ranking'] for i in range(len(ranking_abb)) if i in ranking_abb['Ranking']}
        self.input_data['Ranking'] = list(map(lambda x: ranking_abb[x] if x in ranking_abb.keys() else x, self.input_data['Ranking']))
        return self

    def add_projects_flag_file(self):
        flag_file = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\Tier 1 hybrid cloud & AI flagsv1.xlsx")
        projects = flag_file['Project Id'].unique()
        missing_projects = set(self.input_data[self.input_data['pred_firm'].isin(['Gartner Inc.','IDC','Forrester Research, Inc.'])]['Project Id'].unique())-set(projects)#new projects
        missing_projects = self.input_data[self.input_data.index.isin(missing_projects)][['Project Id',	'Title','End Date']]
        missing_projects['Hybrid Cloud related'] = ''
        missing_projects['AI related'] = ''
        missing_projects['Hybrid Cloud & AI related'] = ''
        appended_file = flag_file.append(missing_projects)
        appended_file.to_excel("c:\\garbage\\flag_check.xlsx")
        return self


    def clean(self):
        self.input_data = self.input_data.drop(
            ["concat", 'tbd_records', 'other_reports', 'non_eval_reports', 'ranking_check', 'more_than_one_comp',
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
        x.insert(x.index("Firms") + 1,"pred_firm")
        x.insert(x.index('Description') + 1, "mod_Description")
        x.insert(x.index("Title")+1,"Title on Chart")
        self.input_data = self.input_data[x]
        return self

    def abbreviate_title(self):
        #self.input_data['Title on Chart'] = list(map(self.abbreviation,self.input_data['index']))
        self.input_data['Title on Chart'] = list(map(self.abbreviation, self.input_data['Title']))
        return self

    def abbreviation(self,text):
        for i in self.text_abbreviations:
            text = text.replace(i,self.text_abbreviations[i])
            for j in self.gartner_words:
                text = text.replace(j,"").strip(" ").strip(",").strip(":")
        return text


    def issue_count(self):
        self.input_data['issue_count'] = list(map(lambda x: len(x)-len(x.replace("|",""))+1 if x != '' else 0, self.input_data['issues'].astype("str")))

    def comments(self):
        self.input_data['comments'] = list(map(lambda x : "More than Firm" if x == True else '',self.input_data['more_than_one_comp']))




    def output_it(self):
        self.input_data.rename(columns={'Leader':'Leader?'},inplace=True)
        self.input_data.to_excel(self.output_file,index=False)
        print(dt.now())


if __name__ == "__main__":
    qc_class("C:\\Users\\KarthickK\\Downloads\\Projects_20220222.xlsx","C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\QC\\QC_output.xlsx",'2019-01-01','2022-12-31')
