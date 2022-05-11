import pandas as pd
import numpy as np
from fuzzywuzzy import process
from fuzzywuzzy import fuzz as fw

class best_match:
    def __init__(self,text, list_text):
        self.text = text
        self.list_text = list_text
        #print(text,list_text)
        self.score()
        self.abbreviation_score()


    def score(self):
        vector = np.zeros((1,len(self.list_text)))
        for i in range(len(self.list_text)):

            #print(i,self.list_text[i])
            vector[0,i] = fw.ratio(self.list_text[i].lower(),self.text.lower()) + \
                          fw.token_sort_ratio(self.list_text[i].lower(),self.text.lower()) + \
                          fw.token_set_ratio(self.list_text[i].lower(), self.text.lower())

        self.score = [self.list_text[np.argmax(vector)],np.max(vector)/3]
        return self

    def abbreviation_score(self):
        vector = np.zeros((1, len(self.list_text)))
        self.abbreviated_text = []
        for i in range(len(self.list_text)):
            vector[0,i] = fw.ratio("".join([j[0].lower() for j in self.list_text[i].lower().split(" ") if len(j) > 0 and j != 'worldwide']),self.text.lower())
        self.abb_score = [self.list_text[np.argmax(vector)],np.max(vector)]

        if self.score[1] == self.abb_score[1]:
            self.output = self.score[0]
            print(self.output,self.score[1])
        elif self.score[1] > self.abb_score[1]:
            self.output = self.score[0]
            print(self.output, self.score[1])
        else:
            self.output = self.abb_score[0]
            print(self.output, self.abb_score[1])
        return self



if __name__ == "__main__":
    df = pd.read_excel("C:\\Users\\KarthickK\\Downloads\\mq_dot_plot.xlsx",sheet_name="Gartner Master Data")
    match_df = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\MQs & CCs.xlsx",sheet_name="List of MQs")

    gartner_words = ['MQ', 'CC for', 'CCs:', 'Magic Quadrant for', ' Magic Quadrant + Critical Capabilities for', 'Worldwide',
                     'Magic Quadrant and Critical Capabilities for ','Market Guide for ','Critical Capabilities for ',
                          'Magic Quadrant and Critical Capabilities for', '2019', '2020', '2021', '2022',
                          'Gartner', '/', ':', '-','Market Guide','Critical Capabilities']
    df['Title'] = df['Title'].astype("str")
    df['Original Title'] = df['Title']
    #match_df['Dummy Name'] = match_df['Name'].astype("str") + " " + match_df['Abbreviations'].astype("str")
    for i in range(len(df['Title'])):
        for j in gartner_words:
             df['Title'][i] = df.iloc[i]['Title'].replace(j, " ").strip(" ").strip(",").strip(":").replace("Worldwide","")

    # x = []
    # for i in df['Title'].astype(str):
    #     print(i)
    #     print(i.lower().split(" "))
    #     p = "".join([j[0] for j in i.lower().split(" ") if len(j) > 0])
    #     if  p != i.lower()  and not str(i) in match_df['Name']:
    #         x.append(best_match(str(i),match_df['Name']).output)
    #     else:
    #         x.append(i)

    x = [best_match(str(i),match_df['Name']).output
         if "".join([j[0].lower() for j in i.lower().split(" ") if len(j) > 0]) != i.lower() and
            not str(i) in match_df['Name'] else i for i in df['Title'].astype("str")]

    df['mq_abbreviations'] = x
    df.to_excel("c:\\garbage\\latestmqdotplotdata.xlsx")
