from random import randint
import pandas as pd
import numpy as np

class pi_key_generator:
    def __init__(self,vals):
        self.vals = vals
        self.df = pd.read_excel("C:\\garbage\\AR Lead Keys.xlsx")
        self.check_add()
        self.write()

    def check_add(self):
        if self.vals in self.df['AR Lead'].unique():
            self.result = self.df[self.df['AR Lead']==self.vals]['AR Lead Key'].values[0]
        else:
            self.result = self.add_ar()
            print(self.df)
        return self.result

    def add_ar(self):
        if type(self.vals) == str:
            keys = self.pi_key(self.vals)
            self.df = pd.concat([self.df,pd.DataFrame([[self.vals,keys]],columns=["AR Lead","AR Lead Key"])],ignore_index=True)
            #self.df = self.df.append([[self.vals, keys]], ignore_index=True)
            return keys
        else:
            return "Not a Name"


    def pi_key(self,x):
        x = x.replace(",","").replace(".","").replace("'","").replace("  "," ").strip(" ")
        temp = x.split(" ")
        keys = ""
        for i in temp:
            if len(i) >= 3:
                keys = keys + i[:3].upper() + "-"
            else:
                keys = keys + i.upper() + "-"
        keys = keys + str(randint(1000,9999))
        return keys

    def write(self):
        self.df.to_excel("C:\\garbage\\AR Lead Keys.xlsx",index=False)



class report_key_generator:
    def __init__(self,title, vals, doc_code):
        self.vals = vals
        self.title = title
        self.doc_code = doc_code
        self.df = pd.read_excel("C:\\garbage\\AR report keys.xlsx")
        self.check_add()
        self.write()

    def check_add(self):
        if self.title in self.df['Report'].unique():
            self.result = self.df[self.df['Report']==self.title]['Report Key'].values[0]
        else:
            self.result = self.add_ar()
            print(self.df)
        return self.result

    def add_ar(self):
        if pd.isnull(self.doc_code):
            if type(self.vals) == str:
                keys = self.pi_key(self.vals)
                self.df = pd.concat([self.df,pd.DataFrame([[self.title,keys]],columns=["Report","Report Key"])],ignore_index=True)
                #self.df = self.df.append([[self.vals, keys]], ignore_index=True)
                return keys
            else:
                return "Not a Name"
        else:
            self.df = pd.concat([self.df, pd.DataFrame([[self.title, self.doc_code]], columns=["Report", "Report Key"])],ignore_index=True)
            return self.doc_code

    def pi_key(self,x):
        x = x.replace(",","").replace(".","").replace("'","").replace("  "," ").replace("("," ").replace(")"," ").strip(" ")
        temp = x.split(" ")
        keys = ""
        for i in temp:
            if len(i) >= 3:
                keys = keys + i[:3].upper() + "-"
            else:
                keys = keys + i.upper() + "-"
        keys = keys + str(randint(10000,99999))
        return keys #"-".join(set(keys.split("-")))

    def write(self):
        self.df.to_excel("C:\\garbage\\AR Report Keys.xlsx",index=False)






