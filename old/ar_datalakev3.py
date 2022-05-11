import pandas as pd
import numpy as np
import sqlalchemy as alch
from pi_key_generator import pi_key_generator as pk
from pi_key_generator import report_key_generator as rk

from datetime import datetime as dt

class create_new_mq_data:
    def __init__(self,df):
        self.df = df
        self.text_abbreviations = {'Platform': 'platf',
                                   'Management': 'Mgmnt',
                                   'Application': 'App',
                                   'Performance': 'Perf',
                                   'Solution': 'Soln',
                                   'Financial': 'Fin',
                                   'Infrastructure': 'Infra',
                                   'Machine Learning': 'ML',
                                   'Data Center Outsourcing': 'DCO',
                                   'Enterprise Asset Management': 'EAM',
                                   'Asia/Pacific': 'AP',
                                   'Asia Pacific': 'AP',
                                   'Worldwide': 'WW',
                                   'North America': 'NA',
                                   'Europe':'EUR',
                                   'Americas': 'AM',
                                   'As a Service': 'aaS',
                                   'Software':'SW',
                                   'Hardware':'HW',
                                   ' and ':'&',
                                   'As-A-Service': 'aas',
                                   'Database':'DB',
                                   'Business':'Biz',
                                   'Intelligence':'Intel',
                                   "Artificial Intelligence":'AI',
                                   'Service': 'Svc'}
        self.defin = {'int': ['YEAR', 'QUARTER', 'REPORT_TOTAL', 'IN_SUPPRESSION_TABLE', 'LEADER?'],
                 'float': ['INTERACTION_COUNT', 'X', 'Y', 'N-1_X', 'N-1_Y', 'dist', 'p_dist', 'change_percent'],
                 'date': ['PUBLISH_DATE', 'DATE2', 'NEXTGENDATE', 'DATE3', 'NGDATE2']}

    def drop_unwanted_cols(self):
        drop_col = []
        for i in list(self.df.columns):
            try:
                if "Calculated Field" in i:
                    drop_col.append(i)
            except:
                pass
        drop_col.append(self.df.columns[0])
        drop_col.append(1)
        self.df = self.df.drop(drop_col,axis=1)

        return self

    def abbreviation(self, text):
        for i in self.text_abbreviations:
            text = text.replace(i, self.text_abbreviations[i])
        return text

    def remove_unwanted_text(self):
        self.df['Firm'] = list(map(lambda x: "Gartner" if x == 'Gartner Magic Quadrant' else ("Forrester" if x == "Forrester Wave" else "IDC"),self.df['Report Type']))
        self.df['Title on Chart'] = list(map(lambda x,y: y.replace("Magic Quadrant for ","") if x == 'Gartner' else y ,self.df['Firm'],self.df['Title']))
        self.df['Title on Chart'] = list(map(lambda x,y: y[:-9] if x == 'Forrester' else y, self.df['Firm'],self.df['Title on Chart']))
        self.df['20xx vendor assessment'] = self.df['Title'].str.contains("20.*–20.* Vendor Assessment")
        self.df['Title on Chart'] = list(map(lambda x, y, z: y if z != 'IDC' else (y[:y.find("Vendor Assessment") - 10].strip().replace("IDC MarketScape: ","") if x == True  else ( y[:y.find(
            "Vendor Assessment") - 5].strip().replace("IDC MarketScape: ",""))), self.df['20xx vendor assessment'], self.df['Title on Chart'], self.df['Firm']))
        self.df['Title on Chart'] = [i.strip() for i in self.df['Title on Chart']]
        self.df['Title on Chart'] = list(map(self.abbreviation, self.df['Title on Chart']))
        self.df['Leader?'] = [1 if i == 'Leader' else 0 for i in self.df['Leader?']]
        self.df = self.df.drop(['20xx vendor assessment','nextgendate'],axis=1)

        return self

    def removenan(self):
        self.df['Licensed externally'] = self.df['Licensed externally'].fillna("error").replace(0,"Zeros")
        self.df['Participants'] = self.df['Participants'].fillna("error").replace(0,"Zeros")
        return self


    def data_def(self, dty):
        diction = {}
        for i in dty:
            if type(dty[i]) == list:
                for j in dty[i]:
                    if i == 'int':
                        diction[j] = alch.types.INTEGER()

                    elif i == 'float':
                        diction[j] = alch.types.FLOAT()

                    elif i == 'date':
                        diction[j] = alch.types.DATE()

                    else:
                        diction[j] = alch.types.VARCHAR(500)
            else:
                if dty[i] == 'int':
                    diction[i] = alch.types.INTEGER()

                elif dty[i] == 'float':
                    diction[i] = alch.types.FLOAT()
                elif dty[i] == 'date':
                    diction[i] = alch.types.DATE()

                else:
                    diction[i] = alch.types.VARCHAR(500)

        print(diction.keys())
        check = set(self.df.columns) - set(diction.keys())
        for i in check:
            diction[i] = alch.types.VARCHAR(500)
        print(diction)
        return diction

    def drop_personal_information(self):
        self.df['AR Lead Key'] = [pk(i).result for i in self.df['AR Lead']]
        self.df = self.df.drop(['AR Lead','Participants'],axis=1)
        return self

    def calculations(self):
        self.df['dist'] = list(map(lambda x, y: ((1-x)**2 + (1-y)**2)**0.5, self.df['x'],self.df['y']))
        self.df['p_dist'] = list(map(lambda x, y: ((1-x)**2 + (1-y)**2)**0.5, self.df['n-1 x'],self.df['n-1 y']))
        self.df['change_percent'] = (self.df['dist'] - self.df['p_dist'])/self.df['p_dist']
        self.df['change'] = list(map(lambda a,x,y: "New" if np.isnan(x) and np.isnan(y) else
                                    ("Neutral" if a >= -0.01 and a <= 0.01 else
                                    ("Negative" if a > 0 else "Positive")),self.df['change_percent'],self.df['n-1 x'],self.df['n-1 y']))

        return self

    def tier2(self):
        self.df = self.df.drop("Unnamed: 0",axis=1)
        self.t2_map = {'HfS Top 10':'HFS',
                       'Everest Peak Matrix':'Everest',
                       'Omdia':'Omdia'}
        self.df['New Doc Code'] = list(map(lambda a,b,c,d : rk(a,self.t2_map[c] + " " + str(b)[-2:],d).result , self.df['Title'],self.df['year'],self.df['Report Type'],self.df['Document Code']))
        self.df = self.df.drop("Document Code",axis=1)
        self.df['Score'] = self.df['Score'].astype("str")
        return self



    def write(self,schema, tblname, dty, drop=None):
        db2 = alch.create_engine(
            'ibm_db_sa://kartkam:yGuI7wqOAT4wp1Gc@g42-a-cmdpprod1.az2.ash.cpc.ibm.com:51000/cmdp;SECURITY=SSL;SSLCLIENTKEYSTOREDB=C:\db2_ssl\IBM_DB2_SSL_Client.kdb;SSLCLIENTKEYSTASH=C:\db2_ssl\IBM_DB2_SSL_Client.sth')
        con = db2.connect()
        if drop != None:
            self.df = self.df.drop(drop, axis=1)
        dty = self.data_def(dty)
        self.df.columns = [i.replace(" ", "_") for i in self.df.columns]
        self.df['load_ts'] = dt.now()
        self.df.to_sql(tblname, con=con, schema=schema, index=True, chunksize=100, dtype=dty, if_exists="replace")
        #[self.df.columns[:30]]
        return self

    def write_cmdp(self,schema, tbl,defin):
        print(self.df.dtypes)
        print(self.df.shape)
        print(self.df.iloc[0])
        self.df.columns = [i.replace(" ","_").upper() for i in self.df.columns]


        #self.write("ACS_RGWW1", "AR_VOT_TIER1".lower(), dty=defin)
        self.write(schema, tbl.lower(), dty=defin)





if __name__ == "__main__":
    print(dt.now())
    x = create_new_mq_data(pd.read_excel("C:\\Users\\KarthickK\\Downloads\\Datalake scratch v13.xlsm",sheet_name="Data combine"))
    x.drop_unwanted_cols().remove_unwanted_text().removenan().drop_personal_information().calculations().write_cmdp("ACS_RGWW1", "AR_VOT_TIER1".lower(), dty=defin)

    y = create_new_mq_data(pd.read_excel("C:\\Users\\KarthickK\\Downloads\\Datalake scratch v13.xlsm",sheet_name="T2 Data combine"))
    defin = {"int":['YEAR'],
             'date':['PUBLISH_DATE']}
    y.tier2().write_cmdp("ACS_RGWW1","AR_VOT_TIER2",defin)
    y.df.to_excel("c:\\garbage\\checkv3.xlsx")
    print(dt.now())