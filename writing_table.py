import pandas as pd
import sqlalchemy as alch
from datetime import datetime as dt
import os
import numpy as np

def data_def(df,dty=None):
    diction = {}
    if dty != None:
        for i in dty:
            if type(dty[i])==list:
                for j in dty[i]:
                    if i == 'int':
                        diction[j] = alch.types.INTEGER()

                    elif i == 'float':
                        diction[j] = alch.types.FLOAT()

                    elif i == 'date':
                        diction[j] = alch.types.DATE()

                    else:
                        diction[j] = alch.types.VARCHAR(200)
            else:
                if dty[i] == 'int':
                    diction[i] = alch.types.INTEGER()

                elif dty[i] == 'float':
                    diction[i] = alch.types.FLOAT()
                elif dty[i] == 'date':
                    diction[i] = alch.types.DATE()

                else:
                    diction[i] = alch.types.VARCHAR(200)

        print(diction.keys())
        check = set(df.columns) - set(diction.keys())
        for i in check:
            diction[i] = alch.types.VARCHAR(200)
        print(diction)
    else:
        check = set(df.columns)
        for i in check:
            diction[i] = alch.types.VARCHAR(200)
        print(diction)
    return diction



def write(df,schema,tblname,dty=None,drop=None):
    #os.add_dll_directory("C:\\Users\\KarthickK\\Downloads\\v11.5.7_ntx64_odbc_cli\\clidriver\\bin")
    #db2 = alch.create_engine('ibm_db_sa://kartkam:yGuI7wqOAT4wp1Gc@g42-a-cmdpprod1.az2.ash.cpc.ibm.com:51000/cmdp;SECURITY=SSL;SSLCLIENTKEYSTOREDB=C:\\db2_ssl\\IBM_DB2_SSL_Client.kdb;SSLCLIENTKEYSTASH=C:\\db2_ssl\\IBM_DB2_SSL_Client.sth;SSLServerCertificate=C:\\Users\\KarthickK\\Downloads\\IBM_CLOUD.crt')
    db2 = alch.create_engine('db2+ibm_db://kartkam:yGuI7wqOAT4wp1Gc@g42-a-cmdpprod1.az2.ash.cpc.ibm.com:'+str(51000) + '/cmdp;SECURITY=ssl;SSLServerCertificate=C:\\Users\\KarthickK\\Downloads\\IBM_ROOT.cer')
    con = db2.connect()
    if drop != None:
        df = df.drop(drop,axis=1)
    dty = data_def(df, dty)
    df.columns = [i.replace(" ", "_") for i in df.columns]
    df['load_ts'] = dt.now()
    df.to_sql(tblname, con=con, schema=schema,index = True,chunksize=100,dtype=dty,if_exists="replace")


# report_type = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\Report Types.xlsx")
# write(report_type,"ACS_RGWW1","AR_REPORT_TYPES",{'index':alch.types.INTEGER(),"Report_Type":alch.types.VARCHAR(100),"Comparative_report?":alch.types.INTEGER()})

# report_type = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\rankingv1.xlsx")
# write(report_type,"ACS_RGWW1","AR_RANKING",{'index':alch.types.INTEGER(),"Ranking":alch.types.VARCHAR(100),"New_Ranking":alch.types.VARCHAR(100)})

#report_type = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\field Definitions.xlsx")
#print(report_type.columns)
# defin = {'index':alch.types.INTEGER(),
#          "Original_Title":alch.types.VARCHAR(200),
#          "End_Date": alch.types.DATE(),
#          "Ranking":alch.types.VARCHAR(100),
#          "Business_Unit":alch.types.VARCHAR(100),
#          "AR_Pro":alch.types.VARCHAR(70),
#          "Report_Type":alch.types.VARCHAR(200),
#          "Project_Type":alch.types.VARCHAR(200),
#          "Publishing year":alch.types.INTEGER(),
#          "Interaction_Count":alch.types.INTEGER(),
#          "plot x":alch.types.FLOAT(),
#          "Plot y":alch.types.FLOAT(),
#          'Leader?':alch.types.INTEGER(),
#          "Title on Chart":alch.types.VARCHAR(200),
#          "Firm":alch.types.VARCHAR(200),
#          "Architect Project ID":alch.types.VARCHAR(50),
#          "non_eval_reports":alch.types.INTEGER(),
#          "YTD?":alch.types.VARCHAR(50),
#          "data_source":alch.types.VARCHAR(100),
#          "n-1 x":alch.types.FLOAT(),
#          "n-1 y":alch.types.FLOAT(),
#          "Movement":alch.types.VARCHAR(100)}

# defin = {'int':['index','Leader?','non_eval_reports'],
#          'float':['plot x','Plot y','n-1 x','n-1 y','Interaction Count'],
#          'date':['End Date']}
#
#
# write(report_type,"ACS_RGWW1","AR_ARCHI_VOT".lower(),dty=defin)

# report_type = pd.read_excel("C:\\Users\\KarthickK\\Downloads\\publication_calendar-1.xlsx",sheet_name="Markets as of February-1-2022")
# report_type['evaluative_report'] = list(map(lambda i: 1 if i == 'Magic Quadrant' else 0,report_type['Document Type']))
# report_type['retired'] = list(map(lambda i: 1 if i == 'This Market is retired.' else 0,report_type['Updates From Previous Month']))
# report_type['new_evaluative_report'] = list(map(lambda i,j: 1 if np.isnan(i) and j== 1 else 0,report_type['Gartner.com Doc Code'],report_type['evaluative_report']))
# defin = {'int': ['Gartner.com Doc Code'],
#          'date':['Expected Research Kick-off Month',
#                  'Next Expected Publish Month',
#                  'Last Published Date']}
#
# write(report_type,"ACS_RGWW1","AR_GARTNER_UPCOMING_PUBLICATIONS",dty=defin,drop=['Project Contact','Lead Analyst','Co-authors '])





# defin = {'int':['index','Leader?','non_eval_reports'],
#          'float':['plot x','Plot y','n-1 x','n-1 y','Interaction Count'],
#          'date':['End Date']}

#dty = {'Field Name':'str','Definitions':'str'}

#write(report_type,"ACS_RGWW1","AR_VOT_DEF".lower(),dty=dty)


