import pandas as pd
import sqlalchemy as alch
from datetime import datetime as dt



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
    dsn_hostname = 'g42-a-cmdpprod1.az2.ash.cpc.ibm.com'  # hostname
    dsn_uid = 'kartkam'  # user ID
    dsn_pwd = 'yGuI7wqOAT4wp1Gc'  # password
    dsn_database = 'CMDP'  # name of the database
    dsn_port = '51000'  #
    ssl_trust_store_location = 'C:\\Users\\KarthickK\\Downloads\\IBM_ROOT.cer'
    # ssl_trust_store_password='mightyStrongPa5!w0Rd'
    cmdp_credentials = {"host": dsn_hostname, "port": dsn_port, "db": dsn_database, "user": dsn_uid, "pw": dsn_pwd}


    cmdp_e = alch.create_engine(
        "db2+ibm_db://{0}:{1}@{2}:{3}/{4};SECURITY=ssl;SSLServerCertificate={5}".format(cmdp_credentials['user'],
                                                                                        cmdp_credentials['pw'],
                                                                                        cmdp_credentials['host'],
                                                                                        str(cmdp_credentials['port']),
                                                                                        cmdp_credentials['db'],
                                                                                        ssl_trust_store_location))

    con = cmdp_e.connect()
    if drop != None:
        df = df.drop(drop,axis=1)
    dty = data_def(df, dty)
    df.columns = [i.replace(" ", "_") for i in df.columns]
    df['load_ts'] = dt.now()
    df.to_sql(tblname, con=con, schema=schema,index = True,chunksize=10000,if_exists="append")#dtype=dty,


df = pd.read_excel("c:\\garbage\\vlrc.xlsx")
write(df,"ACS_RGWW1","AR_VLRC_CYPY_SUMMARY_TEST".lower())

