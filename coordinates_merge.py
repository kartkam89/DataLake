import pandas as pd
import numpy as np

def merge_it(ar_df, ar_key, coord_df, coord_key):
    merge_file = ar_df.set_index(ar_key).join(coord_df.set_index(coord_key),lsuffix="_arch",rsuffix="_coord",)
    return merge_file

if __name__ == "__main__":
    from datetime import datetime as dt
    print(dt.now())
    temp_df = merge_it(pd.read_excel("C:\\Users\\KarthickK\\Downloads\\std_output_Projects_YTD.xlsx"),"Project Id",
                        pd.read_excel("C:\\Users\\KarthickK\\Downloads\\From Karthick -- mq_dot_plot_dataentry v2.xlsx"),"Project Id")
    temp_df.to_excel("c:\\garbage\\merged_file.xlsx")
    flag_df = pd.read_excel("C:\\Users\\KarthickK\\Downloads\\Hybrid - AI matching file -- Projects_20211214 v13.xlsx")
    #print(flag_df)
    final_df = temp_df.join(pd.read_excel("C:\\Users\\KarthickK\\Downloads\\Hybrid - AI matching file -- Projects_20211214 v13.xlsx").set_index("Project Id"),rsuffix="_flag")
    #final_df['new_records'] = list(map(lambda a,b,x,y,z: 1 if a+x+y+z == 0 and b in ['Gartner','IDC','Forrester Research'] else 0,final_df['Hybrid Cloud related'],final_df['pred_firm'],final_df['AI related'],final_df['Hybrid Cloud & AI related'],final_df['No hybrid or AI']))
    final_df['new_records'] = list(
        map(lambda yr, comp, a, x, y, z: 1 if yr == 2021 and comp in ['Gartner','IDC','Forrester Research'] and a + x + y + z == 0 else 0 ,
            final_df['Publishing year'],final_df['pred_firm'], final_df['Hybrid Cloud related'].fillna(0).astype("int"),  final_df['AI related'].fillna(0).astype("int"),
            final_df['Hybrid Cloud & AI related'].fillna(0).astype("int"), final_df['No hybrid or AI'].fillna(0).astype("int")))
    print("Some projects have gone missing",set(flag_df['Project Id'].unique())-set(final_df.index))
    final_df['coordinates_missing'] = list(map(lambda yr, comp, x,y: 1 if yr == 2021 and comp == 'Gartner' and np.isnan(x)==True and np.isnan(y)==True else 0,final_df['Publishing year'],
                                                                                                           final_df['pred_firm'],final_df['plot x'],final_df['Plot y']))

    final_df.to_excel("c:\\garbage\\merged_file.xlsx")
    final_df[final_df['Recency Indicator']==1].to_excel("c:\\garbage\\for_coordinates.xlsx")
    print(dt.now())

    import ibm_db
    import ibm_db_dbi
    import pandas as pd
    import sqlalchemy as alch


    db2 = alch.create_engine('ibm_db_sa://kartkam:yGuI7wqOAT4wp1Gc@g42-a-cmdpprod1.az2.ash.cpc.ibm.com:51000/cmdp;SECURITY=SSL;SSLCLIENTKEYSTOREDB=C:\db2_ssl\IBM_DB2_SSL_Client.kdb;SSLCLIENTKEYSTASH=C:\db2_ssl\IBM_DB2_SSL_Client.sth')
    con = db2.connect()
    final_df.to_sql("AR_TIER2_RECORDS",con=con,schema="ACS_RGWW1")