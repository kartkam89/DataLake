import pandas as pd
import numpy as np
import querying as q
import writing_tablev1 as wt
import pyodbc as odbc


class datalake_vlrc:
    sqls = """
                Select
                A.loadtimeid,
                A.INFL_CATG_DISP,
                A.pipetypcd,
                sql.dimlevel1shortname as sql_cat_level1,
                sql.dimlevel2shortname as sql_cat_level2,
                sql.dimrecgrp1 as mktg_sales,
                sql.dimrecgrp2 as mktg_sales_cat,
                ZZ.DIMLEVEL1SHORTNAME as sales_stage,
                Year(A.LEAD_CREATE_DT) as Create_Yr, 
                Quarter(A.LEAD_CREATE_DT) as Create_Qtr,
                Year(A.LEAD_CREATE_DT)||'Q'||Quarter(A.LEAD_CREATE_DT) as Create_Yr_Qtr,
                Year(A.MEAS_DATE)||'Q'||Quarter(A.MEAS_DATE) as Forecast_Yr_Qtr,
                x.DIMRECGRP1 as UT_Business_Unit,
                x.DIMLEVEL2LONGNAME as UT_Mktg_SubBrand,
                x.DIMLEVEL3LONGNAME  as UT_L15, 
                x.DIMLEVEL4LONGNAME as UT_L17,
                x.DIMLEVEL5LONGNAME as UT_L20, 
                x.DIMLEVEL6LONGNAME as UT_L30,
                I.dimlevel1shortname as Geo_Level1, 
                I.dimlevel2shortname as Geo_Level2, 
                AST.UUCID_L1_NM, 
                AST.UUCID_KEY,
                AST.RPTG_PAGE_URL,
                DRV.lvl1shortname as driver_plan,
                DRV.lvl2shortname as driver_sub_plan,
                DRV.lvl3code as driver_global_campaign#,
                DRV.lvl4code as driver_campaign_code#,
                Cont.dimlevel1shortname as asset_type,
                sum(A.cy_sql_cnt) as SQL_cnt, 
                sum(A.cy_sql_rev_amt/1000000) as SQL_VLRC, 
                sum(A.CAP_CY_sql_REV_AMT/1000000) as CAP_SQL_VLRC, 
                sum(A.CY_sql_win_cnt) as SQL_WC, 
                sum(A.CY_sql_win_REV_AMT/1000000) as SQL_WR, 
                sum(A.cap_CY_sql_win_REV_AMT/1000000) as CAP_SQL_WR
                from ADR.BLD_MKO_RESP_OPTY_INFL_METRICS_ACTIVE A
                LEFT OUTER JOIN ADR.SRC_RRT_REFMKTGGEO_ACTIVE I ON (A.MKTGGEOID=I.MKTGGEOID)
                LEFT OUTER JOIN ADR.SRC_RRT_REFDEALSZ_ACTIVE J on (A.DEALSZID = J.DEALSZID)
                LEFT OUTER join ADR.DIMTIME_ACTIVE U on A.MEASTIMEID= U.TIMEID
                LEFT OUTER JOIN ADR.BLD_RRT_REFUTBRAND_ACTIVE x ON A.UTBRANDID = x.UTBRANDID
                left join ADR.BLD_MCP_REFUNIVCONT_WKLY_ACTIVE ast on A.UVUQCTNTID = ast.UVUQCTNTID
                left join ADR.BLD_RRT_REFCONTENTTYPE_ACTIVE cont on Ast.conttypid = cont.conttypid
                LEFT OUTER JOIN ADR.SRC_RRT_REFSSM_ACTIVE ZZ ON (A.SSMID = ZZ.SSMID)
                LEFT OUTER JOIN ADR.BLD_MCP_REFMKTGCAMPHIER_WKLY_ACTIVE DRV on A.CMPPLNCONTDRVID = DRV.campplnhierid
                left join ADR.SRC_RRT_REFSQLCREATE_ACTIVE sql on A.sqlcrtid = sql.sqlcrtid
                where
                A.loadtimeid in ('20220323','20210324')
                and A.pipetypcd <> 'N'
                and x.dimrecgrp1 is not null
                and (A.cy_sql_cnt <> 0 or A.cap_CY_sql_win_REV_AMT <> 0 or A.cap_cy_sql_rev_amt <> 0 or CY_sql_WIN_CNT <> 0)
                
                group by 
                A.loadtimeid,
                Year(A.LEAD_CREATE_DT),
                Quarter(A.LEAD_CREATE_DT),
                Year(A.LEAD_CREATE_DT)||'Q'||Quarter(A.LEAD_CREATE_DT),
                x.DIMRECGRP1,
                x.DIMLEVEL2LONGNAME ,
                x.DIMLEVEL3LONGNAME ,
                x.DIMLEVEL4LONGNAME ,
                x.DIMLEVEL5LONGNAME ,
                x.DIMLEVEL6LONGNAME ,
                I.dimlevel1shortname ,
                I.dimlevel2shortname ,
                AST.RPTG_PAGE_URL,
                AST.UUCID_L1_NM, 
                AST.UUCID_KEY,
                ZZ.DIMLEVEL1SHORTNAME,
                DRV.lvl1shortname ,Cont.dimlevel1shortname,A.INFL_CATG_DISP,
                sql.dimlevel1shortname,
                sql.dimlevel2shortname,
                sql.dimrecgrp1 ,
                sql.dimrecgrp2 ,
                A.pipetypcd,
                DRV.lvl2shortname ,
                DRV.lvl3code ,
                DRV.lvl4code ,
                Year(A.MEAS_DATE)||'Q'||Quarter(A.MEAS_DATE)
                """
    conn = odbc.connect(("DSN=SMARSRAWODBC;UID=karthico;PWD=pw3v8dn9fry35peqrn"))
    #df = pd.io.sql.read_sql(sqls,con=conn)
    #df = pd.io.sql.read_sql(sqls, con=("DSN=SMARSRAWODBC"))
    df = pd.read_excel("c:\\garbage\\revised ar response datalake.xlsx")
    dty = {'int':['LOADTIMEID'],
           'float':['CREATE_YR','CREATE_QTR','SQL_CNT','SQL_VLRC','CAP_SQL_VLRC','SQL_WC','SQL_WR','CAP_SQL_WR']}

    wt.write(df.iloc[0:10],"ACS_RGWW1","AR_VLRC_CYPY_SUMMARY_TEST".lower())
    #, dty = dty


