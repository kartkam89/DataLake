import pandas as pd
import querying as q


def pull_data():
    sqls = """
                Select 
                A.OPP_NO, 
                A.DETAIL_KEY,
                M.DIMLEVEL2TIMENAME AS DATA_AS_OF,
                ZZ.DIMLEVEL1SHORTNAME as sales_stage,
                Year(A.OPTYDTLCREATEDT) as Create_Yr, 
                Quarter(A.OPTYDTLCREATEDT) as Create_Qtr,
                Year(A.OPTYDTLCREATEDT)||'Q'||Quarter(A.OPTYDTLCREATEDT) as Create_Yr_Qtr,
                
                x.DIMRECGRP1 as "UT Business Unit",
                x.DIMLEVEL2LONGNAME as "UT Mktg SubBrand",
                x.DIMLEVEL3LONGNAME  as "UT L15", 
                x.DIMLEVEL4LONGNAME as "UT L17",
                x.DIMLEVEL5LONGNAME as "UT L20", 
                x.DIMLEVEL6LONGNAME as "UT L30",
                
                H.dimlevel1shortname as Mktg_comm_channel_name, 
                H.dimlevel2shortname as Mktg_comm_channel_type, 
                
                I.dimlevel1shortname as Geo_Level1, 
                I.dimlevel2shortname as Geo_Level2, 
                
                L.ORIG_MKTG_TEAM_NAME as Originating_Marketing_Team,
                L.lvl3cd as Parent_Project_Code, 
                
                --astt.dimlevel2code as mktg_ASSET_CODE,
                --ASTT.dimlevel2longname as mktg_ASSET_NAME,
                AST.ASSET_NAME, 
                AST.ASSET_CODE as ASSET_CODE,
                AST.ASSET_URL,
                V.DIMLEVEL1LONGNAME as Origin,
                AST.PARENT_ASSET_TYPE,
                AST.PARENT_ASSET_CREATEDATE,
                AST.PARENT_ASSET_REGISTRATION_REQUIREMENT as GATED_FLG,
                AST.PARENT_ASSET_BUYERS_JOURNEY,
                OFT.DIMLEVEL1SHORTNAME AS Asset_Type_Group,
                OFT.DIMLEVEL2SHORTNAME AS Asset_Type,
                BJ.DIMLEVEL1SHORTNAME AS BUYING_CYCLE,
                
                
                
                sum(case when M.DIMLEVEL2TIMENAME= 'Current Week' then A.cy_srcd_pipe_cnt end) as CY_sourced_VLC, 
                sum(case when M.DIMLEVEL2TIMENAME= 'Current Week' then A.cy_srcd_pipe_rev_amt/1000000 end) as CY_SRCD_VLRC, 
                sum(case when M.DIMLEVEL2TIMENAME= 'Current Week' then A.CAP_CY_srcd_pipe_REV_AMT/1000000 end) as CY_CAP_SRCD_VLRC, 
                
                sum(case when M.DIMLEVEL2TIMENAME= 'Current Week' then A.CY_srcd_win_cnt end) as CY_SRCD_WC, 
                sum(case when M.DIMLEVEL2TIMENAME= 'Current Week' then A.CY_srcd_win_REV_AMT/1000000 end) as CY_SRCD_WR, 
                sum(case when M.DIMLEVEL2TIMENAME= 'Current Week' then A.cap_CY_srcd_win_REV_AMT/1000000 end) as CY_cap_SRCD_WR,
                
                sum(case when M.DIMLEVEL2TIMENAME= 'Prior Year' then A.cy_srcd_pipe_cnt end) as PY_sourced_VLC, 
                sum(case when M.DIMLEVEL2TIMENAME= 'Prior Year' then A.cy_srcd_pipe_rev_amt/1000000 end) as PY_SRCD_VLRC, 
                sum(case when M.DIMLEVEL2TIMENAME= 'Prior Year' then A.CAP_CY_srcd_pipe_REV_AMT/1000000 end) as PY_CAP_SRCD_VLRC, 
                
                sum(case when M.DIMLEVEL2TIMENAME= 'Prior Year' then A.CY_srcd_win_cnt end) as PY_SRCD_WC, 
                sum(case when M.DIMLEVEL2TIMENAME= 'Prior Year' then A.CY_srcd_win_REV_AMT/1000000 end) as PY_SRCD_WR, 
                sum(case when M.DIMLEVEL2TIMENAME= 'Prior Year' then A.cap_CY_srcd_win_REV_AMT/1000000 end) as PY_cap_SRCD_WR,
                
                sum(A.cy_srcd_pipe_cnt) as Sourced_VLC,
                sum(A.cy_srcd_pipe_rev_amt/1000000) as SRCD_VLRC,
                sum(A.CAP_CY_srcd_pipe_REV_AMT/1000000) as CAP_SRCD_VLRC,
                sum(A.CY_srcd_win_cnt) as SRCD_WC,
                sum(A.CY_srcd_win_REV_AMT/1000000) as SRCD_WR,
                sum(A.cap_CY_srcd_win_REV_AMT/1000000) as CAP_SRCD_WR
                
                from ADR.BLD_MKO_RESP_OPTY_METRICS_ACTIVE A
                --LEFT OUTER JOIN ADR.SRC_RRT_REFSSM_ACTIVE C ON (C.SSMID=A.SSMID)
                --LEFT OUTER JOIN ADR.SRC_RRT_REFOPPTYSOURCE_ACTIVE D ON (A.OPPTYSOURCEID = D.OPPTYSOURCEID)
                --LEFT OUTER JOIN ADR.SRC_RRT_REFOWNINGORG_ACTIVE E on (A.OWNORGID = E.OWNORGID)
                --LEFT OUTER JOIN ADR.SRC_RRT_REFINDUST_ACTIVE F on (A.INDUSTID = F.INDUSTID_CUR)
                --LEFT OUTER JOIN ADR.SRC_RRT_REFPLANELEMENT_ACTIVE G on (A.PLANELEMID = G.PLANELEMID_CUR)
                LEFT OUTER JOIN ADR.SRC_RRT_REFMKTGCOMMCHN_ACTIVE H on (A.MKTGCOMMCHNID = H.MKTGCOMMCHNID) 
                LEFT OUTER JOIN ADR.SRC_RRT_REFMKTGGEO_ACTIVE I ON (A.MKTGGEOID=I.MKTGGEOID)
                LEFT OUTER JOIN ADR.SRC_RRT_REFDEALSZ_ACTIVE J on (A.DEALSZID = J.DEALSZID)
                --LEFT OUTER JOIN ADR.SRC_RRT_REFOPTYDTLAGE_ACTIVE K on (A.OPTYDTLAGEID = K.OPTYDTLAGEID)
                LEFT OUTER JOIN adr.bld_tsi_mktgplnhier_active L on (A.MKTGPLNHIERID_RPTG = L.mktgplnhierid)
                ----LEFT OUTER JOIN ADR.SRC_RRT_REFCLIENTTIER_ACTIVE M on (A.clienttierid = M.clienttierid_cur)
                --LEFT OUTER JOIN (select distinct mktggeoid_cur, dimlevel1shortname, dimlevel2shortname 
                --                 from ADR.SRC_RRT_REFMKTGGEO_ACTIVE 
                --                 where DIMLEVEL1SHORTNAME <> 'Growth Markets Unit') N ON (A.MKTGPLCMTGEOID=N.MKTGGEOID_CUR)
                --LEFT OUTER JOIN (select distinct mktggeoid_cur, dimlevel1shortname, dimlevel2shortname 
                --                 from ADR.SRC_RRT_REFMKTGGEO_ACTIVE 
                --                 where DIMLEVEL1SHORTNAME <> 'Growth Markets Unit') O ON (A.MKTGFUNDGEOID=O.MKTGGEOID_CUR)
                --
                --LEFT OUTER JOIN ADR.SRC_RRT_REFRESPSCORE_ACTIVE Q on (A.respscoreid = Q.RESPSCOREID_cur)
                --LEFT OUTER JOIN ADR.SRC_RRT_REFSSM_ACTIVE R ON A.SSMID = R.SSMID
                --LEFT OUTER JOIN ADR.BLD_SOP_REFCOVSECTOR_ACTIVE S ON A.COVSECTID = S.COVSECTID
                --LEFT OUTER join ADR.SRC_RRT_REFSLSPLY_ACTIVE T on A.SLSPLYID = T.SLSPLYID
                LEFT OUTER join (select distinct DIMLEVEL2TIMEID,DIMLEVEL2TIMENAME from ADR.BLD_MKI_SRRPTGMKTGACTDATE_ACTIVE) M on A.LOADTIMEID = M.DIMLEVEL2TIMEID
                LEFT OUTER join ADR.DIMTIME_ACTIVE U on A.MEASTIMEID= U.TIMEID
                LEFT OUTER join ADR.SRC_RRT_REFPERSENTRYPT_ACTIVE V on A.PERSENTRYPTID = V.PERSENTRYPTID
                LEFT OUTER JOIN ADR.BLD_RRT_REFUTBRAND_ACTIVE x ON A.UTBRANDID = x.UTBRANDID
                --LEFT OUTER JOIN  adr.BLD_RRT_REFTOPACCTOPTY_ACTIVE TR ON (A.TOPACCTID = TR.TOPACCTID)
                
                left outer join adr.bld_x1p_asset_hier_active ASTT on (A.asset_id = ASTT.mktgassetid)
                LEFT OUTER JOIN ADR.SRC_X1I_MCT_ASSETS_ACTIVE AST ON (astt.dimlevel2code = AST.ASSET_code)
                LEFT OUTER JOIN ADR.SRC_RRT_REFOFFERTYPEGRPMAP_ACTIVE OFT ON (A.OFRTYPGRPMAPID = OFT.OFRTYPGRPMAPID)
                LEFT OUTER JOIN ADR.SRC_RRT_REFBUYERJOURNEY_ACTIVE BJ ON (A.BUYJOURNID = BJ.BUYERJOURNID)
                LEFT OUTER JOIN ADR.SRC_RRT_REFSSM_ACTIVE ZZ ON (A.SSMID = ZZ.SSMID)
                
                where 
                M.DIMLEVEL2TIMENAME in ('Current Week','Prior Year'/*,'Final Rptg Prior Year','Final Rptg Prior Year - 1'*/)
                
                and A.INFL_CATG_DISP IN('RLM MCR','BP Source','Assist to Create')
                and A.pipetypcd <> 'N'
                ---and x.dimlevel3shortname = 'Mainframe HW'
                ---and i.dimlevel1shortname = 'EMEA'
                and x.DIMLEVEL2LONGNAME NOT IN ('IBM Global Financing','No Source UT Value','Project 2 UTL10')
                and x.dimrecgrp1 is not null
                and (A.cy_srcd_pipe_cnt <> 0 or A.cap_CY_srcd_win_REV_AMT <> 0 or A.cap_cy_srcd_pipe_rev_amt <> 0 or CY_SRCD_WIN_CNT <> 0)
                and PARENT_ASSET_TYPE ='Analyst Research'
                
                group by 
                A.OPP_NO, 
                A.DETAIL_KEY,
                
                M.DIMLEVEL2TIMENAME,
                Year(A.OPTYDTLCREATEDT),
                Quarter(A.OPTYDTLCREATEDT),
                Year(A.OPTYDTLCREATEDT)||'Q'||Quarter(A.OPTYDTLCREATEDT),
                x.DIMRECGRP1,L.lvl3cd,
                x.DIMLEVEL2LONGNAME ,
                x.DIMLEVEL3LONGNAME ,
                x.DIMLEVEL4LONGNAME ,
                x.DIMLEVEL5LONGNAME ,
                x.DIMLEVEL6LONGNAME ,
                H.dimlevel1shortname ,
                H.dimlevel2shortname ,
                I.dimlevel1shortname ,
                I.dimlevel2shortname ,
                L.ORIG_MKTG_TEAM_NAME ,
                L.lvl3cd ,
                AST.ASSET_URL,
                --astt.dimlevel2code,
                --ASTT.dimlevel2longname,
                AST.ASSET_NAME, 
                AST.ASSET_CODE,
                V.DIMLEVEL1LONGNAME ,
                AST.PARENT_ASSET_TYPE,
                AST.PARENT_ASSET_CREATEDATE,
                AST.PARENT_ASSET_REGISTRATION_REQUIREMENT,
                AST.PARENT_ASSET_BUYERS_JOURNEY,
                OFT.DIMLEVEL1SHORTNAME,
                OFT.DIMLEVEL2SHORTNAME ,
                BJ.DIMLEVEL1SHORTNAME,
                ZZ.DIMLEVEL1SHORTNAME
            """
    df = q.querying("SMARSRAWODBC",sqls=sqls).import_sql().connect_warehouse().pull_data().data
    df.to_excel("c:\\garbage\\adr_data.xlsx")
    print(df.columns)
    return df