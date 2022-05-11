import tensorflow as tf
import pyodbc as odbc
import pandas as pd
# sqls = """
# select ast.*, cont.*
# from ADR.BLD_MCP_REFUNIVCONT_WKLY_ACTIVE ast
# left join ADR.BLD_RRT_REFCONTENTTYPE_ACTIVE cont on Ast.conttypid = cont.conttypid
# where cont.dimlevel1shortname = 'Research report'
# and EFFECTIVEDATE >= '2019-01-01'
# """
#
# conn = odbc.connect(("DSN=SMARSRAWODBC;UID=karthico;PWD=pw3v8dn9fry35peqrn"))
# df = pd.io.sql.read_sql(sqls,con=conn)
# # df = pd.io.sql.read_sql(sqls, con=("DSN=SMARSRAWODBC"))
# df.to_excel("c:\\garbage\\adr_assets.xlsx")

from googletrans import Translator

tr = Translator()
print(tr.detect("안녕하세요."))