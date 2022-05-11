
import pandas as pd
import pyodbc as odbc
import sqlite3 as sql



class querying():
    current_qtr = '3Q'
    next_qtr = '4Q'
    current_yr = 2017


    def __init__(self, warehouse, file_loc = None,sqls=None):
        if file_loc != None:
            file = open(file_loc, "r")
            self.file = file
        print("File Identified")
        print("Warehouse Loaded")

        self.warehouse = warehouse
        self.sqls = sqls
    def process_it(self):
        self.import_sql().connect_warehouse().pull_data()
        return self.data

    def import_sql(self):
        if self.sqls != None:
            self.sqls = self.sqls
        else:
            business_sql = self.file.read()
            self.sqls = business_sql
        print("SQL added")
        return self

    def get_data(self):
        return self.data

    def connect_warehouse(self):
        self.connection = odbc.connect(("DSN=" + self.warehouse + ";UID=kartkam;PWD=yGuI7wqOAT4wp1Gc"))
        print("Connection established")
        return self

    def pull_data(self):
        print("pullling data")
        data = pd.io.sql.read_sql(self.sqls,con=self.connection)
        self.data = data
        return self

    def write_excel(self):
        self.data.to_excel("c:\\pytsk\\imt.xlsx")
        return self



    def summarise(self,dimension):
        x = "[" + dimension + "]"
        summarise_dim = pd.groupby(x)['Validated_Pipeline'].sum()
        self.summarise_dim = summarise_dim
        return self







if __name__ == "__main__":
    #x = querying("c:\\pytsk\\valpipe.txt","ESAPDA")
    x = querying("CMDP",sqls="select count(*) from ACS_RGWW1.AR_VOT_TIER1")
    print(x.import_sql().connect_warehouse().pull_data().data)





