import pandas as pd
import numpy as np
from pdf_html_downloaderv2 import file_download as file_downloader
import extract_comps as comp_name
import new_rec_extractor as new_rec
import datapull_ar as dataset
from datetime import datetime as dt


class ar_master:
    def __init__(self):
        #self.generate_data()
        #self.new_record_identifier()
        self.download_asset()
        self.extract_comp()
        self.merge_extracted_comp_to_df()
        self.write_op()

    def generate_data(self):
        print("pulling data from ADR")
        self.new_data = dataset.pull_data()
        print("Completed pulling data from ADR")
        return self

    def new_record_identifier(self):
        print("Comparing new records with old records")
        self.old_data = pd.read_excel("c:\\garbage\\old_data.xlsx",sheet_name="Raw Data")[['ASSET_CODE','ASSET_URL','FIRMS','Commisioned']]
        print(self.new_data.columns)

        self.new_data = new_rec.new_rec_extractor(self.old_data,self.new_data).merged_df
        print("Completed Comparing new records with old records")
        return self

    def download_asset(self):
        print("Downloading newly identified assets")
        self.new_data = pd.read_excel("c:\\garbage\\merged_df.xlsx")
        self.all_good_df = self.new_data[self.new_data['FIRMS'].notnull()]
        self.search_df = self.new_data[self.new_data['FIRMS'].isnull()]
        self.file_download_df = self.search_df[['ASSET_CODE','ASSET_URL']].drop_duplicates()
        file_downloader("C:\\Users\\KarthickK\\Box\\VLRC Report\\2021Q3\\", self.file_download_df)
        print("Completed Downloading newly identified assets")
        return self

    def merge_extracted_comp_to_df(self):
        print("merging extracted company names to dataframe")
        self.file_download_df = pd.read_excel("c:\\garbage\\AR\\output.xlsx")
        self.search_df = pd.merge(self.search_df,self.file_download_df,left_on="ASSET_CODE",right_on="asset_name",how="left")
        self.search_df.to_excel("c:\\garbage\\AR\\search_df.xlsx")

    def append_merged_df_all_good_df(self):
        self.all_good_df

    def extract_comp(self):
        print("Extracting company names of newly identified assets")
        comp_name.extract_comps()
        print("Completed Extracting company names of newly identified assets")
        return self
    def write_op(self):
        self.new_data.to_excel("C:\\Users\\KarthickK\\Box\\VLRC Report\\2021Q3\\final_output.xlsx")

if __name__ == "__main__":
    print(dt.now())
    ar_master()
    print(dt.now())