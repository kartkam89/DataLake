

class new_rec_extractor:
    def __init__(self,old_df,new_df):
        print(new_df['ASSET_CODE'].values)
        self.new_df = new_df
        self.old_df = old_df
        self.match_df()


    def match_df(self):
        self.new_df = self.new_df.set_index("ASSET_CODE")
        self.old_df = self.old_df.set_index("ASSET_CODE")
        self.old_df = self.old_df.drop_duplicates()
        self.merged_df = self.new_df.join(self.old_df[['FIRMS','Commisioned']])
        self.merged_df.to_excel("c:\\garbage\\merged_df.xlsx")
        return self


if __name__ == "__main__":
    import pandas as pd
    import numpy as np
    old_df = pd.read_excel("c:\\garbage\\old_data.xlsx",sheet_name="Raw Data")[['ASSET_CODE','ASSET_URL','FIRMS','Commisioned']]
    old_df = old_df.set_index("ASSET_CODE")
    old_df.drop_duplicates().to_excel("c:\\garbage\old_Df.xlsx")
