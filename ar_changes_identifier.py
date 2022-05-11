import pandas as pd
import numpy as np

class change_identifier:
    def __init__(self, new_data,overwrite=False):
        self.new_data = new_data
        self.focus_fields = ["AR Pro","Status","Start Date_arch","End Date_arch",
                             "Modified Date","Modified By","Ranking","Report Type"]
        self.new_data = self.new_data.set_index("Project Id")[self.focus_fields]
        self.overwrite = overwrite
    def get_prev_data(self):
        self.old_data = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\versions\\X-1.xlsx")
        self.old_data = self.old_data.set_index("Project Id")[self.focus_fields]
        print(self.old_data.columns==self.new_data.columns)

        return self

    def compare(self):
        one_way = set(self.new_data.index) - set(self.old_data.index)
                  #- set(self.old_data.index)
        two_way = set(self.old_data.index) -  set(self.new_data.index)
                  #- set(self.new_data.index)
        print(len(one_way), len(two_way))
        compared_df = self.old_data.compare(self.new_data,keep_shape=False)
        compared_df.to_excel("c:\\garbage\\compared_df.xlsx")
        return self

    def create_copy(self):
        if self.overwrite == True:
            pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\versions\\X-1.xlsx").to_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\versions\\X-2.xlsx")
            self.new_data.to_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\versions\\X-1.xlsx")



if __name__ == "__main__":
    change_identifier(pd.read_excel("c:\\garbage\\merged_file.xlsx"),False).get_prev_data().compare()

