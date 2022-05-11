import pandas as pd
import numpy as np

# firms_abb = pd.read_excel("C:\\Users\\KarthickK\\Box\\Dashboard Datasets\\standard\\firm.xlsx").to_dict()
# print(firms_abb)

# texts = "Access Management"
# #texts = "".join([i[0].lower() for i in texts.split(" ")])
# y = "".join([j[0].lower() for j in texts.lower().split(" ")])
# print(y)

x = pd.DataFrame([[1,2]],columns=["a","b"])
y = pd.DataFrame([[3,4]])

print(pd.concat([x,y]))

