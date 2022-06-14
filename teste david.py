import pandas as pd

#IMPORT SOURCE FILE
df = pd.read_csv('train.csv',encoding='UTF-8', sep = ',') # SOURCE
df.to_csv (r'C:\Python\File11.csv', index = None, header=True) 
print(df)