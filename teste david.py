from pyspark.sql import SparkSession
spark = SparkSession.builder \
         .appName('SparkByExamples.com') \
         .getOrCreate()

data = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  ]
columns = ["Name","Dept","Salary"]
df = spark.createDataFrame(data=data,schema=columns)
df.show()



#/

import pandas as pd

#IMPORT SOURCE FILE
df = pd.read_csv('train.csv',encoding='UTF-8', sep = ',') # SOURCE
df.to_csv (r'C:\Python\File11.csv', index = None, header=True) 
print(df)
#/

# Biblioteca para LOG 