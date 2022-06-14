#HEMA Chalenge

#LIBRARIES
import pandas as pd

#IMPORT SOURCE FILE
df = pd.read_csv('train.csv',encoding='UTF-8') # SOURCE

#DATA FORMATING
#CamelCase
df.columns = df.columns.str.title() 
df.columns = df.columns.map(lambda x : x.replace("-", "").replace(" ", "")) 
# DF SALES
dfsales = df[['OrderId','OrderDate','ShipDate','ShipMode','City']]
dfsales['OrderDate']= pd.to_datetime(dfsales['OrderDate'])
dfsales['ShipDate']= pd.to_datetime(dfsales['ShipDate'])
dfsales['OrderDate']= dfsales['OrderDate'].dt.strftime('%Y/%m/%d')
dfsales['ShipDate']= dfsales['ShipDate'].dt.strftime('%Y/%m/%d')
#SET INGESTION DATE
today = pd.to_datetime("today")
dfsales['IngestionDate'] = pd.Timestamp.today().strftime('%Y-%m-%d')
#DF CUSTOMER





#dfcustomer = df['CustomerName'].str.extract(r'(?P<FirstName>\w+) (?P<LastName>\w+)$', expand=True)

#dfcustomerfirstname = df['CustomerName'].str.extract(r'(?P<FirstName>\w+)', expand=True)
#dfcustomerlastname = df['CustomerName'].str.extract(r' (?P<LastName>\w+)$', expand=True)

dfcustomerfirstnamelastname = df['CustomerName'].str.split(n=1,expand=True).rename(columns={0:'First Name' , 1:'Last Name'})

dfcustomer = df[['CustomerId','CustomerName','ShipMode','Segment']]
#dfcustomer['FirstName'] = dfcustomerfirstname
#dfcustomer['LastName'] = dfcustomerlastname

dfcustomer=pd.concat([dfcustomer,dfcustomerfirstnamelastname],axis=1)
dfcustomer = dfcustomer[['CustomerId','First Name','Last Name','CustomerName','ShipMode','Segment']]
#print(df)
#print(dfsales)
print(dfcustomer)

#dfcustomer = df[['CustomerId','CustomerName','ShipMode','Segment',dfcustomerfirstnamelastname]]