#HEMA Chalenge
#LIBRARIES
from fileinput import filename
import pandas as pd
import datetime
import os 
#IMPORT SOURCE FILE
df = pd.read_csv('C:/Python/HEMA/FILE/train.csv',sep=';') # SOURCE
path = r'C:/Python/HEMA/FILE/'
file_name = os.listdir(path)
my_var = ''.join(str(x) for x in file_name)

#DATE PARAMETERS
dtparameter = datetime.datetime(2018, 9, 9)
last5days = datetime.timedelta(days=5)
last5days = (dtparameter - last5days)

last15days = datetime.timedelta(days=15)
last15days = (dtparameter - last15days)

last30days = datetime.timedelta(days=30)
last30days = (dtparameter - last30days)
#DATA FORMATING
#CamelCase
df.columns = df.columns.str.title() 
df.columns = df.columns.map(lambda x : x.replace("-", "").replace(" ", "")) 
# DF SALES
#dfsales = df[['OrderId','OrderDate','ShipDate','ShipMode','City','CustomerId']]
dfsales = df[['OrderId','OrderDate','ShipDate','ShipMode','City','CustomerId']]
#dfsales = pd.DataFrame()
#dfsales.reset_index(inplace=True)
#dfsales = dfsales.assign(fileName = file_name)
#dfsales['OrderDate']= pd.to_datetime(dfsales['OrderDate'],yearfirst = True , format='%Y/%m/%d')
dfsales['OrderDate']= pd.to_datetime(dfsales['OrderDate'])
dfsales['OrderDateCalc']= dfsales['OrderDate']
dfsales['ShipDate']= pd.to_datetime(dfsales['ShipDate'])

#dfsales['OrderDate']= dfsales['OrderDate'].dt.strftime('%Y/%m/%d')

dfsales['ShipDate']= dfsales['ShipDate'].dt.strftime('%Y/%m/%d')
#SET INGESTION DATE
today = pd.to_datetime("today")
dfsales['IngestionDate'] = pd.Timestamp.today().strftime('%Y-%m-%d')
dfsales['loadingTime'] = pd.Timestamp.today().strftime('%I:%M:%S')
dfsales = dfsales.assign(filename=my_var)

#DF CUSTOMER
dfcustomerfirstnamelastname = df['CustomerName'].str.split(n=1,expand=True).rename(columns={0:'FirstName' , 1:'LastName'})
#totalorders = df['count'].nunique()
dfcustomer = df[['OrderId','CustomerName','CustomerId','ShipMode','Segment','City','Country']]
dfcustomer=pd.concat([dfcustomer,dfcustomerfirstnamelastname],axis=1)
dfcustomer['ingestionDate'] = pd.Timestamp.today().strftime('%Y-%m-%d')
dfcustomer['loadingTime'] = pd.Timestamp.today().strftime('%I:%M:%S')

dfgroup = dfcustomer.groupby('CustomerId')['OrderId'].nunique()
dfgroup= dfgroup.to_frame()
dfgroup.reset_index(inplace=True)
dfgroup = dfgroup.rename(columns={'OrderId':'totalQuantityOfOrders'})

#dfgrouplast5days
dfgrouplast5days = dfsales[(dfsales['OrderDateCalc']>= last5days) & (dfsales['OrderDateCalc']<= dtparameter)]
dfgrouplast5days = dfgrouplast5days.groupby('CustomerId')['OrderId'].nunique() 
dfgrouplast5days= dfgrouplast5days.to_frame()
dfgrouplast5days.reset_index(inplace=True)
dfgrouplast5days = dfgrouplast5days.rename(columns={'OrderId':'quantityOfOrderslast5days'})

#dfgrouplast15days
dfgrouplast15days = dfsales[(dfsales['OrderDateCalc']>= last15days) & (dfsales['OrderDateCalc']<= dtparameter)]
dfgrouplast15days = dfgrouplast15days.groupby('CustomerId')['OrderId'].nunique() 
dfgrouplast15days= dfgrouplast15days.to_frame()
dfgrouplast15days.reset_index(inplace=True)
dfgrouplast15days = dfgrouplast15days.rename(columns={'OrderId':'quantityOfOrderslast15days'})

#dfgrouplast30days
dfgrouplast30days = dfsales[(dfsales['OrderDateCalc']>= last30days) & (dfsales['OrderDateCalc']<= dtparameter)]
dfgrouplast30days = dfgrouplast30days.groupby('CustomerId')['OrderId'].nunique() 
dfgrouplast30days= dfgrouplast30days.to_frame()
dfgrouplast30days.reset_index(inplace=True)
dfgrouplast30days = dfgrouplast30days.rename(columns={'OrderId':'quantityOfOrderslast30days'})

dfcustomer = dfcustomer[['CustomerId','CustomerName','FirstName','LastName','ShipMode','Segment','Country','City','ingestionDate','loadingTime']]

dfjoin = pd.DataFrame()
dfjoin = dfcustomer.join(dfgroup.set_index('CustomerId'), on='CustomerId')
dfjoin.reset_index(inplace=True)
dfjoin = dfjoin.join(dfgrouplast5days.set_index('CustomerId'), on='CustomerId')
dfjoin = dfjoin.join(dfgrouplast15days.set_index('CustomerId'), on='CustomerId')
dfjoin = dfjoin.join(dfgrouplast30days.set_index('CustomerId'), on='CustomerId')

dfcustomer = dfjoin[['CustomerId','CustomerName','FirstName','LastName','ShipMode','Segment','Country','City','quantityOfOrderslast5days','quantityOfOrderslast15days','quantityOfOrderslast30days','ingestionDate','loadingTime']].assign(filename=my_var)
dfsales.drop("OrderDateCalc", axis=1, inplace=True)
#dfcustomer=pd.concat([dfcustomer,dfgroup],axis=0)
#print(df)
#print(dfgroup)
#print(type(file_name))
#print(my_var)

#dfjoin.info()
#dfgrouplast5days.info()
print(dfcustomer)
#print(file_name)
print(dfsales)

#print(dtparameter)
#print(dfgrouplast5days)
#print(totalQuantityOfOrders)


dfsales.to_parquet('C:\Python\HEMA\CONSUMPTION/SALES.parquet', engine='pyarrow')
dfcustomer.to_parquet('C:\Python\HEMA\CONSUMPTION/CUSTOMER.parquet', engine='pyarrow')
#dfsales.to_csv(path_or_buf='C:\Python\HEMA\CONSUMPTION\SALES.csv', sep=';', na_rep='', float_format=None, columns=None, header=True, index=False, index_label=None, mode='w', encoding='UTF-8', compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal='.', errors='strict', storage_options=None)
#dfcustomer.to_csv(path_or_buf='C:\Python\HEMA\CONSUMPTION\CUSTOMER.csv', sep=';', na_rep='', float_format=None, columns=None, header=True, index=False, index_label=None, mode='w', encoding='UTF-8', compression='infer', quoting=None, quotechar='"', line_terminator=None, chunksize=None, date_format=None, doublequote=True, escapechar=None, decimal='.', errors='strict', storage_options=None)