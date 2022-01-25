from xmlrpc.client import boolean
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import (AutoMinorLocator, MultipleLocator)
from matplotlib.dates import DateFormatter

#Importing database and creating base dataframe
df = pd.read_csv("transactional-sample.csv")
print(df.dtypes)

#Creating xlsx file to save dataframes (if needed)
data_analysis = pd.ExcelWriter('data_analysis.xlsx', engine='xlsxwriter')

#Gettin the index columns of the base dataframe
columns = list(df.columns)
#['transaction_id', 'merchant_id', 'user_id', 'card_number', 'transaction_date', 'transaction_amount', 'device_id', 'has_cbk']

#Creating new index and columns type for the new dataframe
columns_mod = ['transaction_id', 'merchant_id', 'user_id', 'card_number', 'transaction_date', 'transaction_time', 'transaction_amount', 'device_id', 'has_cbk']
dtypes = {columns_mod[0]:np.int64,columns_mod[1]:np.int64,columns_mod[2]:np.int64,columns_mod[3]:np.object_,columns_mod[4]:np.datetime64,
        columns_mod[5]:np.object_,columns_mod[6]:np.float_,columns_mod[7]:np.object_,columns_mod[8]:np.bool_}

#Creating new dataframe with date and time splitted
df_mod = df.copy()
df_mod['transaction_time'] = pd.to_datetime(df['transaction_date']).dt.time
df_mod['transaction_date'] = pd.to_datetime(df['transaction_date']).dt.date
 

df_mod = df_mod.reindex(columns=columns_mod)
df_mod['transaction_amount'] = df_mod['transaction_amount'].astype(float)
df_mod['device_id'] = df_mod['device_id'].astype(float)
df_mod = df_mod.astype(dtypes)

print(df_mod.dtypes)

#Dataframe to discover users that made above 3 transactions
df_gp_mer_tot = df_mod.drop(columns=['transaction_id', 'merchant_id', 'card_number', 'transaction_date', 'transaction_time', 'device_id', 'has_cbk'],inplace=False)
df_gp_mer_tot = df_mod.groupby('user_id',as_index=False).count()
df_gp_mer_tot_f = df_gp_mer_tot[df_gp_mer_tot['transaction_amount'] > 3]
df_gp_mer_tot_f = df_gp_mer_tot_f.sort_values(by=['transaction_amount'], ascending=False)

#Graph showing users that made more than 3 transactions
fig, graph1 = plt.subplots()
df_gp_mer_tot_f.plot(x='user_id',y='transaction_amount',color=(52/235, 180/235, 235/235),kind="bar",ax=graph1)
plt.xlabel("User_ID")
plt.ylabel("Transaction registered")
graph1.yaxis.grid('on')
graph1.minorticks_on()
graph1.yaxis.grid(which='major',color='black', linestyle='-')
graph1.yaxis.grid(which='minor',color='gray', linestyle=':')
graph1.set_zorder(10)
graph1.set_ylim(0, 35)
graph1.get_legend().remove()


#Dataframe grupby date and giving total amount and number of transactions
df_date_total = df_mod.drop(columns=['transaction_id', 'merchant_id','user_id', 'card_number', 'transaction_time', 'device_id', 'has_cbk'],inplace=False)
df_date_total = df_date_total.sort_values(by=['transaction_date'], ascending=True, inplace=False).groupby('transaction_date',as_index=False).agg(total_amout=('transaction_amount','sum'),number_transaction=('transaction_amount','count'))
df_date_total['transaction_date'] = pd.to_datetime(df_date_total['transaction_date'].dt.strftime("%Y-%m-%d"))

#Graph total amount and number os transactions of the original data
fig = plt.figure()
graph2 = fig.add_subplot(111)
graph3 = graph2.twinx()

width = 0.4
df_date_total.plot(x='transaction_date',y='total_amout',kind='bar',color='red', ax=graph2, width=width, position=0)
df_date_total.plot(x='transaction_date',y='number_transaction',kind='bar',color='blue', ax=graph3, width=width, position=1)

graph2.set_xlabel("Date")
graph2.set_ylabel('Amount')
graph2.yaxis.set_major_formatter('R${x:1.2f}')
graph2.legend(['Total amount'],loc='upper left')
graph2.set_ylim(0,305000)

graph3.set_ylabel('Number of transactions')
graph3.legend(['Total transactions'],loc='upper right')
graph3.set_ylim(0, 450)


#Getting critical users id (creating a list with values)
crit_user = df_gp_mer_tot_f['user_id'].tolist()
print(crit_user)
#Create dataframe with only critical users transaction's data
df_crit_user = df_mod[df_mod['user_id'].isin(crit_user)]
# print(df_crit_user.head(10))

#Getting lists of critical users' transaction_id and card_number
crit_transaction_id = df_crit_user['transaction_id'].tolist()
crit_credit_card = df_crit_user['card_number'].tolist()

#Getting merchant_id and device_id from crit_transaction_id
crit_merchant_id = df_crit_user['merchant_id'].tolist()
crit_device_id = df_crit_user['device_id'].tolist()

#Creating dataframe with unique critical data involved in possible fraud
df_crit_data = pd.DataFrame({'crit_transaction_id':crit_transaction_id,'crit_credit_card':crit_credit_card,'crit_merchant_id':crit_merchant_id,'crit_device_id':crit_device_id})

#Saving dataframe into a xlsx file
df_crit_data.to_excel(data_analysis,sheet_name='critical_data_list',index=False)

#Group data of critical users transaction's dataframe and creating total_amount and transactions_count colunms
df_crit_user.to_excel(data_analysis,sheet_name='critical_user',index=False)
df_crit_user = df_crit_user.sort_values(by=['transaction_date'], ascending=False, inplace=False).groupby(['merchant_id','user_id','device_id','transaction_date','card_number'],as_index=False).agg(total_amout=('transaction_amount','sum'),number_transaction=('transaction_amount','count'))
df_crit_user.to_excel(data_analysis,sheet_name='critical_user_groupby',index=False)

# fig, graph4 = plt.subplots()
# df_gp_mer_tot_f.plot(x='user_id',y='transaction_amount',color=(52/235, 180/235, 235/235),kind="bar",ax=graph1)
# plt.xlabel("User_ID")
# plt.ylabel("Transaction registered")
# graph4.yaxis.grid('on')
# graph4.minorticks_on()
# graph4.yaxis.grid(which='major',color='black', linestyle='-')
# graph4.yaxis.grid(which='minor',color='gray', linestyle=':')
# graph4.set_zorder(10)
# graph4.set_ylim(0, 35)
# graph4.get_legend().remove()


#Save xlsx file
data_analysis.save()

#Display graphs
plt.show()







