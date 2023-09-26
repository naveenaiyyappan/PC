#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import os
import json
import sqlite3


# In[2]:


#1
df_agg_trans = pd.read_csv("agg_trans.csv")


# In[3]:


df_agg_trans


# In[4]:


print(len(df_agg_trans))


# In[5]:


df_aggregated_transaction = pd.DataFrame(df_agg_trans)     #Df


# In[6]:


df_aggregated_transaction


# In[7]:


#2
df_agg_user=pd.read_csv("agg_user.csv")


# In[8]:


df_agg_user


# In[9]:


print(len(df_agg_user))


# In[10]:


df_aggregated_user = pd.DataFrame(df_agg_user)      #Df


# In[11]:


df_aggregated_user


# In[12]:


#3
df_map_trans=pd.read_csv("map_trans.csv")


# In[13]:


df_map_trans


# In[14]:


print(len(df_map_trans))


# In[15]:


df_map_transaction = pd.DataFrame(df_map_trans)       #Df


# In[16]:


df_map_transaction 


# In[17]:


#4
df_map_user=pd.read_csv("map_user.csv")


# In[18]:


df_map_user


# In[19]:


print(len(df_map_user))


# In[20]:


df_map_user_transaction = pd.DataFrame(df_map_user)      #Df


# In[21]:


df_map_user_transaction


# In[22]:


#5
df_top_trans_dist = pd.read_csv("top_trans_dist.csv")


# In[23]:


df_top_trans_dist


# In[24]:


print(len(df_top_trans_dist))


# In[25]:


df_top_transaction_dist = pd.DataFrame(df_top_trans_dist)       #Df


# In[26]:


df_top_transaction_dist


# In[27]:


#6
df_top_trans_pin = pd.read_csv("top_trans_pin.csv")


# In[28]:


df_top_trans_pin


# In[29]:


print(len(df_top_trans_pin))


# In[30]:


df_top_transaction_pin = pd.DataFrame(df_top_trans_pin)         #Df


# In[31]:


df_top_transaction_pin 


# In[32]:


#7
df_top_user_dist = pd.read_csv("top_user_dist.csv")


# In[33]:


df_top_user_dist


# In[34]:


print(len(df_top_user_dist))


# In[35]:


df_top_user_district = pd.DataFrame(df_top_user_dist)    #Df


# In[36]:


df_top_user_district


# In[37]:


#8
df_top_user_pin = pd.read_csv("top_user_pin.csv")


# In[38]:


df_top_user_pin


# In[39]:


print(len(df_top_user_pin))


# In[40]:


df_top_user_pin_trans = pd.DataFrame(df_top_user_pin)    #Df


# In[41]:


df_top_user_pin_trans


# In[42]:


#connent the sqlite3
connection = sqlite3.connect("Phonepe pulse_data.db")


# In[43]:


cursor = connection.cursor()


# In[44]:


cursor.close()


# In[45]:


import sqlite3
import sqlalchemy
from sqlalchemy import create_engine


# In[46]:


connection = sqlite3.connect("PHONEPE PULSE.db")


# In[47]:


#create a new database engine
engine = create_engine("sqlite:///PHONEPE PULSE.db",echo = True)


# In[48]:


#1
df_aggregated_transaction.to_sql('aggregated_transaction', engine, if_exists = 'replace', index=False,   
                                 dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                       'Year': sqlalchemy.types.Integer, 
                                       'Quater': sqlalchemy.types.Integer, 
                                       'Transaction_type': sqlalchemy.types.VARCHAR(length=50), 
                                       'Transaction_count': sqlalchemy.types.Integer,
                                       'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})


# In[49]:


#2
df_aggregated_user.to_sql('aggregated_user', engine, if_exists = 'replace', index=False,
                          dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                 'Year': sqlalchemy.types.Integer, 
                                 'Quater': sqlalchemy.types.Integer,
                                 'Brands': sqlalchemy.types.VARCHAR(length=50), 
                                 'User_Count': sqlalchemy.types.Integer, 
                                 'User_Percentage': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})


# In[50]:


#3
df_map_transaction.to_sql('map_transaction', engine, if_exists = 'replace', index=False,
                          dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                 'Year': sqlalchemy.types.Integer, 
                                 'Quater': sqlalchemy.types.Integer, 
                                 'District': sqlalchemy.types.VARCHAR(length=50), 
                                 'Transaction_Count': sqlalchemy.types.Integer, 
                                 'Transaction_Amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})


# In[51]:


#4
df_map_user_transaction.to_sql('map_user', engine, if_exists = 'replace', index=False,
                   dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                          'Year': sqlalchemy.types.Integer, 
                          'Quater': sqlalchemy.types.Integer, 
                          'District': sqlalchemy.types.VARCHAR(length=50), 
                          'Registered_User': sqlalchemy.types.Integer, })


# In[52]:


#5
df_top_transaction_dist.to_sql('top_transaction', engine, if_exists = 'replace', index=False,
                         dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                'Year': sqlalchemy.types.Integer, 
                                'Quater': sqlalchemy.types.Integer,   
                                'District_Pincode': sqlalchemy.types.Integer,
                                'Transaction_count': sqlalchemy.types.Integer, 
                                'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})


# In[53]:


#6
df_top_transaction_pin.to_sql('top_transaction', engine, if_exists = 'replace', index=False,
                         dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                                'Year': sqlalchemy.types.Integer, 
                                'Quater': sqlalchemy.types.Integer,   
                                'District_Pincode': sqlalchemy.types.Integer,
                                'Transaction_count': sqlalchemy.types.Integer, 
                                'Transaction_amount': sqlalchemy.types.FLOAT(precision=5, asdecimal=True)})


# In[54]:


#7
df_top_user_district.to_sql('top_user', engine, if_exists = 'replace', index=False,
                   dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                          'Year': sqlalchemy.types.Integer, 
                          'Quater': sqlalchemy.types.Integer,                           
                          'District_Pincode': sqlalchemy.types.Integer, 
                          'Registered_User': sqlalchemy.types.Integer,})


# In[55]:


#8
df_top_user_pin_trans.to_sql('top_user', engine, if_exists = 'replace', index=False,
                   dtype={'State': sqlalchemy.types.VARCHAR(length=50), 
                          'Year': sqlalchemy.types.Integer, 
                          'Quater': sqlalchemy.types.Integer,                           
                          'District_Pincode': sqlalchemy.types.Integer, 
                          'Registered_User': sqlalchemy.types.Integer,})


# In[56]:


get_ipython().system('pip install streamlit')
get_ipython().system('pip install plotly')


# # DASHBOARD CREATING

# In[57]:


import streamlit as st
import plotly.express as px
import matplotlib.pyplot as plt


# In[58]:


get_ipython().system('pip install dash')


# In[59]:


import dash
import dash_html_components as html
import dash_core_components as dcc


# In[60]:


transaction = px.bar(df_aggregated_transaction,x=df_aggregated_transaction["State"],y=df_aggregated_transaction["Transaction_count"])
transaction.update_layout(title="transaction counts of every State",title_x=0.5)

brands = px.bar(df_aggregated_user,x=df_aggregated_user["State"],y=df_aggregated_user["Transaction_count"])
brands.update_layout(title="brand transaction percentage",title_x=0.25)

Quarter = px.bar(df_map_transaction,x=df_map_transaction["Year"],y=df_map_transaction["Transaction_amount"])
Quarter.update_layout(title="quarter transaction",title_x=0.5)

Region = px.bar(df_map_user_transaction,x=df_map_user_transaction["District"],y=df_map_user_transaction["Registered_users"])
Region.update_layout(title="region phonepe users",title_x=0.25)


Top_transaction = px.bar(df_top_transaction_dist,x=df_top_transaction_dist["Transaction_count"],y=df_top_transaction_dist["District"])
Top_transaction.update_layout(title="top transaction",title_x=0.5)

Top_user = px.bar(df_top_user_district,x=df_top_user_district["District"],y=df_top_user_district["Registered_users"])
Top_user.update_layout(title="top user district",title_x=0.25)


# In[61]:


app = dash.Dash(__name__)
app.title = "MY_Phonepe"

#plotly graph 1
transaction = px.bar(df_aggregated_transaction,x=df_aggregated_transaction["State"],y=df_aggregated_transaction["Transaction_count"])
transaction.update_layout(title="transaction counts of every State",title_x=0.5)

#2 
brands = px.bar(df_aggregated_user,x=df_aggregated_user["State"],y=df_aggregated_user["Transaction_count"])
brands.update_layout(title="brand transaction percentage",title_x=0.25)

#3
Quarter = px.bar(df_map_transaction,x=df_map_transaction["Year"],y=df_map_transaction["Transaction_amount"])
Quarter.update_layout(title="quarter transaction",title_x=0.5)

#4 
Region = px.bar(df_map_user_transaction,x=df_map_user_transaction["District"],y=df_map_user_transaction["Registered_users"])
Region.update_layout(title="region phonepe users",title_x=0.25)

#5
Top_transaction = px.bar(df_top_transaction_dist,x=df_top_transaction_dist["Transaction_count"],y=df_top_transaction_dist["District"])
Top_transaction.update_layout(title="top transaction",title_x=0.5)

#6
Top_user = px.bar(df_top_user_district,x=df_top_user_district["District"],y=df_top_user_district["Registered_users"])
Top_user.update_layout(title="top user district",title_x=0.25)

app.layout = html.Div([
    html.H1("Phonepe_Pulse",style = {"text-align":"center","color":"purple"}),
    html.Hr(),
    dcc.Graph(figure = transaction),
    dcc.RadioItems(df_aggregated_transaction["Transaction_type"].unique(),df_aggregated_transaction["Transaction_type"].unique()[0],id="Radio"),
    dcc.Graph(figure = brands),
    dcc.Dropdown(df_aggregated_user["Brand"].unique(),df_aggregated_user["Brand"].unique()[0],id="Drop"),
    dcc.Graph(figure=Quarter),
    dcc.Graph(figure=Region),
    dcc.Dropdown(df_map_user_transaction["Region"].unique(),df_map_user_transaction["Region"].unique()[0],id="Drop"),
    dcc.Graph(figure=Top_transaction),
    dcc.Graph(figure=Top_user)
])



if __name__ == "__main__":
    app.run_server(debug = True, port = 4050)


# In[62]:


# top transaction pin
df_top_transaction_pin.groupby("State")["Transaction_count"].sum().plot(kind="pie")
plt.show()


# In[63]:


df_top_user_pin_trans["State"].unique


# In[64]:


df_top_user_pin_trans.groupby("State")["Registered_users"].sum().plot(kind="pie")
plt.show()


# In[65]:


x=df_top_user_dist["Year"]
area1=df_top_user_dist["Registered_users"]
area2=df_top_user_dist["State"]
plt.bar(x, area1, color='g')
plt.bar(x, area2, color='y')
plt.show()


# In[67]:


a=df_top_user_pin_trans["Year"]
b=df_top_user_pin_trans["Registered_users"]
plt.scatter(a,b)
plt.show()


# In[ ]:




