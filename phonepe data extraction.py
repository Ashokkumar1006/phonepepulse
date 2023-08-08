import git
git.Repo.clone_from("https://github.com/PhonePe/pulse.git", "phonepe_pulse")\
import pandas as pd
import json
import os

# This is to direct the path to get the data as states

# Aggregate Transaction DataFrame

path="pulse/data/aggregated/transaction/country/india/state/"
Agg_state_list = os.listdir(path)
Agg_state_list
# Agg_state_list--> to get the list of states in India

# <------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

# This is to extract the data's to create a dataframe

aggtrans = {'State':[], 'Year':[],'Quater':[],'Transacion_type':[], 'Transacion_count':[], 'Transacion_amount':[]}
for i in Agg_state_list:
    p_i = path+i+"/"
    Agg_yr = os.listdir(p_i)
    for j in Agg_yr:
        p_j = p_i+j+"/"
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
            p_k = p_j+k
            Data = open(p_k,'r')
            D = json.load(Data)
            for z in D['data']['transactionData']:
                Name = z['name']
                count = z['paymentInstruments'][0]['count']
                amount = z['paymentInstruments'][0]['amount']
                aggtrans['Transacion_type'].append(Name)
                aggtrans['Transacion_count'].append(count)
                aggtrans['Transacion_amount'].append(amount)
                aggtrans['State'].append(i)
                aggtrans['Year'].append(j)
                aggtrans['Quater'].append(int(k.strip('.json')))
# Succesfully created a dataframe
Agg_TransDF=pd.DataFrame(aggtrans)
#<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

# Agregated User DataFrame

agguserpath = path="pulse/data/aggregated/user/country/india/state/"
agguserlist = os.listdir(agguserpath)

Agg_user = {'State':[], 'Year':[],'Quater':[],'Device Brand':[], 'Count':[],'Percentage':[]}

for i in agguserlist:
    p_i = path+i+"/"   #"/content/pulse/data/aggregated/user/country/india/state/i/(andaman & nicobar islands,...)"
    agguseryr = os.listdir(p_i)
    for j in agguseryr:
        p_j = p_i+j+"/"   #"/content/pulse/data/aggregated/user/country/india/state/i/(andaman & nicobar islands,...)/j(2018,2019,...)"
        agguseryr_list = os.listdir(p_j)
        for k in agguseryr_list:
            p_k = p_j+k     #"/content/pulse/data/aggregated/user/country/india/state/i/(andaman & nicobar islands,...)/j(2018,2019,...)/k(1.json,..)"
            Data = open(p_k,'r')
            agguserraw = json.load(Data)
            if agguserraw['data']['usersByDevice'] is not None:
                for z in agguserraw['data']['usersByDevice']:
                    Name=z['brand']
                    count=z['count']
                    percentage=z['percentage']
                    Agg_user['Device Brand'].append(Name)
                    Agg_user['Count'].append(count)
                    Agg_user['Percentage'].append(percentage)
                    Agg_user['State'].append(i)
                    Agg_user['Year'].append(j)
                    Agg_user['Quater'].append(int(k.strip('.json')))

# #Succesfully created a dataframe
Agg_userDF = pd.DataFrame(Agg_user)
#<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

# Map Transaction DataFrame

maptranspath = 'pulse/data/map/transaction/hover/country/india/state/'
maptranslist = os.listdir(maptranspath)
maptrans_raw = {'State':[],'District Name':[],'Year':[],'Quater':[],'Count':[],'amount':[]}
for state in maptranslist:
    p_state=maptranspath+state+"/"
    maptrans_state = os.listdir(p_state)
    for year in maptrans_state:
        p_year = p_state+year+"/"
        maptrans_yrlist = os.listdir(p_year)
        for doc in maptrans_yrlist:
            p_doc = p_year+doc
            Data = open(p_doc,'r')
            D = json.load(Data)
            for z in D['data']['hoverDataList']:
                Name = z['name']
                count = z['metric'][0]['count']
                amount = z['metric'][0]['amount']
                # print(Name,count,amount)
                maptrans_raw['District Name'].append(Name)
                maptrans_raw['Count'].append(count)
                maptrans_raw['amount'].append(amount)
                maptrans_raw['State'].append(state)
                maptrans_raw['Year'].append(year)
                maptrans_raw['Quater'].append(int(doc.strip('.json')))

maptransdf = pd.DataFrame(maptrans_raw)
#maptransdf
#<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

# Map User DataFrame
mapuserpath = 'pulse/data/map/user/hover/country/india/state/'
mapuserlist = os.listdir(mapuserpath)
mapuserraw = {'State':[],'District Name':[],'Year':[],'Quater':[],'Registered Users':[],'App opens':[]}
for state in mapuserlist:
    p_state = mapuserpath+state+"/"
    mapuserstate = os.listdir(p_state)
    for year in mapuserstate:
        p_year = p_state+year+"/"
        mapuseryrlist = os.listdir(p_year)
        for doc in mapuseryrlist:
            p_doc = p_year+doc
            Data = open(p_doc,'r')
            mapuserjson  =json.load(Data)
            for z ,values in mapuserjson['data']['hoverData'].items():
                name=z
                regusers=values['registeredUsers']
                appopen=values['appOpens']
                state=state
                district=z
                quater=int(doc.strip('.json'))
                mapuserraw['State'].append(state)
                mapuserraw['District Name'].append(z)
                mapuserraw['Year'].append(year)
                mapuserraw['Quater'].append(quater)
                mapuserraw['Registered Users'].append(regusers)
                mapuserraw['App opens'].append(appopen)
mapuserdf=pd.DataFrame(mapuserraw)

#<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

# Top Transaction DataFrame
toptranspath = 'pulse/data/top/transaction/country/india/state/'
toptranslist = os.listdir(toptranspath)
toptransraw = {'State':[],'District Name':[],'Year':[],'Quater':[],'Transaction Type':[],'Transaction Count':[],'Transaction amount':[]}
for state in toptranslist:
    p_state = toptranspath+state+"/"
    toptrans_state = os.listdir(p_state)
    for year in toptrans_state:
        p_year = p_state+year+"/"
        toptrans_yrlist = os.listdir(p_year)
        for doc in toptrans_yrlist:
            p_doc = p_year+doc
            Data =open(p_doc,'r')
            D = json.load(Data)
            # print(D)
            for z in D['data']['districts']:
              state = state
              transyear = year
              # quater=
              district = z['entityName']
              transtype = z['metric']['type']
              transcount = z['metric']['count']
              transamount = z['metric']['amount']
              toptransraw['State'].append(state)
              toptransraw['District Name'].append(district)
              toptransraw['Year'].append(year)
              toptransraw['Quater'].append(int(doc.strip('.json')))
              toptransraw['Transaction Type'].append(transtype)
              toptransraw['Transaction Count'].append(transcount)
              toptransraw['Transaction amount'].append(transamount)
toptransdf = pd.DataFrame(toptransraw)

#<------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------>#

#Top User DataFrame
topuserpath= 'pulse/data/top/user/country/india/state/'
topuserlist=os.listdir(topuserpath)
topuserraw={'State':[],'District Name':[],'Year':[],'Quater':[],'Registered Users':[]}
for state in topuserlist:
    p_state=topuserpath+state+"/"
    topuserstate=os.listdir(p_state)
    for year in topuserstate:
        p_year=p_state+year+"/"
        topuseryrlist=os.listdir(p_year)
        for jsons in topuseryrlist:
            p_doc=p_year+jsons
            Data=open(p_doc,'r')
            topuserjson=json.load(Data)
            for z in topuserjson['data']['districts']:
                name=z['name']
                regusers=z['registeredUsers']
                state=state
                quater=int(doc.strip('.json'))
                topuserraw['State'].append(state)
                topuserraw['District Name'].append(name)
                topuserraw['Year'].append(year)
                topuserraw['Quater'].append(int(jsons.strip('.json')))
                topuserraw['Registered Users'].append(regusers)
topuserdf=pd.DataFrame(topuserraw)
# To see the DATAFRAMES uncomment the below code ▼
# print('\nAgg_Trans:','\n',Agg_TransDF,'\nAgg_User:','\n',Agg_userDF,'\nMap_Trans:','\n',maptransdf,'\nMap_User:','\n',mapuserdf,'\nTop_Trans:','\n',toptransdf,'\nTop_User:','\n',topuserdf)
