import pandas as pd
import json
import os

def data_aggregated_transaction(clm, data, i, j, k):

    if "Transaction_type" not in clm.keys():
        clm['Transaction_type'] = []
        clm['Transaction_count'] = []
        clm['Transaction_amount'] = []

    transaction_data = data.get('data', {}).get('transactionData', [])
    for transaction in transaction_data:
        name = transaction['name']
        count = transaction['paymentInstruments'][0]['count']
        amount = transaction['paymentInstruments'][0]['amount']

        clm['State'].append(i)
        clm['Year'].append(j)
        clm['Quarter'].append(int(k.strip('.json')))
        clm['Transaction_type'].append(name)
        clm['Transaction_count'].append(count)
        clm['Transaction_amount'].append(amount)
    return clm

def data_aggregated_user(clm,data,i,j,k):

    if "Brand" not in clm.keys():
        clm['Brand'] = []
        clm['Count'] = []
        clm['Percentage'] = []

    users_data = data.get('data', {}).get('usersByDevice')
    if users_data is not None:
        for user in users_data:
            brand = user.get('brand', '')
            count = user.get('count', 0)
            percentage = user.get('percentage', 0)

            clm['State'].append(i)
            clm['Year'].append(j)
            clm['Quarter'].append(int(k.strip('.json')))
            clm['Brand'].append(brand)
            clm['Count'].append(count)
            clm['Percentage'].append(percentage)
    return clm

def data_map_transaction(clm, data, i, j, k):

    if "District" not in clm.keys():
        clm['District'] = []
        clm['Count'] = []
        clm['Amount']  = []

    hover_data_list = data.get('data', {}).get('hoverDataList', [])
    for hover_data in hover_data_list:
        name = hover_data['name']
        count = hover_data['metric'][0]['count']
        amount = hover_data['metric'][0]['amount']

        clm['State'].append(i)
        clm['Year'].append(j)
        clm['Quarter'].append(int(k.strip('.json')))
        clm['District'].append(name)
        clm['Count'].append(count)
        clm['Amount'].append(amount)
    return clm

def data_map_user(clm, data, i, j, k):

    if 'District' not in clm.keys():
        clm['District'] = []
        clm['Users'] = []

    hover_data = data.get('data', {}).get('hoverData', {})
    for district, values in hover_data.items():
        users = values.get('registeredUsers')

        clm['State'].append(i)
        clm['Year'].append(j)
        clm['Quarter'].append(int(k.strip('.json')))
        clm['District'].append(district)
        clm['Users'].append(users)
    return clm

def data_top_transaction(clm, data, i, j, k):

    if 'District' not in clm.keys():
        clm['District'] = []
        clm['Count'] = []
        clm['Amount'] = []

    districts = data.get('data', {}).get('districts', [])
    for district in districts:
        count = district['metric'].get('count')
        amount = district['metric'].get('amount')
        name = district['entityName']

        clm['State'].append(i)
        clm['Year'].append(j)
        clm['Quarter'].append(int(k.strip('.json')))
        clm['District'].append(name)
        clm['Count'].append(count)
        clm['Amount'].append(amount)
    return clm

def data_top_users(clm,data,i,j,k):

    if "Users" not in clm.keys():
        clm["Users"] = []
        clm["District"] = []

    districts = data.get('data', {}).get('districts', [])
    for district in districts:
        name = district['name']
        users = district.get('registeredUsers')

        clm['State'].append(i)
        clm['Year'].append(j)
        clm['Quarter'].append(int(k.strip('.json')))
        clm['District'].append(name)
        clm['Users'].append(users)
    return clm

def data(path,req):

    clm = {
        'State': [],
        'Year': [],
        'Quarter': [],
    }

    state_list = os.listdir(path)
    for i in state_list:
        p_i = os.path.join(path, i)
        yr_list = os.listdir(p_i)
        for j in yr_list:
            p_j = os.path.join(p_i, j)
            qtr_list = os.listdir(p_j)
            for k in qtr_list:
                p_k = os.path.join(p_j, k)
                with open(p_k, 'r') as file:
                    data = json.load(file)

                if req == "top_users":
                    data_access = data_top_users(clm, data, i, j, k)
                elif req == "aggregated_transaction":
                    data_access = data_aggregated_transaction(clm, data, i, j, k)
                elif req == "aggregated_user":
                    data_access = data_aggregated_user(clm, data, i, j, k)
                elif req == "map_transaction":
                    data_access = data_map_transaction(clm, data, i, j, k)
                elif req == "map_user":
                    data_access = data_map_user(clm, data, i, j, k)
                elif req == "top_transaction":
                    data_access = data_top_transaction(clm, data, i, j, k)

    return pd.DataFrame(data_access)




def main():

    path = "C:\\Users\\giriv\\phonepe_pulse\\pulse\\data\\aggregated\\transaction\\country\\india\\state"
    df1 = data(path, "aggregated_transaction")


    path = "C:\\Users\\giriv\\phonepe_pulse\\pulse\\data\\aggregated\\user\\country\\india\\state"
    df2 = data(path, "aggregated_user")

    path = "C:\\Users\\giriv\\phonepe_pulse\\pulse\\data\\map\\transaction\\hover\\country\\india\\state"
    df3 = data(path, "map_transaction")

    path = "C:\\Users\\giriv\\phonepe_pulse\\pulse\\data\\map\\user\\hover\\country\\india\\state"
    df4 = data(path, "map_user")

    path = "C:\\Users\\giriv\\phonepe_pulse\\pulse\\data\\top\\transaction\\country\\india\\state"
    df5 = data(path, "top_transaction")

    path = "C:\\Users\\giriv\\phonepe_pulse\\pulse\\data\\top\\user\\country\\india\\state"
    df6 = data(path, "top_users")

    df1.to_csv('agg_transaction.csv', index=False)
    df2.to_csv('agg_users.csv', index=False)
    df3.to_csv('map_transcation.csv', index=False)
    df4.to_csv('map_users.csv', index=False)
    df5.to_csv('top_transcation.csv', index=False)
    df6.to_csv('top_users.csv', index=False)



if __name__ == "__main__":
    main()




