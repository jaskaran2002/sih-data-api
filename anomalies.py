import json
import pandas as pd
import os


# file_path_1 = './junction_temp.json'
# file_path_2 = './junction_groups.json'
file_path_1 = os.getcwd() + '/sih-data-api/' + "junction_temp.json" # for deployment
file_path_2 = os.getcwd() + '/sih-data-api/' + "junction_groups.json" # for deployment

with open(file_path_1, 'r') as file:
    NODES = json.load(file)
    NODES = NODES["NODES"]
    # print(NODES)

with open(file_path_2, 'r') as file:
    JUNCTIONS = json.load(file)
    JUNCTIONS = JUNCTIONS["JUNCTIONS"]
    # print(JUNCTIONS)

# df_past = pd.read_csv("./temp_conn_data.csv")
df_past = pd.read_csv(os.getcwd() + '/sih-data-api/' + "temp_conn_data.csv") #deployment
# df_past = df_past.rename(columns={'Unnamed: 0': 'Date'})
df_past.set_index(pd.to_datetime(df_past['Unnamed: 0']), inplace=True)
df_past.drop(labels=["reservoir_1", "reservoir_2", "Unnamed: 0"], axis=1, inplace=True)




# print(df_past.index)


# p1=pd.read_csv("sim_data_2(a).csv")
# p2=pd.read_csv("sim_data_2(b).csv")

# pressure=pd.concat([p1,p2])

    # forecast_df=pd.read_csv(r"network_1_sim_results.csv")

def anomalies(pressure,forecast_df):



    temp=[]
    
    # pressure.drop_duplicates(inplace=True)
    # pressure.set_index(pd.to_datetime(pressure["Unnamed: 0"]),inplace=True)
    # pressure.drop(labels=["reservoir_1","reservoir_2","Unnamed: 0"],axis=1,inplace=True)

    # forecast_df= forecast_df.rename(columns= {'Unnamed: 0': "Date"})
    # forecast_df.set_index(pd.to_datetime(forecast_df["Date"]),inplace=True)
    # forecast_df.drop(labels=["reservoir_1","reservoir_2","Date"],axis=1,inplace=True)

    # forecast_df=forecast_df[(forecast_df.index.month==1) & (forecast_df.index.year==2022)]



    for timestamp, data in pressure.iterrows():
    

        for node in NODES:

            IQR = forecast_df[node].quantile(0.75)-forecast_df[node].quantile(0.25)
            # print(IQR)

            ### theshold corssing
            # print(forecast_df[node].loc[timestamp]-data[node])
            if (forecast_df[node].loc[timestamp]-data[node]>IQR) and not NODES[node][0]:
                NODES[node][0]=1    ### theshold check
                NODES[node][1]=timestamp    ### origin timestamp
                NODES[node][2]=1




            ### min count
                
            elif NODES[node][0]:
                # print("ha")

                if (forecast_df[node].loc[timestamp]-data[node]>IQR*10):
                    NODES[node][2]+=1

                    if NODES[node][2] >= 24*60/15 and node.split("_")[0] != "junction":
                        NODES[node][3]+=1   ### Update Junction anomaly count
                        for _ in JUNCTIONS:
                            if node in JUNCTIONS[_]:
                                if NODES[_][2] <= 5:    ### Check for change in junction_node pressure

                                    # pressure_drop = forecast_df[_].loc[NODES][node][1] - pressure[node].loc[NODES][node][1]
                                    d={}
                                    d["node_1"]=node
                                    d["node_2"]=_
                                    d["anomaly_type"]="Clogging"
                                    temp1 = pd.to_datetime(NODES[node][1])
                                    print("**********************************************")
                                    print(type(temp))
                                    d["time_stamp"]= temp1.strftime('%Y:%m:%d %H:%M:%S')
                                    
                                    d["latitute"]=30.35
                                    d["longitude"]=76.36
                                   
                                    # print("HA")
                                    # check for duplicacies in temp
                                    for i in temp:
                                        t=[d["node_1"],d["node_2"]]
                                        if i["node_1"] in t and i["node_2"] in t:
                                            break
                                    else:
                                        temp.append(d)
                                    # end check


                                else:
                                    # print("Break",NODES[node][1],node,_)
                                    d={}
                                    d["node_1"]=node
                                    d["node_2"]=_
                                    d["anomaly_type"]="Break"
                                    temp1 = pd.to_datetime(NODES[node][1])
                                    print("**********************************************")
                                    print(type(temp))
                                    d["time_stamp"]= temp1.strftime('%Y:%m:%d %H:%M:%S')
                                    d["latitute"]=30.35
                                    d["longitude"]=76.36

                                    # check for duplicacies in temp
                                    for i in temp:
                                        t=[d["node_1"],d["node_2"]]
                                        if i["node_1"] in t and i["node_2"] in t:
                                            break
                                    else:
                                        temp.append(d)
                                    # end check
                                            

                                    NODES[_][3]+=1  ### Update Junction anomaly count
                                    NODES[_][4].append(forecast_df[_].loc[timestamp]-data[_])
                                break

                            else:   ### Update Junction anomaly count
                                NODES[node][3]=0
                                for _ in JUNCTIONS:
                                    if node in JUNCTIONS[_]:
                                        NODES[_][3]=0
                                        break

                    else:
                        if forecast_df[node].loc[timestamp]-data[node]*0.8>IQR*10 + sum(NODES[node][4]):
                            NODES[node][3]+=1
                            NODES[node][4].append(forecast_df[node].loc[timestamp]-data[node])
                            for _ in JUNCTIONS:
                                if _ != node and (forecast_df[_].loc[timestamp]-data[_]>IQR*10 + sum(NODES[_][4])):
                                    NODES[_][3]+=1
                                    NODES[_][4].append(forecast_df[node].loc[timestamp]-data[node])
                                    NODES[_][-1]+=1
                                    NODES[node][-1]+=1
                                    # print("Break",NODES[node][1],node,_)
                                    d={}
                                    d["node_1"]=node
                                    d["node_2"]=_
                                    d["anomaly_type"]="Break"
                                    temp1 = pd.to_datetime(NODES[node][1])
                                    print("**********************************************")
                                    print(type(temp))
                                    d["time_stamp"]= temp1.strftime('%Y:%m:%d %H:%M:%S')
                                    d["longitude"]=76.36
                                    
                                    # check for duplicacies in temp
                                    for i in temp:
                                        t=[d["node_1"],d["node_2"]]
                                        if i["node_1"] in t and i["node_2"] in t:
                                            break
                                    else:
                                        temp.append(d)
                                    # end check
                                            
                        else:
                            if NODES[node][-1]:
                                NODES[_][3]-=1 ### count of number of flaws on the same pipe
                                NODES[node][4].pop(-1) ### all diff in press
                                NODES[node][-1]-=1

        # print(temp)

    print(temp)
    return temp


# actual_df= actual_df.iloc[len(df_past)+stamp-24*4*7:len(df_past)+stamp]

# print(df)
# print(actual_df)

# anomalies(df,actual_df)