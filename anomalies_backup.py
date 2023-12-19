import json
import pandas as pd
import os


file_path_1 = './junction_temp.json'
file_path_2 = './junction_groups.json'
# file_path_1 = os.getcwd() + '/sih-data-api/' + "junction_temp.json" # for deployment
# file_path_2 = os.getcwd() + '/sih-data-api/' + "junction_groups.json" # for deployment

with open(file_path_1, 'r') as file:
    NODES = json.load(file)
    NODES = NODES["NODES"]
    # print(NODES)

with open(file_path_2, 'r') as file:
    JUNCTIONS = json.load(file)
    JUNCTIONS = JUNCTIONS["JUNCTIONS"]
    # print(JUNCTIONS)


# p1=pd.read_csv("sim_data_2(a).csv")
# p2=pd.read_csv("sim_data_2(b).csv")

# pressure=pd.concat([p1,p2])

    # forecast_df=pd.read_csv(r"network_1_sim_results.csv")

def anomalies(pressure,forecast_df):

    d = []
#     d=[{
#     "node_1":[],
#     "node_2":[],
#     "anomaly_type":[],
#     "time_stamp":[]
#    }]
    
    counter=0

    pressure.drop_duplicates(inplace=True)
    pressure.set_index(pd.to_datetime(pressure["Unnamed: 0"]),inplace=True)
    pressure.drop(labels=["reservoir_1","reservoir_2","Unnamed: 0"],axis=1,inplace=True)
    # pressure.drop_duplicates(inplace=True)
    # pressure = pressure.drop(columns=['reservoir_1', 'reservoir_2'])
    # pressure = pressure.rename(columns= {'Unnamed: 0': "Date"})
    # pressure['Date'] = pd.to_datetime(pressure.Date)
    # pressure = pressure.set_index(pressure.Date)
    # pressure.drop('Date', axis = 1, inplace = True)



    forecast_df.set_index(pd.to_datetime(forecast_df["Unnamed: 0"]),inplace=True)
    forecast_df.drop(labels=["reservoir_1","reservoir_2","Unnamed: 0"],axis=1,inplace=True)

    forecast_df=forecast_df[(forecast_df.index.month==1) & (forecast_df.index.year==2022)]



    for timestamp, data in pressure.iterrows():

        for node in NODES:

            IQR = forecast_df[node].quantile(0.75)-forecast_df[node].quantile(0.25)
            # print(IQR)

            ### theshold corssing

            if (forecast_df[node].loc[timestamp]-data[node]>IQR) and not NODES[node][0]:
                NODES[node][0]=1    ### theshold check
                NODES[node][1]=timestamp    ### origin timestamp
                NODES[node][2]=1


            ### min count
                
            elif NODES[node][0]:

                if (forecast_df[node].loc[timestamp]-data[node]>IQR):
                    NODES[node][2]+=1

                    if NODES[node][2] >= 24*60/15 and node.split("_")[0] != "junction":
                        NODES[node][3]+=1   ### Update Junction anomaly count
                        for _ in JUNCTIONS:
                            if node in JUNCTIONS[_]:
                                if NODES[_][2] <= 5:    ### Check for change in junction_node pressure
                                    print("Clogging",NODES[node][1],node,_)
                                    temp = {}
                                    temp["node_1"]=node
                                    temp["node_2"]=_
                                    temp["anomaly_type"]="Clogging",
                                    temp["time_stamp"]=NODES[node][1]
                                    d.append(temp)
                                else:
                                    print("Break",NODES[node][1],node,_)
                                    temp = {}
                                    temp["node_1"]=node
                                    temp["node_2"]=_
                                    temp["anomaly_type"]="Break",
                                    temp["time_stamp"]=NODES[node][1]
                                    d.append(temp)
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
                                    print("Break",NODES[node][1],node,_)
                                    temp = {}
                                    temp["node_1"]=node
                                    temp["node_2"]=_
                                    temp["anomaly_type"]="Break",
                                    temp["time_stamp"]=NODES[node][1]
                                    d.append(temp)
                                    # counter+=1

                        else:
                            if NODES[node][-1]:
                                NODES[_][3]-=1 ### count of number of flaws on the same pipe
                                NODES[node][4].pop(-1) ### all diff in press
                                NODES[node][-1]-=1
    print(d)
    return d

