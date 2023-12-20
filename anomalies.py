import json
import pandas as pd
from geopy.distance import geodesic
import math
import warnings

warnings.filterwarnings("ignore")



file_path_1 = "./junction_temp.json"
file_path_2 = "./junction_groups.json"
file_path_3 = "./junctions_coordinates.json"



def calculate_new_coordinates(coord_a, coord_b, distance_ac):
    # Calculate the differences in latitude and longitude
    delta_latitude = coord_b[0] - coord_a[0]
    delta_longitude = coord_b[1] - coord_a[1]

    # Calculate the distance between A and B
    distance_ab = geodesic(coord_a, coord_b).kilometers

    # Calculate the fraction of the total distance represented by distance_ac
    fraction = distance_ac / distance_ab

    # Calculate the new coordinates of point C
    new_latitude = coord_a[0] + fraction * delta_latitude
    new_longitude = coord_a[1] + fraction * delta_longitude

    return new_latitude, new_longitude


with open(file_path_1, 'r') as file:
    NODES = json.load(file)
    NODES = NODES["NODES"]
    # print(NODES)

with open(file_path_2, 'r') as file:
    JUNCTIONS = json.load(file)
    JUNCTIONS = JUNCTIONS["JUNCTIONS"]

with open(file_path_3, 'r') as file:
    COORDINATES = json.load(file)
    COORDINATES = COORDINATES["locations"]

df_past = pd.read_csv("./temp_conn_data.csv")
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
    alerts=[]
    alerts_check=[]
    tank_quality={"junction_3":[100],"junction_6":[100]}
    
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

                if (forecast_df[node].loc[timestamp]-data[node]>IQR):
                    NODES[node][2]+=1

                    if NODES[node][2] >= 24*60/15 and node.split("_")[0] != "junction":
                        NODES[node][3]+=1   ### Update Junction anomaly count
                        for _ in JUNCTIONS:
                            if node in JUNCTIONS[_]:
                                if NODES[_][2] <= 5:    ### Check for change in junction_node pressure

                                    ### changes to get anomaly coordinates ###

                                    pressure_drop = (forecast_df[_].loc[NODES[node][1]]) - (pressure[node].loc[NODES[node][1]])
                                    import joblib

                                    # Predict distance from end node
                                    loaded_model = joblib.load('./distance_prediction.pkl')
                                    X_test = (pressure_drop) / (forecast_df[_].loc[NODES[node][1]])
                                    # print(X_test)
                                    
                                    X_test=X_test.mean()

                                    predictions = loaded_model.predict([[X_test]])
                                    # print(predictions)

                                    for i in COORDINATES:
                                        if i["location"]==node:
                                            coord_a=i["coordinates"]
                                        if i["location"]==_:
                                            coord_b=i["coordinates"]


                                    new_coordinates_C = calculate_new_coordinates(coord_a, coord_b, predictions[0])

                                    ### changes end ###

                                    d={}
                                    d["node_1"]=node
                                    d["node_2"]=_
                                    d["anomaly_type"]="Clogging"
                                    temp1 = pd.to_datetime(NODES[node][1])
                                    print("**********************************************")
                                    print(type(temp))
                                    d["time_stamp"]= temp1.strftime('%Y:%m:%d %H:%M:%S')
                                    d["latitude"]=new_coordinates_C[0]
                                    d["longitude"]=new_coordinates_C[1]
                                   
                                    # check for duplicacies in temp
                                    for i in temp:
                                        t=[d["node_1"],d["node_2"]]
                                        if i["node_1"] in t and i["node_2"] in t:
                                            break
                                    else:
                                        temp.append(d)
                                    # end check
                                    
                                    if node not in alerts_check:
                                        temp_dic={}
                                        temp_dic["node_name"]= node
                                        temp1 = pd.to_datetime(NODES[node][1])
                                        print("**********************************************")
                                        print(type(temp))
                                        temp_dic["time_stamp"]= temp1.strftime('%Y:%m:%d %H:%M:%S')
                                        temp_dic["alert_type"]= f"Pipeline Clogging in {node} and {_} connection pipeline"
                                        alerts.append(temp_dic)
                                        alerts_check.append(node)


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
                                    d["latitude"]=30.35
                                    d["longitude"]=76.36

                                    # check for duplicacies in temp
                                    for i in temp:
                                        t=[d["node_1"],d["node_2"]]
                                        if i["node_1"] in t and i["node_2"] in t:
                                            break
                                    else:
                                        temp.append(d)
                                    # end check
                                    
                                    if node not in alerts_check:
                                        temp_dic={}
                                        temp_dic["node_name"]= node
                                        temp1 = pd.to_datetime(NODES[node][1])
                                        print("**********************************************")
                                        print(type(temp))
                                        temp_dic["time_stamp"]= temp1.strftime('%Y:%m:%d %H:%M:%S')
                                        temp_dic["alert_type"]= f"Pipeline Breakage in {node} connection pipeline"
                                        alerts.append(temp_dic)
                                        alerts_check.append(node)

                                    NODES[_][3]+=1  ### Update Junction anomaly count
                                    NODES[_][4].append(forecast_df[_].loc[timestamp]-data[_])
                                break

                            else:   ### Update Junction anomaly count
                                NODES[node][3]=0
                                for _ in JUNCTIONS:
                                    if node in JUNCTIONS[_]:
                                        NODES[_][3]=0
                                        break

                    elif NODES[node][2] <= 24*60/15 and NODES[node][2] >= 12*60/15 and node.split("_")[0] != "junction":
                        if node not in alerts_check:
                            temp_dic={}
                            temp_dic["node_name"]= node
                            # temp_dic["time_stamp"]= timestamp
                            temp1 = pd.to_datetime(timestamp)
                            print("**********************************************")
                            print(type(temp))
                            temp_dic["time_stamp"]= temp1.strftime('%Y:%m:%d %H:%M:%S')
                            temp_dic["alert_type"]= "High Usage"
                            alerts.append(temp_dic)
                            alerts_check.append(node)

                    else:
                        if forecast_df[node].loc[timestamp]-data[node]>IQR*10 + sum(NODES[node][4]):
                            NODES[node][3]+=1
                            NODES[node][4].append(forecast_df[node].loc[timestamp]-data[node])
                            for _ in JUNCTIONS:
                                if _ != node and (forecast_df[_].loc[timestamp]-data[_]>IQR*0.8 + sum(NODES[_][4])):
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
                                    d["latitude"]=30.35
                                    d["longitude"]=76.36

                                    if node not in alerts_check:
                                        temp_dic={}
                                        temp_dic["node_name"]= node
                                        temp1 = pd.to_datetime(NODES[node][1])
                                        print("**********************************************")
                                        print(type(temp))
                                        temp_dic["time_stamp"]= temp1.strftime('%Y:%m:%d %H:%M:%S')
                                        temp_dic["alert_type"]= f"Pipeline Breakage in {node} and {_} connection pipeline"
                                        alerts.append(temp_dic)
                                        alerts_check.append(node)
                                    
                                    # check for duplicacies in temp
                                    for i in temp:
                                        t=[d["node_1"],d["node_2"]]
                                        if i["node_1"] in t and i["node_2"] in t:
                                            break
                                    else:
                                        temp.append(d)
                                    # end check
                                            
                        else:
                            if node in ["junction_3","junction_6"]:
                                diff=forecast_df[node].loc[timestamp]-data[node]
                                if node=="junction_3":
                                    tank_quality[node].append(100-diff)
                                else:
                                    tank_quality[node].append(100-diff)
                            if NODES[node][-1]:
                                NODES[_][3]-=1 ### count of number of flaws on the same pipe
                                NODES[node][4].pop(-1) ### all diff in press
                                NODES[node][-1]-=1


        # print(temp)
    for i in tank_quality:
        tank_quality[i]=sum(tank_quality[i])/len(tank_quality[i])

    for i in alerts:
        for temp2 in temp:
            if i["node_name"] in [temp2["node_1"],temp2["node_2"]] and i["alert_type"]=="High Usage":
                i["alert_type"]=temp2["anomaly_type"]
    # print(temp)
    # print(alerts)
    # print(tank_quality)
    output = {}
    output['anomaly'] = temp
    output['alerts'] = alerts
    output['tank_quality'] = tank_quality
    print(output)
    return output

# import pandas as pd

# df = pd.read_csv("./sim_data_sensor.csv")
# # print(len(df_past))

# df = pd.concat([df_past,df])
# stamp=24*4*7
# df= df.iloc[len(df_past)+stamp-24*4*2:len(df_past)+stamp]
# df = pd.DataFrame(df)#.transpose() 


# actual_df = pd.read_csv("./network_1_sim_results.csv")

# df.drop_duplicates(inplace=True)
# df.set_index(pd.to_datetime(df["Unnamed: 0"]),inplace=True)
# df.drop(labels=["reservoir_1","reservoir_2","Unnamed: 0"],axis=1,inplace=True)

# actual_df= actual_df.rename(columns= {'Unnamed: 0': "Date"})
# actual_df.set_index(pd.to_datetime(actual_df["Date"]),inplace=True)
# actual_df.drop(labels=["reservoir_1","reservoir_2","Date"],axis=1,inplace=True)
# actual_df= pd.concat([df_past,actual_df])
# actual_df= actual_df.iloc[len(df_past)+stamp-24*4*7:len(df_past)+stamp]

# print(df)
# print(actual_df)

# anomalies(df,actual_df)

