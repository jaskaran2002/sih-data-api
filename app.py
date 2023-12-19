from flask import Flask,Response,request
import numpy as np
import pandas as pd
import json
from flask_cors import CORS, cross_origin
import os
from anomalies import anomalies

app = Flask(__name__)
CORS(app)

df_past = pd.read_csv("./temp_conn_data.csv")
# df_past = pd.read_csv(os.getcwd() + '/sih-data-api/' + "temp_conn_data.csv") #deployment
# df_past = df_past.rename(columns={'Unnamed: 0': 'Date'})
df_past.set_index(pd.to_datetime(df_past['Unnamed: 0']), inplace=True)
df_past.drop(labels=["reservoir_1", "reservoir_2", "Unnamed: 0"], axis=1, inplace=True)

@app.route('/')
def hello():
    temp =  os.getcwd()
    return Response(json.dumps({'path': temp}), mimetype='application/json', status=200)
    return "hello world"


@app.get('/flow')
def flow():
    l = request.get_json()
    print(l)
    try:
        out = []
        # with open(os.getcwd() + '/sih-data-api/' + 'network_1_pipes.json', 'r') as f: # Deployment
        with open('./network_1_pipes.json', 'r') as f: # local Testing
            network_pipes = json.load(f)
        for key in l.keys():
            if key == 'Date' or 'junction' in key or 'reservoir' in key:
                continue
            start_node = key
            for pipe_obj in network_pipes['pipe_data']:
                k = list(pipe_obj.keys())[0]
                if pipe_obj[k]['start_node_name'] != start_node:
                    continue
                end_node = pipe_obj[k]['end_node_name']
                length = pipe_obj[k]['length']
                diameter = pipe_obj[k]['diameter']
                f = 1
                density = 1000
                factor = (2 * diameter) / (f * length * density)
                diff_df = l[end_node] - l[start_node]
                final_df = np.sqrt(factor* diff_df) * 1000
                dict_ = {}
                dict_['node_name'] = key
                dict_['timestamp'] = l['Date']
                dict_['flow_rate'] = final_df
                out.append(dict_)
                break
        print(dict_)
        return Response(json.dumps(out), mimetype='application/json', status=200)
    except Exception as e:
        return Response(json.dumps({'Error': str(e)}), mimetype='application/json', status=200)
    


@app.get('/anomaly')
def aaaa():
    l = request.get_json()
    i = l['counter']
    stamp = i
    # actual = pd.read_csv('./sim_data_sensor.csv')
    # pressure = pd.DataFrame(actual.iloc[i]).T

    # predicted = pd.read_csv('./network_1_sim_results.csv')
    # # forecast_df = predicted.iloc[i]
    # forecast_df = predicted

    df = pd.read_csv("./sim_data_sensor.csv")
    # df = pd.read_csv(os.getcwd() + '/sih-data-api/' + "sim_data_sensor.csv") # deployment
    # print(len(df_past))

    df = pd.concat([df_past,df])
    # stamp=24*4*27
    df= df.iloc[len(df_past)+stamp-24*4*2:len(df_past)+stamp]
    df = pd.DataFrame(df)#.transpose() 


    actual_df = pd.read_csv("./network_1_sim_results.csv")
    # actual_df = pd.read_csv(os.getcwd() + '/sih-data-api/' + "network_1_sim_results.csv") #deployment

    df.drop_duplicates(inplace=True)
    df.set_index(pd.to_datetime(df["Unnamed: 0"]),inplace=True)
    df.drop(labels=["reservoir_1","reservoir_2","Unnamed: 0"],axis=1,inplace=True)

    actual_df= actual_df.rename(columns= {'Unnamed: 0': "Date"})
    actual_df.set_index(pd.to_datetime(actual_df["Date"]),inplace=True)
    actual_df.drop(labels=["reservoir_1","reservoir_2","Date"],axis=1,inplace=True)
    actual_df= pd.concat([df_past,actual_df])
    print('before')
    ret = anomalies(df, actual_df)
    print('after')
    return Response(json.dumps(ret), mimetype='application/json', status=200)