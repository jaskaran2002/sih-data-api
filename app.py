from flask import Flask,Response,request
import numpy as np
import pandas as pd
import json
from flask_cors import CORS, cross_origin
import os

app = Flask(__name__)
CORS(app)

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
        with open(os.getcwd() + '/sih-data-api/' + 'network_1_pipes.json', 'r') as f: # Deployment
        # with open('./network_1_pipes.json', 'r') as f: # local Testing
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