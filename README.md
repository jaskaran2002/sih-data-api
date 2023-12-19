jaskaran2002.pythonanywhere.com


GET /flow:
  Input Json:
    ```
    {
      "Date": ,
      "Hostel_O" : value of hostel O,
      "Junction_1": junctions related to hostel_O or just do all the junctions
    }
    ```
  Output Json:
    ```
    [
      {
        "node name": ,
        "timestamp": ,
        "flow_rate": ,
        }
    ]
    ```
     



Example INPUT:

{"hostel_O": 189.34443689099413,
 "hostel_D": 187.261095850864,
 "hostel_C": 198.1062973562738,
 "hostel_B": 183.0343904225088,
 "hostel_A": 207.01363649827795,
 "hostel_J": 182.78448667639915,
 "hostel_H": 209.67384042706323,
 "hostel_Q": 207.4047641554224,
 "hostel_K": 94.04253118022912,
 "hostel_I": 226.7181143018747,
 "hostel_M": 174.92384299315708,
 "hostel_PG": 179.02025713037278,
 "library": 254.7948879323372,
 "pool": 198.64934806642472,
 "cos_complex": 191.70902923831443,
 "synthetic_running_track": 147.35597528936643,
 "admin_block": 253.07076990184552,
 "D_block": 259.87863847442424,
 "F_block": 237.03929178870524,
 "E_block": 246.9805192571457,
 "G_block": 233.84239598834307,
 "junction_1": 214.6206671102335,
 "junction_2": 215.39203515951945,
 "junction_3": 215.00009157384667,
 "junction_4": 211.87598445351657,
 "junction_5": 260.42345611836834,
 "junction_6": 262.2548039713629,
 "Date": "2023-01-01 00:00:00"}
