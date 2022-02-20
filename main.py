#START application
#START imports
import PySimpleGUI as sg
import json
import datetime
import os

import gcsfs
import pyarrow.parquet as pq
import pandas as pd
#END imports

#Theme of the application
sg.theme('DarkAmber')

#Output format
output = { "full_visitor_id": "[numeric]",
           "address_changed": "[boolean]",
           "is_order_placed": "[boolean]",
           "is_order_delivered": "[boolean]",
           "application_type": "[iOS|Android|BlackBerry]" }

json_output = json.dumps(output, indent=2)

#Event list to decide if address is changed           
event_list = ['address.submitted',
'address_update.submitted',
'address_update.clicked',
'geolocation_dialogue.accepted',
'address_edit.clicked',
'new_address.clicked',
'other_location.clicked',
'geolocation.received']

#Requirements screen text
requirements = "Requirements:\n\n\
For this app, you will need GSC project name and bucket for sessions and backend. \n\
You need to give at least one full_visitor_id. If you want to add more than one, please enter them using comma seperator (,). \n\
Your data structure for sessions need to have; full_visitor_id, operatingSystem, hit.eventAction, hit.transactionId. \n\
Your data structure for backend need to have frontendOrderId, geopointCustomer and geopointDropoff.\n\
Please first proceed to 'Filter Data' screen with the button to get your results after that,\n\
if you want to download your data please press 'Download Data' button.\n\
If you want to filter your data, please press 'Find Data' button and if you want to download your data please select Ok button.\n"

#bucket names      
default_bucket_name_sessions = "product-analytics-hiring-tests-public/GoogleAnalyticsSample/ga_sessions_export/"
default_bucket_name_backend = "product-analytics-hiring-tests-public/BackendDataSample/transactionalData/"

#layout of requirements (first) screen
layout_initial_screen = [[sg.Text(requirements)]]

#layout of inputs that is required to get data in Filter Data (second) screen
layout_input = [[sg.Text('full_visitor_id', size = (len('bucket_name_backend'),1)), sg.InputText(default_text= 10142370647475443937, size = (len(default_bucket_name_sessions),1))],
               [sg.Text('project_name', size = (len('bucket_name_backend'),1)), sg.InputText(default_text= "DHH - Analytics - Hiring Space", size = (len(default_bucket_name_sessions),1),key="project_name")],
               [sg.Text('bucket_name_sessions', size = (len('bucket_name_backend'),1)), sg.InputText(default_text= default_bucket_name_sessions, size = (len(default_bucket_name_sessions),1),key="bucket_name_sessions")],
               [sg.Text('bucket_name_backend', size = (len('bucket_name_backend'),1)), sg.InputText(default_text= default_bucket_name_backend, size = (len(default_bucket_name_sessions),1),key="bucket_name_backend")],
               [sg.Button('Find Data')]]

#layout for outputs in Filter Data (second) screen
layout_output = [[sg.Text('json_output')],
                [sg.Text(json_output, key = "-OUTPUT-")]]

#layout for Download (third) screen    
layout_download = [ [sg.Text('Choose where to download: '), sg.FolderBrowse('Browse')],
                    [sg.Text('File has been updated!',visible=False, key = "fe")],
                    [sg.Text('File has been created!',visible=False, key = "fd")],[sg.Button('Ok')]]        

#layout to combine input and output layouts in Filter Data screen
layout_filter = [[sg.Column(layout_input)], [sg.Column(layout_output)]]

#Layout to combine all screens
layout_main = [[sg.Column(layout_initial_screen,key='-COL0-'), sg.Column(layout_filter, key='-COL1-',visible=False), sg.Column(layout_download, key='-COL2-',visible=False)],
          [sg.Button('Filter Data'), sg.Button('Download Data'), sg.Button('Exit')]]

# Create the Window
window = sg.Window('Filter Data', layout_main)
while True:
    
    # Event Loop to process "events" and get the "values" of the inputs. 
    #events are used for button clickes and values are the input and output data results
    event, values = window.read()
    #if user closes window or clicks cancel
    if event == sg.WIN_CLOSED or event == 'Exit': 
        break
    #if "Filter Data" button is clicked, only Filter Data screen is visible and other screens are invisible.
    if event == 'Filter Data':
        window['-COL2-'].update(visible=False)
        window['-COL1-'].update(visible=True)
        window['-COL0-'].update(visible=False)
    
    #if "Find Data" button is clicked, result is calculated according to inputs that are given
    if event == 'Find Data':
        #connecting to project in gsc
        try:
            gs = gcsfs.GCSFileSystem(project = values['project_name'])
        except:
            print("there is a problem connecting to " + values['project_name'] +", please retry")
            exit(1)
            
        file_path_sessions = []
        #connecting to sessions bucket, if problem exit
        try:
            file_list_sessions = gs.glob(values['bucket_name_sessions'])
        except:
            print("there is a problem connecting to "+values['bucket_name_sessions']+", please retry")
            exit(1)
        #getting parquet datasets and turning them into pandas dataframes, if a problem occurs exit.
        try:
            for s_sessions in file_list_sessions:
                file_path_sessions.append("gs://" + s_sessions)
            arrow_df_sessions = pq.ParquetDataset(file_path_sessions, filesystem=gs)
            df_sessions_data = arrow_df_sessions.read_pandas().to_pandas()
       except:
            print("there is a problem loading data, please retry")
            exit(1)
        #getting one or more full_visitor_ids into list
        full_visitor_id_list = values[0].split(",")
        #constructing json format
        if len(full_visitor_id_list) > 1:
            json_new_output = "["
        else:
            json_new_output = ""
            
        #filtering data with chosen fullvisitorid, and getting operatingSystem, eventAction,transactionId results.
        for i in range(len(full_visitor_id_list)):
            full_visitor_id = full_visitor_id_list[i].strip()
            #filtering data with fullvisitorid, if not present break window is cloesed.
            try:
                df_sessions_data_detail = df_sessions_data[df_sessions_data.fullvisitorid == full_visitor_id]
            except:
                print("fullvisitorid column is not present in data, please retry")
                exit(1)
            #getting operatingSystem result, if not present break window is cloesed.
            try:
                application_type = df_sessions_data_detail["operatingSystem"].iloc[0]
            except:
                print("operatingSystem column is not present in data, please retry")
                exit(1)
                
            is_address_changed = 0
            transaction_id = None
            
            #getting eventAction result from nested hit data to determine if address is changed, if not present break window is cloesed.
            try:
                for sub in df_sessions_data_detail["hit"]:
                    for search in sub:
                        if search['eventAction'] is not None:
                            if search['eventAction'] in event_list:
                                is_address_changed = 1
                    if is_address_changed:
                        break
            except:
                print("eventAction or hit column is not present in data, please retry")
                exit(1)
            #getting transactionId result from nested hit data, if not present break window is cloesed.
            try:
                for sub in df_sessions_data_detail["hit"]:
                    for search in sub:
                        if search["transactionId"] is not None:
                            transaction_id = search["transactionId"]
                    if transaction_id is not None:
                        break
            except:
                print("transactionId column is not present in data, please retry")
                exit(1)
            is_order_placed = 0
            is_order_delivered = 0
            #if transactionId exists for result, connection to backend bucket is constructed and data is filtered accordingly
            if transaction_id is not None:
                file_path_backend = []
                #connection to backend bucket, if not present break window is cloesed.
                try:
                    file_list_backend = gs.glob(values['bucket_name_backend'])
                except:
                    print("there is a problem connecting to "+values['bucket_name_backend']+", please retry")
                    exit(1)
                #getting parquet datasets and turning them into pandas dataframes, if not present break window is cloesed.
                try:
                    for s_backend in file_list_backend:
                        file_path_backend.append("gs://" + s_backend)

                    arrow_df_backend = pq.ParquetDataset(file_path_backend, filesystem=gs)
                    df_backend_data = arrow_df_backend.read_pandas().to_pandas()
                except:
                    print("there is a problem loading data, please retry")
                    exit(1)

                #filtering data with transactionId and finding if customer placed an order and if customer got the order, if not present break window is cloesed.
                try:
                    df_backend_data = df_backend_data[df_backend_data.frontendOrderId == transaction_id]
                except:
                    print("frontendOrderId is not present in the data, please retry.")
                    exit(1)
                try:
                    is_order_placed_data = df_backend_data[df_backend_data["geopointCustomer"].notnull()]
                except:
                    print("geopointCustomer is not present in the data, please retry.")
                    exit(1)
                try:
                    is_order_delivered_data = df_backend_data[df_backend_data["geopointDropoff"].notnull()]
                except:
                    print("geopointDropoff is not present in the data, please retry.")
                    exit(1)
                if is_order_placed_data.shape[0] > 1:
                    is_order_placed = 1
                if is_order_delivered_data.shape[0] > 1:
                    is_order_delivered = 1
            #json output
            output = { "full_visitor_id": full_visitor_id,
                       "address_changed": is_address_changed,
                       "is_order_placed": is_order_placed,
                       "is_order_delivered": is_order_delivered,
                       "application_type": application_type }
            
            #json formatting with 1 or more elements
            if i == 0:
                json_new_output = json_new_output + json.dumps(output, indent=2)
            else:
                json_new_output = json_new_output + "," + json.dumps(output, indent=2)
            output = dict()
        #json formatting with 1 or more elements
        if len(full_visitor_id_list) > 1:
            to_output = json_new_output + "]"
        else:
            to_output = json_new_output
        # Update the ourtput text element to be the values returned from query
        window["-OUTPUT-"].update(to_output)

    #if "Download Data" button is clicked, results are downloaded accordingly
    if event == 'Download Data':
        window['-COL1-'].update(visible=False)
        window['-COL2-'].update(visible=True)
        window['-COL0-'].update(visible=False)
        
    #if Ok button is clicked, data is going to be saved in folder selected as json format
    if event == "Ok":
        #file name is constructed as filter_data_ouyput_yyyymmdd.json
        file_name = "filter_data_output_" + str(datetime.datetime.now().strftime("%Y%m%d"))+".json"
        
        
        #if result from "Browse" has / than path needs to be constructed with / else \
        if "/" in values['Browse']:
            seperator = "/"
        else:
            seperator = "\\"
        
        #path to save file
        path = values['Browse'] + seperator + file_name
        
        #finding if path exists
        f_exists = 0
        if os.path.exists(path):
            f_exists = 1
            
        #file is constructed at file path
        with open(path,"w") as fp:
            fp.write(to_output)
            
        #if file exists, than result of fe is written when save is completed else fd.
        if f_exists:
            window['fe'].update(visible=True)
        else:
            window['fd'].update(visible=True)
            
#closing the window      
window.close()


#END application