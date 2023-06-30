import json
import h5py
import docker
import os
import ciso8601
import datetime
import numpy as np
import pandas as pd
import requests
from pathlib import Path
from tsfresh.feature_extraction import extract_features, MinimalFCParameters
from interq_cip_qhs.process.utils import copy_to_container, jprint


class SawingProcessData:
    def __init__(self, path_data):
        self.owner = "ptw"
        self.docker_client = docker.from_env()
        self.tmp_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../tmp_files"))
        self._path = path_data
        self.process_name = "cutting"
        self.cid = "6LHWRqwyG1jGobMJMyUjsgsA5u52y37dtiu6bPSrXFX1"
        self.pwd = "interq"
        self.api_endpoint = "http://localhost:6005/interq/tf/v1.0/qhs"
        self.dqaas_endpoint = "http://localhost:8000/DuplicateRecords/"
        self.features = [
            "CPU_Kuehler_Temp",
            "CPU_Temp",
            "CutCounter"
            "CutTime",
            "FFT_Anforderung",
            "FlatstreamCutCounter",
            "FlatstreamDone",
            "FsMode_1Raw_2FftRaw_3FttHK",
            "HebenAktiv",
            "MotorAn",
            "PData.CosPhi",
            "PData.CutEnergy",
            "PData.PEff",
            "P_Vorschub",
            "Position",
            "Position_Band",
            "TData.T1",
            "TData.T2",
            "TData.T3",
            "TData.T4",
            "TData.T_IR",
            "Vib01.CREST",
            "Vib01.Kurtosis",
            "Vib01.Peak",
            "Vib01.RMS",
            "Vib01.Skewness",
            "Vib01.VDI3832",
            "Vib02.CREST",
            "Vib02.Kurtosis",
            "Vib02.Peak",
            "Vib02.RMS",
            "Vib02.Skewness",
            "Vib02.VDI3832",
            "Vib03.CREST",
            "Vib03.Kurtosis",
            "Vib03.Peak",
            "Vib03.RMS",
            "Vib03.Skewness",
            "Vib03.VDI3832",
            "ZaehneProBand",
            "bCutActive",
            "fLichtschranke",
            "obereMaterialkante",
            "vVorschub"
        ]

    def extract_features(self, data):
        features = extract_features(
            data,
            column_sort="time",
            column_id="id",
            default_fc_parameters=MinimalFCParameters(),
        )
        return features


    def read_raw_from_id(self, id):
        path = os.path.join(self._path, "saw_process_data.h5")
        try:
            hf = h5py.File(path, 'r')
        except:
            print("Failed to open file: " + str(path))
            exit()
        try:
            data_arr = np.array(hf[id])
        except:
            print("Failed to find dataset: " + str(id) + " in file " + str(path))
            exit()
        data_arr = np.array(data_arr[:-1])
        dataframes = []
        for i in range(len(data_arr)):
            data_df = pd.DataFrame(

                columns = [self.features[i], "time"], data = np.array([data_arr[i][0], data_arr[i][1]]).transpose(), index = [k for k in range(len(data_arr[i][0]))]
            )
            data_df.insert(0, "id", [id for i in range(len(data_arr[i][0]))])
            dataframes.append(data_df)
        return dataframes

    def get_processing_time(self, data):
        processing_time = (
            data.time.iloc[-1] - data.time.iloc[0]
        )
        process_end_ts = data.time.iloc[-1] *1e6
        return process_end_ts, processing_time
            

    def get_process_QH_id(self, id):
        dataframes = self.read_raw_from_id(id)
        process_end_ts, process_time = self.get_processing_time(dataframes[0])
        features_dataframe = "uninitialized"
        features_dataframe = self.extract_features(dataframes.pop(0))
        for dataframe in dataframes:
            features = self.extract_features(dataframe)
            features_dataframe = pd.concat([features_dataframe, features], axis=1)
        qh_document = {
            "pwd": self.pwd,
            "cid": self.cid,
            "qhd" : {
                "qhd-header": {
                    "owner": self.owner,
                    "subject": "part::cylinder_bottom,part_id::" + id + ",process::sawing,type::process_qh",
                    "timeref": datetime.datetime.fromtimestamp(process_end_ts/1e6).strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "model" : "None",
                    "asset" : "type::process_qh"
                },
                "qhd-body": {
            
                }
            }
        }
        qh_document["qhd"]["qhd-body"][self.process_name] = {
            "processing_time": process_time
        }
        qh_document["qhd"]["qhd-body"][self.process_name]["features"] = {
            "IND_" + feature: features_dataframe.loc[id, feature]
            for feature in features_dataframe.columns
        }
        return qh_document

    def get_data_QH_id(self, id, container_name):
        dataframes = self.read_raw_from_id(id)

        field = 13 # select data field here
        data = dataframes[field]

        # Reformat timestamps to iso8601 for the data quality analysis to work
        data.time = pd.to_datetime(data.time, unit="s").dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        # Save as .csv and copy into docker container
        csv_path = os.path.join(self.tmp_dir, "tmp_data.csv")
        data.to_csv(csv_path)
 
        container = self.docker_client.containers.get(container_name)
        copy_to_container(container, src=csv_path, dst_dir="/app/data/")               

        # Call the containers rest API with a rule
        query_params = {
            "file_name": "tmp_data.csv",
            "ts_column": "time",
            "value_column_1": self.features[field],
            "qhd_key": "interq_qhd"
        }
        response = requests.get(self.dqaas_endpoint, params=query_params)
        data_qh = json.loads(response.content)
        process_qh = self.get_process_QH_id(id)
        

        # take timestamp from process end. Otherwise timeref of data qh service would be time of processing, 
        # which we chose not to record here as it is not interesting
        data_qh["qhd"]["qhd-header"]["timeref"] = process_qh["qhd"]["qhd-header"]["timeref"]

        # reformatting for identification
        data_qh["qhd"]["qhd-header"]["subject"] = "part::cylinder_bottom,part_id::" + id + ",process::sawing,type::data_qh"
        data_qh["qhd"]["qhd-header"]["asset"] = "type::data_qh"
        data_qh["qhd"]["qhd-header"]["model"] = "None"

        # reformatting so that endpoint accepts it
        del data_qh["qhd"]["qhd-header"]["partID"]
        del data_qh["qhd"]["qhd-header"]["processID"]
        data_qh["pwd"] = self.pwd
        data_qh["cid"] = self.cid

        # add "_IND" to all atomic elements in qhd-body
        data_qh["qhd"]["qhd-body"] = self.reformatAtomicFields(data_qh["qhd"]["qhd-body"])
        return data_qh

    def publish_process_QH_id(self, id):
        qh_document = self.get_process_QH_id(id)
        response = requests.post(self.api_endpoint, json = qh_document)
        response = json.loads(response.content)
        return response

    def publish_data_QH_id(self, id, container_name):
        data_qh = self.get_data_QH_id(id, container_name)
        response = requests.post(self.api_endpoint, json = data_qh)
        response = json.loads(response.content)
        return response

    def reformatAtomicFields(self, document):
        for attribute, value in document.copy().items():
            if type(value) in [str, int, float, bool, list]:
                del document[attribute]
                document["IND_" + attribute] = value
            elif type(value) == dict:
                document[attribute] = self.reformatAtomicFields(document[attribute])
        return document

if __name__ == "__main__":
    pass
