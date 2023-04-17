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
        #self.docker_client = docker.from_env()
        self.tmp_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../tmp_files"))
        self._path = path_data
        self.process_name = "cutting"
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
        print(len(data_arr))
        print(len(self.features))
        print("do feature size fit?")
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
            

    def get_process_QH_id(self, id, cid="6LHWRqwyG1jGobMJMyUjsgsA5u52y37dtiu6bPSrXFX1", pwd="interq"):
        dataframes = self.read_raw_from_id(id)
        process_end_ts, process_time = self.get_processing_time(dataframes[0])
        features_dataframe = "uninitialized"
        features_dataframe = self.extract_features(dataframes.pop(0))
        for dataframe in dataframes:
            features = self.extract_features(dataframe)
            features_dataframe = pd.concat([features_dataframe, features], axis=1)
        print(features_dataframe)
        qh_document = {
            "pwd": pwd,
            "cid": cid,
            "qhd-header": {
                "owner": self.owner,
                "subject": f"part::piston_rod,part_id::{id},process::sawing",
                "timeref": datetime.datetime.fromtimestamp(process_end_ts/1e6).isoformat(),
            },
            "qhd-body": {
            
            }
        }
        qh_document["qhd-body"][self.process_name] = {
            "processing_time": process_time
        }
        qh_document["qhd-body"][self.process_name]["features"] = {
            "IND_" + feature: features.loc[id, feature]
            for feature in features_dataframe.columns
        }
        return qh_document

if __name__ == "__main__":
    pass
