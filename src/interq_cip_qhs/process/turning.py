import json
import h5py
import docker
import os
import ciso8601
import datetime
import numpy as np
import pandas as pd
import requests
import math
from pathlib import Path
from tsfresh.feature_extraction import extract_features, MinimalFCParameters
from interq_cip_qhs.process.utils import copy_to_container, jprint


class TurningProcessData:
    def __init__(self, path_data):
        self.owner = "ptw"
        #self.docker_client = docker.from_env()
        self.tmp_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../tmp_files"))
        self._path = path_data
        self.process_name = "turning"
        self.features = [
            "time",
            "NCLine",
            "ProgramName",
            "ProgramStatus",
            "SpindleRPM",
            "TimeSinceStartup",
            "aaCurr1",
            "aaCurr13",
            "aaCurr14",
            "aaCurr16",
            "aaCurr2",
            "aaCurr3",
            "aaCurr4",
            "aaCurr5",
            "aaCurr6",
            "aaCurr7",
            "aaCurr8",
            "aaCurr9",
            "aaLoad1",
            "aaLoad13",
            "aaLoad14",
            "aaLoad16",
            "aaLoad2",
            "aaLoad3",
            "aaLoad4",
            "aaLoad5",
            "aaLoad6",
            "aaLoad7",
            "aaLoad8",
            "aaLoad9",
            "aaPower1",
            "aaPower13",
            "aaPower14",
            "aaPower16",
            "aaPower2",
            "aaPower20",
            "aaPower3",
            "aaPower4",
            "aaPower5",
            "aaPower6",
            "aaPower7",
            "aaPower8",
            "aaPower9",
            "aaTorque1",
            "aaTorque13",
            "aaTorque14",
            "aaTorque16",
            "aaTorque2",
            "aaTorque3",
            "aaTorque4",
            "aaTorque5",
            "aaTorque6",
            "aaTorque7",
            "aaTorque8",
            "aaTorque9",
            "actFeedRate1",
            "actFeedRate13",
            "actFeedRate14",
            "actFeedRate16",
            "actFeedRate2",
            "actFeedRate3",
            "actFeedRate5",
            "actFeedRate6",
            "actFeedRate7",
            "actFeedRate8",
            "actFeedRate9",
            "actSpeed1",
            "actSpeed2",
            "actSpeed3",
            "actToolBasePos1",
            "actToolBasePos3",
            "measPos11",
            "measPos113",
            "measPos114",
            "measPos116",
            "measPos12",
            "measPos13",
            "measPos15",
            "measPos16",
            "measPos17",
            "measPos18",
            "measPos19",
            "measPos213",
            "measPos214",
            "measPos216",
            "measPos22",
            "measPos23",
            "measPos25",
            "measPos28",
            "measPos29",
        ]
    def get_sorted_process_data(self, data):
        timestamps = data[0]
        process_data = data[1:]
        sort_inds = np.argsort(timestamps)
        timestamps = timestamps[sort_inds]
        for i in range(len(process_data)):
            process_data[i] = process_data[i][sort_inds]
        return np.vstack([timestamps, process_data])

    def extract_features(self, data):
        features = extract_features(
            data,
            column_sort="time",
            column_id="id",
            default_fc_parameters=MinimalFCParameters(),
        )
        return features

    def read_raw_from_id(self, id):
        path = os.path.join(self._path, "turning_process_data.h5")
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
        # last field is just made up of NaN Values and field key is also unknown
        # TODO: find out why that is

        data_arr = self.get_sorted_process_data(data_arr)
        data_arr = data_arr[:-1]
        data_df = pd.DataFrame(
            columns = self.features, data = data_arr.transpose(), index = [i for i in range(len(data_arr[0]))]
        )
        data_df.insert(0, "id", [id for i in range(len(data_arr[0]))])
        return data_df

    def get_processing_time(self, data):
        processing_time = (
            data.time.iloc[-1] - data.time.iloc[0]
        )
        process_end_ts = data.time.iloc[-1] *1e6
        return process_end_ts, processing_time

    def get_process_QH_id(self, id, cid="6LHWRqwyG1jGobMJMyUjsgsA5u52y37dtiu6bPSrXFX1", pwd="interq"):
        data = self.read_raw_from_id(id)
        process_end_ts, process_time = self.get_processing_time(data)
        features = self.extract_features(data)
        qh_document = {
            "pwd": pwd,
            "cid": cid,
            "qhd-header": {
                "owner": self.owner,
                "subject": f"part::piston_rod,part_id::{id},process::turning",
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
            for feature in features.columns
        }
        return qh_document

if __name__ == "__main__":
    pass
