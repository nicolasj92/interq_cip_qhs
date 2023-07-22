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
from interq_cip_qhs.config import Config
import csv
config = Config()

class MillingProcessData:
    def __init__(self):
        self.owner = "ptw"
        self.docker_client = docker.from_env()
        self.tmp_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../tmp_files"))
        self._path = os.path.join(config.DATASET_PATH, "cylinder_bottom", "cnc_milling_machine", "process_data")
        self._init_path_dict()
        self.cid = "6LHWRqwyG1jGobMJMyUjsgsA5u52y37dtiu6bPSrXFX1"
        self.pwd = "interq"
        self.api_endpoint = "http://localhost:6005/interq/tf/v1.0/qhs"
        self.dqaas_endpoint = "http://localhost:8000/DuplicateRecords/"
        self.processes = [
            "side_1_outer_contour_roughing_and_finishing",
            "side_1_drilling",
            "side_1_lateral_drilling",
            "side_1_drilling_countersinking",
            "side_1_outer_contour_deburring_holes",
            "side_1_thread_miling",
            "side_1_lateral_groove",
            "side_1_face_milling",
            "side_1_stepped_bore",
            "side_2_face_milling",
            "side_2_circular_pocket_milling",
            "side_2_component_deburring",
            "side_2_ring_groove",
        ]
        self.acc_features = ["acc_x", "acc_y", "acc_z"]
        self.bfc_features = [
            "aaCurr5",
            "aaCurr6",
            "aaTorque5",
            "aaTorque6",
            "aaPower5",
            "aaPower6",
            "actSpeed1",
            "actToolBasePos1",
            "actToolBasePos2",
            "actToolBasePos3",
            "actToolBasePos4",
            "actToolBasePos5",
            "actToolBasePos6",
            "cmdAngPos1",
            "measPos11",
            "measPos12",
            "measPos13",
            "measPos14",
            "measPos15",
            "measPos16",
            "measPos21",
            "measPos22",
            "measPos23",
            "actFeedRate1",
            "actFeedRate2",
            "actFeedRate3",
            "actFeedRate4",
            "actFeedRate5",
            "actFeedRate6",
            "aaLoad5",
            "aaLoad6",
        ]

    def _init_path_dict(self):
        p = Path(self._path)
        subdirectories = [x for x in p.iterdir() if x.is_dir()]
        self._part_id_paths = {
            os.path.basename(path).split("_")[0]: path for path in subdirectories
        }

    def get_sorted_timestamps_processes(self, ts_data):
        timestamps = np.array([float(key) * 1e6 for key in ts_data.keys()])
        processes = np.array([ts_data[key] for key in ts_data.keys()])
        sort_inds = np.argsort(timestamps)
        timestamps = timestamps[sort_inds]
        processes = processes[sort_inds]

        return timestamps, processes

    def extract_bfc_features(self, bfc_data):
        features = extract_features(
            bfc_data,
            column_sort="time",
            column_id="id",
            default_fc_parameters=MinimalFCParameters(),
        )
        return features

    def extract_acc_features(self, acc_data):
        features = extract_features(
            acc_data,
            column_sort="time",
            column_id="id",
            default_fc_parameters=MinimalFCParameters(),
        )
        return features

    def read_raw_acc(self, name, acc_data, ts_data):
        timestamps, processes = self.get_sorted_timestamps_processes(ts_data)
        data = pd.DataFrame(columns=["id", "time", *self.acc_features])

        for i in range(len(timestamps)):
            if i < len(timestamps) - 1:
                process_data = acc_data[
                    (
                        (acc_data[:, 0] >= timestamps[i])
                        & (acc_data[:, 0] < timestamps[i + 1])
                    )
                ]
            else:
                process_data = acc_data[acc_data[:, 0] >= timestamps[i]]

            process_data = pd.DataFrame(
                columns=["id", "time", *self.acc_features],
                data={
                    "id": [name + "_" + processes[i]] * len(process_data),
                    "time": process_data[:, 0],
                    "acc_x": process_data[:, 1],
                    "acc_y": process_data[:, 2],
                    "acc_z": process_data[:, 3],
                },
            )
            data = pd.concat([data, process_data])

        return data

    def new_read_raw_bfc(self, name, bfc_data, ts_data):
        timestamps, processes = self.get_sorted_timestamps_processes(ts_data)
        timestamps = timestamps/1e6
        data = pd.DataFrame(columns=["id", "time", *self.bfc_features])
        
        for i in range(len(timestamps)):
            if len(bfc_data[
                
                        (bfc_data[:,0] >= timestamps[i])
                        
                ]) == 0:
                print("WARNING")
            if i < len(timestamps) - 1:
                bfc_process_data = bfc_data[
                    (
                        (bfc_data[:,0] >= timestamps[i])
                        & (bfc_data[:,0] < timestamps[i + 1])
                    )
                ]
            else:
                bfc_process_data = bfc_data[bfc_data[:,0] >= timestamps[i]]

            process_data = pd.DataFrame(
                columns=["id", "time"],
                data={
                    "id": [name + "_" + processes[i]] * len(bfc_process_data[:,0]),
                    "time": bfc_process_data[:,0]
                },
            )
            for feature_idx, feature in enumerate(self.bfc_features):
                process_data[feature] = bfc_process_data[:,feature_idx + 1]
                
            data = pd.concat([data, process_data])
        return data

    def read_raw_bfc(self, name, bfc_data, ts_data):
        timestamps, processes = self.get_sorted_timestamps_processes(ts_data)

        process_data = {key: [] for key in self.bfc_features}
        process_data["id"] = []
        process_data["time"] = []

        for bfc_message in bfc_data:
            ts = ciso8601.parse_datetime(bfc_message["set"]["timestamp"])
            ts = ts.timestamp() * 1e6
            process_data["time"].append(ts)

            for i in range(len(timestamps)):
                if i < len(timestamps) - 1:
                    if (ts >= timestamps[i]) & (ts < timestamps[i + 1]):
                        process_data["id"].append(name + "_" + processes[i])
                        break
                else:
                    process_data["id"].append(name + "_" + processes[i])

            datapoints = bfc_message["set"]["datapoints"]
            for datapoint in datapoints:
                for feature_name in self.bfc_features:
                    if feature_name == datapoint["name"]:
                        process_data[feature_name].append(datapoint["value"])

        data = pd.DataFrame(
            columns=["id", "time", *self.bfc_features], data=process_data
        )
        return data

    def read_raw_from_folder(self, path):
        side_1_acc = "frontside_external_sensor_signals.h5"
        side_2_acc = "backside_external_sensor_signals.h5"
        side_1_ts = "frontside_timestamp_process_pairs.csv"
        side_2_ts = "backside_timestamp_process_pairs.csv"
        side_1_bfc = "frontside_internal_machine_signals.h5"
        side_2_bfc = "backside_internal_machine_signals.h5"

        part_id = os.path.basename(path).split("_")[0]

        side_1_acc = h5py.File(os.path.join(path, side_1_acc))
        side_1_acc_data = np.array(side_1_acc["data"])

        side_2_acc = h5py.File(os.path.join(path, side_2_acc))
        side_2_acc_data = np.array(side_2_acc["data"])

        side_1_bfc = h5py.File(os.path.join(path, side_1_bfc))
        side_1_bfc_data = np.array(side_1_bfc["data"])

        side_2_bfc = h5py.File(os.path.join(path, side_2_bfc))
        side_2_bfc_data = np.array(side_2_bfc["data"])

        side_1_ts_data = {}
        with open(os.path.join(path, side_1_ts), newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in reader:
                side_1_ts_data[row[0]] = row[1]
        
        side_2_ts_data = {}
        with open(os.path.join(path, side_2_ts), newline='') as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='|')
            for row in reader:
                side_2_ts_data[row[0]] = row[1]
        """with open(os.path.join(path, side_1_ts)) as f:
            side_1_ts_data = json.load(f)

        with open(os.path.join(path, side_2_ts)) as f:
            side_2_ts_data = json.load(f)

        with open(os.path.join(path, side_1_bfc)) as f:
            side_1_bfc_data = json.load(f)

        with open(os.path.join(path, side_2_bfc)) as f:
            side_2_bfc_data = json.load(f)"""

        acc_data_side_1 = self.read_raw_acc("side_1", side_1_acc_data, side_1_ts_data)
        acc_data_side_2 = self.read_raw_acc("side_2", side_2_acc_data, side_2_ts_data)
        acc_data = pd.concat([acc_data_side_1, acc_data_side_2])

        bfc_data_side_1 = self.new_read_raw_bfc("side_1", side_1_bfc_data, side_1_ts_data)
        bfc_data_side_2 = self.new_read_raw_bfc("side_2", side_2_bfc_data, side_2_ts_data)
        bfc_data = pd.concat([bfc_data_side_1, bfc_data_side_2])

        return part_id, acc_data, bfc_data

    def get_processing_times(self, acc_data):
        processing_times = {}
        for process_name in self.processes:
            process_data = acc_data[acc_data.id == process_name]
            processing_times[process_name] = (
                process_data.time.iloc[-1] - process_data.time.iloc[0]
            ) / 1e6
            process_end_ts = process_data.time.iloc[-1]
        return process_end_ts, processing_times

    def get_process_QH_path(self, path):
        part_id, acc_data, bfc_data = self.read_raw_from_folder(path)

        process_end_ts, process_times = self.get_processing_times(acc_data)
        acc_features = self.extract_acc_features(acc_data)
        bfc_features = self.extract_bfc_features(bfc_data)

        acc_features.index = pd.Categorical(acc_features.index, categories=acc_data.id.unique(), ordered=True)
        bfc_features.index = pd.Categorical(bfc_features.index, categories=acc_data.id.unique(), ordered=True)
        acc_features = acc_features.sort_index()
        bfc_features = bfc_features.sort_index()

        qh_document = {
            "pwd": self.pwd,
            "cid": self.cid,
            "qhd": {
                "qhd-header": {
                    "owner": self.owner,
                    "subject": "part::cylinder_bottom,part_id::" + part_id + ",process::milling,type::process_qh",
                    "timeref": datetime.datetime.fromtimestamp(
                        process_end_ts / 1e6
                    ).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "model": "None",
                    "asset": "type::process_qh",
                },
                "qhd-body": {},
            },
        }
        for process_name in self.processes:
            qh_document["qhd"]["qhd-body"][process_name] = {
                "processing_time": process_times[process_name]
            }
            qh_document["qhd"]["qhd-body"][process_name]["features_acc"] = {
                "IND_" + feature: acc_features.loc[process_name, feature]
                for feature in acc_features.columns
            }
            qh_document["qhd"]["qhd-body"][process_name]["features_bfc"] = {
                "IND_" + feature: bfc_features.loc[process_name, feature]
                for feature in bfc_features.columns
            }

        return qh_document

    def get_process_QH_id(self, id):
        return self.get_process_QH_path(self._part_id_paths[id])

    def get_data_QH_path(self, path, container_name):
        part_id, acc_data, bfc_data = self.read_raw_from_folder(path)

        # Reformat timestamps to iso8601 for the data quality analysis to work
        acc_data.time = pd.to_datetime(acc_data.time / 1e6, unit="s").dt.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        bfc_data.time = pd.to_datetime(bfc_data.time / 1e6, unit="s").dt.strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        # Save as .csv and copy into docker container
        acc_path = os.path.join(self.tmp_dir, "tmp_acc_data.csv")
        bfc_path = os.path.join(self.tmp_dir, "tmp_bfc_data.csv")
        acc_data.to_csv(acc_path)
        bfc_data.to_csv(bfc_path)

        container = self.docker_client.containers.get(container_name)
        copy_to_container(container, src=acc_path, dst_dir="/app/data/")
        copy_to_container(container, src=bfc_path, dst_dir="/app/data/")

        # Call the containers rest API with a rule
        query_params = {
            "file_name": "tmp_acc_data.csv",
            "ts_column": "time",
            "value_column_1": "acc_x",
            "qhd_key": "interq_qhd",
        }
        response = requests.get(self.dqaas_endpoint, params=query_params)
        response = json.loads(response.content)
        return response

    def get_data_QH_id(self, id, container_name):
        data_qh = self.get_data_QH_path(self._part_id_paths[id], container_name)
        process_qh = self.get_process_QH_id(id)
        
        # take timestamp from process end. Otherwise timeref of data qh service would be time of processing, 
        # which we chose not to record here as it is not interesting
        data_qh["qhd"]["qhd-header"]["timeref"] = process_qh["qhd"]["qhd-header"][
            "timeref"
        ]

        # reformatting for identification
        data_qh["qhd"]["qhd-header"]["subject"] = "part::cylinder_bottom,part_id::" + id + ",process::milling,type::data_qh"
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
        print("publishing document:")
        jprint(qh_document)
        response = requests.post(self.api_endpoint, json=qh_document)
        response = json.loads(response.content)
        print("got response: ")
        return response

    def publish_data_QH_id(self, id, container_name):
        data_qh = self.get_data_QH_id(id, container_name)
        print("publishing document:")
        jprint(data_qh)
        response = requests.post(self.api_endpoint, json = data_qh)
        response = json.loads(response.content)
        print("got response: ")
        return response

    def reformatAtomicFields(self, document):
        for attribute, value in document.copy().items():
            if type(value) in [str, int, float, bool, list]:
                del document[attribute]
                document["IND_" + attribute] = value
            elif type(value) == dict:
                document[attribute] = self.reformatAtomicFields(document[attribute])
        return document

    def publish_all_process_and_data_qh(self):
        for id in self._part_id_paths:
            self.publish_process_QH_id(id)
            self.publish_data_QH_id(id)


if __name__ == "__main__":
    pass
