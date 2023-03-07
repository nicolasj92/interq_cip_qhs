import json
import h5py
import os
import ciso8601
import numpy as np
import pandas as pd
from pathlib import Path
from tsfresh.feature_extraction import extract_features, MinimalFCParameters


class MillingProcessData:
    def __init__(self, path_data):
        self._path = path_data
        self._init_path_dict()
        self.processes = [
            "side_1_aussenkontur_schruppen_schlichten",
            "side_1_bohren",
            "side_1_bohren_seitlich",
            "side_1_bohren_senken",
            "side_1_endgraten_aussenkontur_bohrungen",
            "side_1_gewinde_fraesen",
            "side_1_nut_seitlich",
            "side_1_planfraesen",
            "side_1_stufenbohrung",
            "side_2_planfraesen",
            "side_2_kreistasche_fraesen",
            "side_2_bauteil_entgraten",
            "side_2_ringnut",
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
        side_1_acc = "part1.h5"
        side_2_acc = "part2.h5"
        side_1_ts = "part_1_timestamp_process_pairs.json"
        side_2_ts = "part_2_timestamp_process_pairs.json"
        side_1_bfc = "part_1_bfc_data.json"
        side_2_bfc = "part_2_bfc_data.json"

        part_id = os.path.basename(path).split("_")[0]

        side_1_acc = h5py.File(os.path.join(path, side_1_acc))
        side_1_acc_data = np.array(side_1_acc["0"])

        side_2_acc = h5py.File(os.path.join(path, side_2_acc))
        side_2_acc_data = np.array(side_2_acc["0"])

        with open(os.path.join(path, side_1_ts)) as f:
            side_1_ts_data = json.load(f)

        with open(os.path.join(path, side_2_ts)) as f:
            side_2_ts_data = json.load(f)

        with open(os.path.join(path, side_1_bfc)) as f:
            side_1_bfc_data = json.load(f)

        with open(os.path.join(path, side_2_bfc)) as f:
            side_2_bfc_data = json.load(f)

        acc_data_side_1 = self.read_raw_acc("side_1", side_1_acc_data, side_1_ts_data)
        acc_data_side_2 = self.read_raw_acc("side_2", side_2_acc_data, side_2_ts_data)
        acc_data = pd.concat([acc_data_side_1, acc_data_side_2])

        bfc_data_side_1 = self.read_raw_bfc("side_1", side_1_bfc_data, side_1_ts_data)
        bfc_data_side_2 = self.read_raw_bfc("side_2", side_2_bfc_data, side_2_ts_data)
        bfc_data = pd.concat([bfc_data_side_1, bfc_data_side_2])

        return part_id, acc_data, bfc_data

    def get_processing_times(self, acc_data):
        processing_times = {}
        for process_name in self.processes:
            process_data = acc_data[acc_data.id == process_name]
            processing_times[process_name] = (
                process_data.time.iloc[-1] - process_data.time.iloc[0]
            ) / 1e6
        return processing_times

    def get_process_QH_path(self, path):
        part_id, acc_data, bfc_data = self.read_raw_from_folder(path)
        process_times = self.get_processing_times(acc_data)
        acc_features = self.extract_acc_features(acc_data)
        bfc_features = self.extract_bfc_features(bfc_data)

        qh_document = {
            "part_id": part_id,
            "part": "cylinder_bottom",
            "process": "milling",
            "processes": {},
        }
        for process_name in self.processes:
            qh_document["processes"][process_name] = {
                "processing_time": process_times[process_name]
            }
            qh_document["processes"][process_name]["features_acc"] = {
                "IND_" + feature: acc_features.loc[process_name, feature]
                for feature in acc_features.columns
            }
            qh_document["processes"][process_name]["features_bfc"] = {
                "IND_" + feature: bfc_features.loc[process_name, feature]
                for feature in bfc_features.columns
            }

        return qh_document

    def get_process_QH_id(self, id):
        return self.get_process_QH_path(self._part_id_paths[id])


if __name__ == "__main__":
    pass
