import numpy as np
from pprint import pprint
import json
from interq_cip_qhs.process.utils import jprint
import os
import matplotlib.pyplot as plt
from interq_cip_qhs.process.sawing import SawingProcessData
from interq_cip_qhs.product.sawing import SawingProductData
from interq_cip_qhs.process.milling import MillingProcessData
from interq_cip_qhs.product.milling import MillingProductData
from interq_cip_qhs.process.turning import TurningProcessData
from interq_cip_qhs.product.turning import TurningProductData
from interq_cip_qhs.config import Config
import h5py
config = Config()

milling_quality_data_with_ts_path = "/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/interq_cip_qhs/src/interq_cip_qhs/notebooks/quality_data_cylinder_bottom.csv"
container_name = "hardcore_kilby"
reader = MillingProcessData()
#print(reader._part_id_paths.keys())

def get_cip_dmd_data(part_id):
    path = reader._part_id_paths[part_id]
    part_id, acc_data, bfc_data = reader.read_raw_from_folder(path)
    process_end_ts, process_times = reader.get_processing_times(acc_data)
    start = process_times["side_1_outer_contour_roughing_and_finishing"]
    end = process_times["side_1_stepped_bore"]
    acc_x = acc_data["acc_x"].to_numpy()
    time = acc_data["time"].to_numpy()
    return acc_x, time

def show_id(part_id, axs, pos, title):
    acc_x, time = get_cip_dmd_data(part_id)
    """    start_idx = np.searchsorted(time, time[0])
    end_idx = np.searchsorted(time, time[0] + 120 * 1e6)
    acc_x = acc_x[start_idx:end_idx]
    time = time[start_idx:end_idx]"""
    axs[pos].plot(time, acc_x)
    axs[pos].set_title(title)
    plt.ylim(-0.4, 0.4)


fig, axs = plt.subplots(2)

# anomalous: 
show_id("124404", axs, 0, "anomal part")
# nonanomalous
show_id("115102", axs, 1, "normal part")
fig.suptitle("Raw X-axis acceleration data")
plt.show()

# get data qh:
"""path_data = "/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/interq_cip_qhs/src/interq_cip_qhs/notebooks/"
reader = TurningProcessData(
    path_data = path_data
)
a = reader.get_data_QH_id("200902", container_name = container_name)
jprint(a)

path_to_missing = '/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/data/cip_dmd/cylinder_bottom/cnc_milling_machine/process_data/126101_04_24_2023_12_23_54/backside_external_sensor_signals.h5'
with h5py.File(path_to_missing, 'r') as hf:
    acc_x = hf["data"][:,1]
idxs = [i for i in range(len(acc_x))]
n_missing_values = 847
acc_x_with_missing = acc_x.copy()
# put missing values
miss_idxs = np.random.choice(idxs, n_missing_values)
for idx in miss_idxs:
    print("set NAN!")
    acc_x_with_missing[idx] = np.nan

with h5py.File(path_to_missing, 'r+') as hf:
    hf["data"][:,1] = acc_x_with_missing
    print("saved changed file!")"""


path_data = "/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/interq_cip_qhs/src/interq_cip_qhs/notebooks/"
reader = MillingProcessData()
a = reader.get_raw_data_QH_id("126101", container_name = container_name)
jprint(a)