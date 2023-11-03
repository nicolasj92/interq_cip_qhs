import numpy as np
import pprint
import json
from interq_cip_qhs.process.utils import jprint
import os
import csv
import matplotlib.pyplot as plt
from interq_cip_qhs.process.sawing import SawingProcessData
from interq_cip_qhs.product.sawing import SawingProductData
from interq_cip_qhs.process.milling import MillingProcessData
from interq_cip_qhs.product.milling import MillingProductData
from interq_cip_qhs.process.turning import TurningProcessData
from interq_cip_qhs.product.turning import TurningProductData
from interq_cip_qhs.config import Config
config = Config()
import random
import datetime


# can't use quality data from dataset because we need timestamp for quality hallmarks
milling_quality_data_with_ts_path = "/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/interq_cip_qhs/src/interq_cip_qhs/notebooks/quality_data_cylinder_bottom.csv"

labels_path = "/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/interq_cip_qhs/src/interq_cip_qhs/notebooks/anomalous_parts_detailed.csv"
all_anomal_keys = []
anomal_1_keys = []

with open(labels_path, 'r') as f:
    reader = csv.reader(f, delimiter = ';')
    init = False
    for row in reader:
        if not init:
            init = True
            continue
        
        all_anomal_keys.append(int(row[0]))
        if str(row[1]) == "1":
            anomal_1_keys.append(int(row[0]))

reader = MillingProcessData()
keys = list(map(int, reader._part_id_paths.keys()))
keys.sort()
keys = np.array(keys)

keys_normal = []
for key in keys:
    if key not in all_anomal_keys:
        keys_normal.append(key)


keys_anomal = anomal_1_keys[:10]
keys_normal = keys_normal[:45]

new_ids = []
initialized = False
# generate features
def generate_features(keys):
    global initialized, new_ids
    all_features = []
    for key in keys:
        feat = reader.get_process_acc_features(str(key))
        cols = feat.columns.to_list()
        rows = feat.index.to_list()
        features_flat = []
        if initialized == False:
            for col in cols:
                for row in rows:
                    new_ids.append((col, row, col + "_" + row))
            initialized = True
        for new_id in new_ids:
            features_flat.append(feat[new_id[0]][new_id[1]])
        all_features.append(features_flat)
    return np.array(all_features)

features_normal = generate_features(keys_normal)
features_anomal = generate_features(keys_anomal)

# refactor data
labels = np.zeros(len(features_normal) + len(features_anomal))
labels[0:len(features_normal)] = 1
print(labels)
data = [*features_normal, *features_anomal]
data = np.array(data)

# analyze 
cov_matrix = np.corrcoef(data.T, labels, rowvar = True)


feature_label_correlation = np.abs(cov_matrix[-1, :-1])
max_idx = np.argmax(feature_label_correlation)
print(new_ids[max_idx][2])

