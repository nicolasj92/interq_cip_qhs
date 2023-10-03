import numpy as np
import pprint
import json
from interq_cip_qhs.process.utils import jprint
import os

from interq_cip_qhs.process.sawing import SawingProcessData
from interq_cip_qhs.product.sawing import SawingProductData
from interq_cip_qhs.process.milling import MillingProcessData
from interq_cip_qhs.product.milling import MillingProductData
from interq_cip_qhs.process.turning import TurningProcessData
from interq_cip_qhs.product.turning import TurningProductData
from interq_cip_qhs.config import Config
config = Config()

# can't use quality data from dataset because we need timestamp for quality hallmarks
milling_quality_data_with_ts_path = "/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/interq_cip_qhs/src/interq_cip_qhs/notebooks/quality_data_cylinder_bottom.csv"



reader = MillingProcessData()
keys = list(map(int, reader._part_id_paths.keys()))
keys.sort()
keys = np.array(keys)
start_idx = np.searchsorted(keys, 111501)#124504
keys = keys[start_idx:start_idx + 50]
print(len(keys))
exit()
for key in keys:
    jprint(reader.publish_process_QH_id(str(key)))


