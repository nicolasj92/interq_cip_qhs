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

reader = MillingProcessData(

    path_data = os.path.join(config.DATASET_PATH, "cylinder_bottom", "cnc_milling_machine" , "process_data")
)
qh = reader.get_process_QH_id("100101")
jprint(qh)
a = reader.publish_process_QH_id("100101")
jprint(a)
