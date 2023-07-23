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
turning_quality_data_with_ts_path = "/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/interq_cip_qhs/src/interq_cip_qhs/notebooks/quality_data_piston_rods.csv"
sawing_quality_data_with_ts_path = "/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/interq_cip_qhs/src/interq_cip_qhs/notebooks/quality_data_sawing.csv"

sawing_and_turning_process_data_path = "/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/interq_cip_qhs/src/interq_cip_qhs/notebooks/"
# sawing and turning process data expected as single .h5 files ("turning_process_data.h5", vice versa) with time progressing in first dimension


#reader = TurningProductData(turning_quality_data_with_ts_path)
#reader.publish_all_product_qh()

#reader = SawingProductData(sawing_quality_data_with_ts_path)
#reader.publish_all_product_qh()

#reader = MillingProductData(milling_quality_data_with_ts_path)
#reader.publish_all_product_qh()

#reader = TurningProcessData(sawing_and_turning_process_data_path)
#reader.publish_all_process_and_data_qh()

#reader = SawingProcessData(sawing_and_turning_process_data_path)
#reader.publish_all_process_and_data_qh()

reader = MillingProcessData()
reader.publish_all_process_and_data_qh()



