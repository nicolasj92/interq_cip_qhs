import pandas as pd
import datetime
import requests
import json
from interq_cip_qhs.config import Config
from interq_cip_qhs.process.utils import copy_to_container, jprint

config = Config()


class MillingProductData:
    def __init__(self, path_csv):
        quality_data = pd.read_csv(path_csv, delimiter=";", encoding="latin1")
        quality_data_en = pd.DataFrame(
            columns=[
                "id",
                "measurement_timestamp",
                "surface_roughness",
                "parallelism",
                "groove_depth",
                "groove_diameter",
            ],
            data={
                "id": quality_data.iloc[3:, 1],
                "measurement_timestamp": quality_data.iloc[3:, 2],
                "surface_roughness": quality_data.iloc[3:, 3].str.replace(",", "."),
                "parallelism": quality_data.iloc[3:, 4].str.replace(",", "."),
                "groove_depth": quality_data.iloc[3:, 5].str.replace(",", "."),
                "groove_diameter": quality_data.iloc[3:, 6].str.replace(",", "."),
            },
        )
        quality_data_en.set_index("id", inplace=True)
        self.quality_data = quality_data_en
        self.pwd = "interq"
        self.cid = "6LHWRqwyG1jGobMJMyUjsgsA5u52y37dtiu6bPSrXFX1"
        self.owner = "ptw"
        self.api_endpoint = "http://localhost:6005/interq/tf/v1.0/qhs"
        self.dqaas_endpoint = "http://localhost:8000/DuplicateRecords/"


    def get_product_QH_id(self, id):
        data = self.quality_data.loc[id]
        qh_document = {
            "pwd": self.pwd,
            "cid": self.cid,
            "qhd": {
                "qhd-header" : {
                    "owner": self.owner,
                    "subject": "part::cylinder_bottom,part_id::" + id + ",process::milling,type::product_qh",
                    "timeref": datetime.datetime.strptime(data["measurement_timestamp"], '%d.%m.%Y %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%SZ'),
                    "model" : "None",
                    "asset" : "type::product_qh"
                },
                "qhd-body": {
                    "IND_measurement_time": data["measurement_timestamp"],
                    "IND_surface_roughness": data["surface_roughness"],
                    "IND_parallelism": data["parallelism"],
                    "IND_groove_depth": data["groove_depth"],
                    "IND_groove_diameter": data["groove_diameter"]
                }
            }
        }
        return qh_document
        
    def publish_product_QH_id(self, id):
        qh_document = self.get_product_QH_id(id)
        print("publishing document: ")
        jprint(qh_document)
        response = requests.post(self.api_endpoint, json = qh_document)
        response = json.loads(response.content)
        print("got response: ")
        jprint(response)
        return response

    def publish_all_product_qh(self):
        for id, row in self.quality_data.iterrows():
            self.publish_product_QH_id(id)
