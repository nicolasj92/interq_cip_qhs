import pandas as pd
import datetime
import requests
import json
from interq_cip_qhs.process.utils import copy_to_container, jprint
from interq_cip_qhs.config import Config
config = Config()

class SawingProductData:
    def __init__(self, path_csv):
        quality_data = pd.read_csv(path_csv, delimiter=",", encoding="latin1")
        quality_data_en = pd.DataFrame(
            columns=[
                "id",
                "measurement_timestamp",
                "weight",
            ],
            data={
                "id": quality_data.iloc[:, 0],
                "measurement_timestamp": quality_data.iloc[:, 1],
                "weight": quality_data.iloc[:, 2],
            },
        )
        quality_data_en.set_index("id", inplace=True, drop=True)
        self.quality_data = quality_data_en
        self.pwd = config.pwd
        self.cid = config.cid
        self.model = config.model
        self.owner = "ptw"
        self.api_endpoint = "http://localhost:6005/interq/tf/v1.0/qhs"
        self.dqaas_endpoint = "http://localhost:8000/DuplicateRecords/"


    def get_product_QH_id(self, id):
        #print(self.quality_data)
        data = self.quality_data.loc[int(float(id))]
        qh_document = {
            "pwd": self.pwd,
            "cid": self.cid,
            "qhd": {
                "qhd-header" : {
                    "owner": self.owner,
                    "subject": "part::cylinder_bottom,part_id::" + str(id) + ",process::sawing,type::product_qh",
                    "timeref": datetime.datetime.strptime(data["measurement_timestamp"], '%d-%m-%Y %H:%M:%S').strftime('%Y-%m-%dT%H:%M:%S+01:00'),
                    "model" : self.model,
                    "asset" : "type::product_qh"
                },
                "qhd-body": {
                    "IND_weight": data["weight"],
                }
            }
        }
        return qh_document
        
    def publish_product_QH_id(self, id):
        qh_document = self.get_product_QH_id(id)
        print("publishing document: ")
        #jprint(qh_document)
        response = requests.post(self.api_endpoint, json = qh_document)
        response = json.loads(response.content)
        print("got response: ")
        jprint(response)
        return response

    def publish_all_product_qh(self):
        for id, row in self.quality_data.iterrows():
            self.publish_product_QH_id(id)
