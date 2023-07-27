import pandas as pd
import datetime
import requests
import json
from interq_cip_qhs.process.utils import copy_to_container, jprint
from interq_cip_qhs.config import Config
config = Config()

class TurningProductData:
    def __init__(self, path_csv):
        quality_data = pd.read_csv(path_csv, delimiter=";", encoding="latin1")
        quality_data_en = pd.DataFrame(
            columns=[
                "id",
                "coaxiality",
                "diameter",
                "length",
            ],
            data={
                "id": quality_data.iloc[7:, 1],
                "coaxiality": quality_data.iloc[7:, 2].str.replace(",", "."),
                "diameter": quality_data.iloc[7:, 4].str.replace(",", "."),
                "length": quality_data.iloc[7:, 6].str.replace(",", "."),
            },
        )
        quality_data_en.set_index("id", inplace=True)
        self.quality_data = quality_data_en
        self.pwd = config.pwd
        self.cid = config.cid
        self.model = config.model
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
                    "subject": "part::piston_rod,part_id::" +  id + ",process::turning,type::product_qh",
                    # randomly picked timeref cuz we don't have none
                    "timeref": "2022-08-16T09:10:26+01:00",
                    "model" : self.model,
                    "asset" : "type::product_qh"
                },
                "qhd-body": {
                    "IND_coaxiality": data["coaxiality"],
                    "IND_diameter": data["diameter"],
                    "IND_length": data["length"],
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
