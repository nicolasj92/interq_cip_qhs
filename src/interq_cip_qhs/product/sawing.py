import pandas as pd


class SawingProductData:
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

    def get_product_QH_id(self, id):
        data = self.quality_data.loc[id]
        return {
            "part_id": id,
            "part": "cylinder_bottom",
            "process": "milling",
            "measurement_time": data["measurement_timestamp"],
            "surface_roughness": data["surface_roughness"],
            "parallelism": data["parallelism"],
            "groove_depth": data["groove_depth"],
            "groove_diameter": data["groove_diameter"],
        }
