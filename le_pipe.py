import dlt
import pandas as pd
from dlt.common.utils import uniq_id

DISASTERS_URL = (
    "https://raw.githubusercontent.com/owid/owid-datasets/master/datasets/"
    "Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020)/"
    "Natural%20disasters%20from%201900%20to%202019%20-%20EMDAT%20(2020).csv"
)

if __name__ == "__main__":

    @dlt.destination(batch_size=100, loader_file_format="parquet")
    def my_destination(items, table) -> None:
        data_frame = pd.DataFrame.from_records(items)
        data_frame.to_csv(f'./{table["name"]}_{uniq_id()}.csv', index = False)

    @dlt.resource(table_name="natural_disasters")
    def resource():
        df = pd.read_csv(DISASTERS_URL)
        yield df.to_dict(orient="records")
    
    dlt.pipeline(pipeline_name="le_pipe", destination=my_destination).run(resource())
