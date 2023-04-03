from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame, column_1: str, column_2: str) -> pd.DataFrame:
    """Fix Data Type issues"""
    df[column_1] = pd.to_datetime(df[column_1])
    df[column_2] = pd.to_datetime(df[column_2])
    print(df.head(2))
    print(f"Columns: {df.dtypes}")
    print(f"Rows: {len(df)}")
    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as a parquet file"""

    # Create a folder data/green in the working directory before running this code
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    # Checking to see if the slashes are forward. Default is backwards in windows
    print(path.as_posix())
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("de-zoomcamp-gcs")
    gcs_block.upload_from_path(from_path=path,
                               to_path=path.as_posix())  # Using as_posix() to convert the slashes to forward
    return


@flow()
def etl_web_to_gcs() -> None:
    """The Main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)

    # For the Green trip data
    # df_clean = clean(df, "lpep_pickup_datetime", "lpep_dropoff_datetime")

    # For the Yellow trip data
    df_clean = clean(df, "tpep_pickup_datetime", "tpep_dropoff_datetime")

    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)


if __name__ == '__main__':
    etl_web_to_gcs()