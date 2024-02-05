import pandas as pd


class EtfScraper:
    def __init__(self) -> None:
        pass

    def get_filtered_data(self, market_filter: str = "us_market"):

        path = f"D:\\etfs_data.csv"  # Change with your local path.
        df = pd.read_csv(path)
        filtered_df = df[df["market"] == market_filter]
        return filtered_df
