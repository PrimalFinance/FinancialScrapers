# Operating system imports
import os
import sys


# Time and date
import datetime as dt

# Yahoo finance imports
import yfinance as yf

cwd = os.getcwd()
path = os.path.join(cwd, "FinancialScrapers\\Scrapers")
sys.path.append(path)

# Now you can use relative imports
from FinancialScrapers.Scrapers.macro_scraper import MacroScraper
from FinancialScrapers.Scrapers.equity_scraper import EquityScraper

# Pandas
import pandas as pd
from pandas.errors import EmptyDataError
import random


# Hardcoded folder paths
cwd = os.getcwd()
commodity_data_folder = f"{cwd}\\Database\\CommoditiesData"
equity_data_folder = f"{cwd}\\Database\\EquityData"
macro_data_folder = f"{cwd}\\Database\\MacroData"

# Hardcoded sub-folder paths
earnings_folder = f"{equity_data_folder}\\Earnings"
filings_folder = f"{equity_data_folder}\\Filings"
stocks_folder = f"{equity_data_folder}\\Stocks"


class DataManager:
    def __init__(
        self, base_data_path: str, chrome_driver_path: str, log_data=True
    ) -> None:
        self.base_path = base_data_path
        self.commodities_folder = os.path.join(self.base_path, "CommoditiesData")
        self.equities_folder = os.path.join(self.base_path, "EquityData")
        self.macro_folder = os.path.join(self.base_path, "MacroData")
        self.log_data = log_data
        self.macro_scraper = MacroScraper()
        self.equity_scraper = EquityScraper(chrome_driver_path)
        self.expired = 180

    ##################################################################### Equity Price Fetching #####################################################################
    def fetch_externally(self, ticker: str, period="max", interval="1d"):
        df = yf.download(ticker, period=period, interval=interval)
        return df

    def get_data(self, ticker: str, crypto: bool = False, force_update: bool = False):
        ticker = ticker.upper()
        ticker_file = os.path.join(
            self.equities_folder,
            f"Stocks\\{ticker}\\{ticker}_prices.csv",
        )
        # Force new data to be written locally.
        if force_update:
            df = self.fetch_externally(ticker)
            df["Close_Pct_Change"] = df["Adj Close"].pct_change() * 100
            df = self.calc_rsi(df)
            df = self.calc_macd(df)
        else:
            # Try to read data locally.
            try:
                df = pd.read_csv(ticker_file)
                df = df.set_index("Date")
                latest_date = df.index[-1]
                outdated = self.is_outdated(latest_date, day_threshold=5)
                # If the local data is outdated.
                if outdated:
                    df = self.fetch_externally(ticker)
                    df.to_csv(ticker_file)  # Save merged data locally.
            # Local data not found.
            except FileNotFoundError:
                df = self.fetch_externally(ticker)
                df["Close_Pct_Change"] = df["Adj Close"].pct_change() * 100
                df = self.calc_rsi(df)
                df = self.calc_macd(df)
                df.to_csv(ticker_file)  # Save locally
        return df

    def get_ticker_list(self, num_tickers: int = 500) -> list:
        path = f"{self.equities_folder}\\Stocks"
        # Get a list of folder names in the specified directory
        folder_names = [
            folder
            for folder in os.listdir(path)
            if os.path.isdir(os.path.join(path, folder))
        ]
        tickers = []
        for folder_name in folder_names:
            tickers.append(folder_name)

        return tickers

    ##################################################################### Equity Earnings Fetching #####################################################################
    def get_earnings(self, ticker: str, frequency: str = "q", expired: int = 90):
        # Path to earnings csv file for the ticker specified.
        earnings_file_path = f"{self.equities_folder}\\Stocks\\{ticker.upper()}\\{ticker.upper()}_earnings.csv"

        print(f"Earnings: {earnings_file_path}")

        # Logic to handle csv reading.
        try:

            earnings_csv_data = pd.read_csv(earnings_file_path)
            # Get the most recent reporting date.
            most_recent_reporting_date = earnings_csv_data["reportedDate"].iloc[0]
            date_difference = self.equity_scraper.get_date_difference(
                target_date=most_recent_reporting_date,
                compare_date=str(dt.datetime.now().date()),
            )

            # If date_difference is greater, fetch new data and write the new rows to the csv file.
            if date_difference > expired:
                # Fetch new earnings data.
                earnings = self.equity_scraper.get_earnings_estimates(
                    frequency=frequency
                )
                # Merge the dataframe from the csv file, and the new dataframe from the earnings file.
                merged_df = pd.concat([earnings_csv_data, earnings], ignore_index=True)
                merged_df = merged_df.drop_duplicates()
                merged_df.to_csv(earnings_file_path, header=True, index=False)
                return merged_df
            else:
                return earnings_csv_data
        except FileNotFoundError as e:
            print(f"[Error] {e}")
            # If the file is not found, fetch data and write to csv file.
            earnings = self.equity_scraper.get_earnings_estimates(
                ticker=ticker, frequency=frequency
            )
            earnings.to_csv(earnings_file_path, header=True, index=False)
            return earnings

    ##################################################################### Filing Dates #####################################################################
    def get_filing_dates(self, ticker: str):
        """
        ticker: Ticker of a company.

        Takes a ticker as a string. Searches a csv file names "quarterly_filings"
        """
        try:
            ticker = ticker.upper()
            file_path = f"{self.equities_folder}\\Filings\\quarterly_filings.csv"
            csv_file = pd.read_csv(file_path)

            # Check if the ticker is in the csv file.
            ticker_found = csv_file[csv_file["ticker"] == ticker]

            if ticker_found.empty:

                # Get the quarterly filings for the income statement.
                fiscal_dates = self.equity_scraper.get_fiscal_dates(ticker)
                # Get the dates of the last 4 quarters for the company.
                last_4_quarters = fiscal_dates[:4].to_list()[::-1]
                # income_statement_cols = income_statement.columns.to_list()

                # Get the fiscal year end for the company.
                fiscal_end = self.equity_scraper.get_fiscal_year_end_date(ticker)
                # Get the organized quarters.
                organized_quarters = self.equity_scraper.organize_quarters(
                    ticker, last_4_quarters, fiscal_end=fiscal_end
                )
                organized_quarters = [organized_quarters]
                # Turn the dictionary into a list. The only element should be this dictionary.
                # Update csv dataframe with new values.
                csv_file = csv_file.from_records(organized_quarters)

                csv_file.to_csv(file_path, mode="a", header=False, index=False)
                ticker_found = csv_file[csv_file["ticker"] == ticker]

        except EmptyDataError:
            csv_file = pd.DataFrame()
            # Get the quarterly filings for the income statement.
            fiscal_dates = self.equity_scraper.get_fiscal_dates(ticker)
            # Get the dates of the last 4 quarters for the company.
            last_4_quarters = fiscal_dates[:4].to_list()[::-1]
            # income_statement_cols = income_statement.columns.to_list()

            # Get the fiscal year end for the company.
            fiscal_end = self.equity_scraper.get_fiscal_year_end_date(ticker)
            # Get the organized quarters.
            organized_quarters = self.equity_scraper.organize_quarters(
                ticker, last_4_quarters, fiscal_end=fiscal_end
            )
            organized_quarters = [organized_quarters]
            # Turn the dictionary into a list. The only element should be this dictionary.
            # Update csv dataframe with new values.
            csv_file = csv_file.from_records(organized_quarters)

            csv_file.to_csv(file_path, header=True, index=False)
            ticker_found = csv_file[csv_file["ticker"] == ticker]

        return ticker_found

    ##################################################################### TA Calculations #####################################################################
    def calc_rsi(self, df: pd.DataFrame, rsi_period: int = 14) -> pd.DataFrame:
        # Calculate daily price changes
        df["Price Change"] = df["Close"].diff()

        # Calculate average gains and losses
        df["Gain"] = df["Price Change"].apply(lambda x: x if x > 0 else 0)
        df["Loss"] = df["Price Change"].apply(lambda x: abs(x) if x < 0 else 0)

        # Calculate average gains and average losses over the specified period
        avg_gain = df["Gain"].rolling(window=rsi_period).mean()
        avg_loss = df["Loss"].rolling(window=rsi_period).mean()

        # Calculate relative strength (RS)
        rs = avg_gain / avg_loss

        # Calculate RSI
        df["RSI"] = 100 - (100 / (1 + rs))

        # Drop intermediate columns used for calculations
        df = df.drop(["Gain", "Loss"], axis=1)
        return df

    def calc_macd(
        self,
        df: pd.DataFrame,
        fast_ma_period: int = 12,
        slow_ma_period: int = 26,
        signal_period: int = 9,
    ) -> pd.DataFrame:
        # Calculate 12-day EMA
        df["EMA_12"] = df["Close"].ewm(span=fast_ma_period, adjust=False).mean()

        # Calculate 26-day EMA
        df["EMA_26"] = df["Close"].ewm(span=slow_ma_period, adjust=False).mean()

        # Calculate MACD line
        df["MACD"] = df["EMA_12"] - df["EMA_26"]

        # Calculate Signal line (typically a 9-day EMA of the MACD line)
        df["Signal_Line"] = df["MACD"].ewm(span=signal_period, adjust=False).mean()

        # Calculate MACD Histogram (the difference between the MACD and Signal line)
        df["MACD_Histogram"] = df["MACD"] - df["Signal_Line"]

        # Drop intermediate columns used for calculations
        df = df.drop(["EMA_12", "EMA_26"], axis=1)
        return df

    def calc_fib_retracement(self, df: pd.DataFrame) -> pd.DataFrame:
        df["Retracement_38.2"] = df["High"] - 0.382 * (df["High"] - df["Low"])
        df["Retracement_50"] = df["High"] - 0.5 * (df["High"] - df["Low"])
        df["Retracement_61.8"] = df["High"] - 0.618 * (df["High"] - df["Low"])

        return df

    ##################################################################### DataFrame Sampling #####################################################################
    def get_sample_size(
        self, df: pd.DataFrame, sample_size: int = 300, start: int = 0, end: int = 0
    ):
        total_periods = len(df) / sample_size
        indexes = df.index
        # Length of df is less than sample size.
        if total_periods < 1:
            sample_size = int(len(df) * 0.75)

        if start == 0 and end == 0:
            try:
                rng = random.randint(indexes[sample_size], indexes[-1])
            except:
                df.reset_index(inplace=True)
                rng = random.randint(df.index[sample_size], df.index[-1])

            end = rng
            start = rng - sample_size

            sampled_df = df.iloc[start:end].reset_index(drop=True)
        # Custom sample parameters.
        else:
            try:
                sampled_df = df.iloc[start:end].reset_index(drop=True)
            # If indexes are still dates, reset them, *but* do not drop the date column!
            except:
                df.reset_index(inplace=True)
                sampled_df = df.iloc[start:end].reset_index(drop=True)
        return sampled_df

    ##################################################################### Macro Data #####################################################################
    def get_cpi(self) -> pd.DataFrame:
        file_path = f"{self.macro_folder}\\CPI\\cpi.csv"
        data = pd.read_csv(file_path)
        # Update csv if outdated.
        if self.is_outdated(data["Date"].iloc[0]):
            self.macro_scraper.update_cpi(path_to_update=file_path)
            data = pd.read_csv(file_path)  # Read again after updating
        return data

    def get_fed_funds(self) -> pd.DataFrame:
        file_path = f"{self.macro_folder}\\FedFunds\\fed_funds.csv"
        data = pd.read_csv(file_path)
        # Update csv if outdated.
        if self.is_outdated(data["Date"].iloc[0]):
            self.macro_scraper.update_fed_funds(path_to_update=file_path)
            data = pd.read_csv(file_path)  # Read again after updating
        return data

    def get_fed_funds(self) -> pd.DataFrame:
        file_path = f"{self.macro_folder}\\Treasury_Yield_Spread_10Y_2Y\\Treasury_Yield_Spread_10Y_2Y.csv"
        print(f"File: {file_path}")
        data = pd.read_csv(file_path)
        # Update csv if outdated.
        if self.is_outdated(data["Date"].iloc[0]):
            self.macro_scraper.update_treasury_yield_spread(path_to_update=file_path)
            data = pd.read_csv(file_path)  # Read again after updating
        return data

    ##################################################################### Financial Statements #####################################################################
    def get_income_statement(
        self,
        ticker: str,
        freq: str = "q",
        force_update: bool = False,
        write_data: bool = True,
    ):
        if freq == "q":
            freq = "Quarter"
        elif freq == "a":
            freq = "Annual"
        file_path = f"{self.equities_folder}\\Stocks\\{ticker.upper()}\\Statements\\{freq}\\{ticker.upper()}_income_statement.csv"
        # Force new data to be written locally regardless of data's staleness.
        if force_update:
            try:
                data = pd.read_csv(file_path)
                try:
                    data.set_index("Unnamed: 0", inplace=True)
                    data.index.rename("index", inplace=True)
                except KeyError:
                    pass
                new_data = self.equity_scraper.get_income_statement(ticker, freq)
                result_data = pd.concat([data, new_data], axis=1, join="inner")
                if write_data:
                    result_data.to_csv(file_path)
                return result_data
            except FileNotFoundError:
                data = self.equity_scraper.get_income_statement(ticker, freq)
                if write_data:
                    data.to_csv(file_path)
                return data
        # If no force update, then get the statement regularly.
        else:
            try:
                data = pd.read_csv(file_path)
                try:
                    data.set_index("Unnamed: 0", inplace=True)
                    data.index.rename("index", inplace=True)
                except KeyError:
                    pass
                most_recent_filing = data.columns[-1]
                if self.is_outdated(
                    most_recent_filing, self.expired
                ):  # Check if most recent filing is outdated.
                    new_data = self.equity_scraper.get_income_statement(ticker, freq)
                    result_data = pd.concat([data, new_data], axis=1, join="inner")
                    if write_data:
                        result_data.to_csv(file_path)
                    return result_data
                else:
                    return data

            except FileNotFoundError:
                data = self.equity_scraper.get_income_statement(ticker.upper(), freq)
                try:
                    if write_data:
                        data.to_csv(file_path)
                except OSError:
                    folder_path = f"{self.equities_folder}\\Stocks\\{ticker.upper()}\\Statements\\{freq}"
                    os.makedirs(folder_path, exist_ok=True)
                    if write_data:
                        data.to_csv(file_path)
                return data

    def get_balance_sheet(
        self,
        ticker: str,
        freq: str = "q",
        force_update: bool = False,
        write_data: bool = True,
    ):
        if freq == "q":
            freq = "Quarter"
        elif freq == "a":
            freq = "Annual"
        file_path = f"{self.equities_folder}\\Stocks\\{ticker.upper()}\\Statements\\{freq}\\{ticker.upper()}_balance_sheet.csv"
        if force_update:
            try:
                data = pd.read_csv(file_path)
                try:
                    data.set_index("Unnamed: 0", inplace=True)
                    data.index.rename("index", inplace=True)
                except KeyError:
                    pass
                new_data = self.equity_scraper.get_balance_sheet(ticker, freq)
                result_data = pd.concat([data, new_data], axis=1, join="inner")
                if write_data:
                    result_data.to_csv(file_path)
                return result_data
            except FileNotFoundError:
                data = self.equity_scraper.get_balance_sheet(ticker, freq)
                if write_data:
                    data.to_csv(file_path)
                return data
        # If no force update, then get the statement regularly.
        else:
            try:
                data = pd.read_csv(file_path)
                try:
                    data.set_index("Unnamed: 0", inplace=True)
                    data.index.rename("index", inplace=True)
                except KeyError:
                    pass
                most_recent_filing = data.columns[-1]
                if self.is_outdated(
                    most_recent_filing, self.expired
                ):  # Check if most recent filing is outdated.
                    new_data = self.equity_scraper.get_balance_sheet(ticker, freq)
                    result_data = pd.concat([data, new_data], axis=1, join="inner")
                    if write_data:
                        result_data.to_csv(file_path)
                    return result_data
                else:
                    return data
            except FileNotFoundError:
                data = self.equity_scraper.get_balance_sheet(ticker.upper(), freq)
                try:
                    if write_data:
                        data.to_csv(file_path)
                except OSError:
                    folder_path = f"{self.equities_folder}\\Stocks\\{ticker.upper()}\\Statements\\{freq}"
                    os.makedirs(folder_path, exist_ok=True)
                    if write_data:
                        data.to_csv(file_path)
                return data

    def get_cash_flow(
        self,
        ticker: str,
        freq: str = "q",
        force_update: bool = False,
        write_data: bool = True,
    ):
        if freq == "q":
            freq = "Quarter"
        elif freq == "a":
            freq = "Annual"
        file_path = f"{self.equities_folder}\\Stocks\\{ticker.upper()}\\Statements\\{freq}\\{ticker.upper()}_cash_flow.csv"

        if force_update:
            try:
                data = pd.read_csv(file_path)
                try:
                    data.set_index("Unnamed: 0", inplace=True)
                    data.index.rename("index", inplace=True)
                except KeyError:
                    pass
                new_data = self.equity_scraper.get_cash_flow(ticker, freq)
                result_data = pd.concat([data, new_data], axis=1, join="inner")
                if write_data:
                    result_data.to_csv(file_path)
                return result_data
            except FileNotFoundError:
                data = self.equity_scraper.get_cash_flow(ticker, freq)
                if write_data:
                    data.to_csv(file_path)
                return data
        # If no force update, then get the statement regularly.
        else:
            try:
                data = pd.read_csv(file_path)
                try:
                    data.set_index("Unnamed: 0", inplace=True)
                    data.index.rename("index", inplace=True)
                except KeyError:
                    pass
                most_recent_filing = data.columns[-1]
                if self.is_outdated(
                    most_recent_filing, self.expired
                ):  # Check if most recent filing is outdated.
                    new_data = self.equity_scraper.get_cash_flow(ticker, freq)
                    result_data = pd.concat([data, new_data], axis=1, join="inner")
                    if write_data:
                        result_data.to_csv(file_path)
                    return result_data
                else:
                    return data
            except FileNotFoundError:
                data = self.equity_scraper.get_cash_flow(ticker.upper(), freq)
                try:
                    if write_data:
                        data.to_csv(file_path)
                except OSError:
                    folder_path = f"{self.equities_folder}\\Stocks\\{ticker.upper()}\\Statements\\{freq}"
                    os.makedirs(folder_path, exist_ok=True)
                    if write_data:
                        data.to_csv(file_path)
                return data

    ##################################################################### Utilities #####################################################################
    def is_outdated(self, date, day_threshold: int = 70):

        has_days = dt.datetime.strptime(date, "%Y-%m-%d").day is not None

        # If the date passed *does not* have days. %Y-%m
        if not has_days:
            # Ensure month has a leading zero
            date_str_padded = dt.datetime.strptime(date, "%Y-%m").strftime("%Y-%m")
            # Convert to datetime object
            date_object = dt.datetime.strptime(date_str_padded, "%Y-%m")
        # If the date passed *does* have days. %Y-%m-%d
        else:
            date_object = dt.datetime.strptime(date, "%Y-%m-%d")
        # Get current datetime
        current_date = dt.datetime.now()
        # Calculate difference between dates.
        elapse = current_date - date_object

        print(f"Elapse: {elapse}")

        if elapse.days >= day_threshold:
            return True
        else:
            return False

    def setup_local_equity_files(self, ticker: str):
        ticker = ticker.upper()

        base_path = f"{self.equities_folder}\\Stocks"

        # Attributes
        # Stock folder
        stock_folder = f"{base_path}\\{ticker.upper()}"
        os.makedirs(stock_folder, exist_ok=True)  # Create folder if it does not exist.
        # Dividends
        dividends_folder = f"{stock_folder}\\Dividends"
        os.makedirs(dividends_folder, exist_ok=True)
        # Institutional holders
        institutional_folder = f"{stock_folder}\\InstitutionalHolders"
        os.makedirs(institutional_folder, exist_ok=True)
        # Financial Statements
        statements_folder = f"{stock_folder}\\Statements"
        os.makedirs(statements_folder, exist_ok=True)
        # Annual statements
        annual_folder = f"{statements_folder}\\Annual"
        os.makedirs(annual_folder, exist_ok=True)
        # Quarterly Statements
        quarter_folder = f"{statements_folder}\\Quarter"
        os.makedirs(quarter_folder, exist_ok=True)
