from DataManager.data_manager import DataManager


if __name__ == "__main__":

    d = DataManager("D:\\FinancialData\\FinancialData")
    tickers = d.get_ticker_list()
    for t in tickers:
        d.get_filing_dates(t)
