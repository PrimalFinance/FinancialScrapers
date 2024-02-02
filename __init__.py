from DataManager.data_manager import DataManager


if __name__ == "__main__":

    d = DataManager("D:\\FinancialData\\FinancialData")
    tickers = ["AAPL", "MFTS", "NVDA", "TSLA", "AMD", "UNH", "PFE", "RKLB", "PLTR"]
    for ticker in tickers:
        freq = "q"
        data = d.get_income_statement(ticker, freq)
        d.get_balance_sheet(ticker, freq)
        d.get_cash_flow(ticker, freq)
        print(f"Data: {data}")
