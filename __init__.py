from DataManager.data_manager import DataManager


if __name__ == "__main__":
    d = DataManager("D:\\FinancialData\\FinancialData")

    data = d.get_fed_funds()
    print(f"Data: {data}")
