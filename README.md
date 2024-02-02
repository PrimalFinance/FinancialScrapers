# Financial Scrapers

### _Note_

Local data for this repository can be found here: https://github.com/PrimalFinance/FinancialDatabase

#### Datamanager

Interfaces with the scrapers. If the data is not found locally the scrapers will fetch data from the web and store it locally.

# Commodity Scraper

- Gets information about various commodities. Includes tickers used for Yahoo Finance.

# Equity Scraper

_Requires Alpha Vantage key_

- Gets historical price data from Yahoo Finance.
- Gets filing dates from the SEC website.
- Gets earnings estimates from alpha vantage.

# Macro Scraper

- Gets CPI from FRED.
- Gets Fed Funds Rate from FRED.
- Gets Treasury 10 Year - 2 Year Yield Spread from FRED.
