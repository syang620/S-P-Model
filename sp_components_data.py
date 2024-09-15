
# Import the necessary modules
import pandas as pd
import numpy as np
import yfinance as yf
import time

from pyfmpcloud import settings
settings.set_apikey([your api key])
from pyfmpcloud import company_valuation as cv

def get_sp_tickers():
    sp_assets = pd.read_html(
        'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')[0]
    assets = sp_assets['Symbol'].str.replace('.', '-').tolist()
    return assets

def get_sp_data(start='2008-01-01', end=None):
    import pandas as pd
    import yfinance as yf
    # Get the current SP components, and get a tickers list
    assets = get_sp_tickers()
    # Download historical data to a multi-index DataFrame
    try:
        data = yf.download(assets, start=start, end=end)
        filename = 'sp_components_data.pkl'
        data.to_pickle(filename)
        print('Data saved at {}'.format(filename))
    except ValueError:
        print('Failed download, try again.')
        data = None
    return data

def get_div_ts_data(period = '2y'):
    tickers = get_sp_tickers()
    all_ticker = ' '.join(tickers)
    stock = yf.Tickers(all_ticker)
    div_data = []
    ticker_errors = []
    for t in tickers:
        try:
            div_df = stock.tickers[t].history(period = period)[['Close', 'Dividends']].dropna()
            div_df.columns = pd.MultiIndex.from_product([[t], div_df.columns])
    #         div_df['Ticker'] = t
            div_data.append(div_df)
        except:
            ticker_errors.append(t)
    try:
        df = pd.concat(div_data)
        filename = 'sp_div_ts_data.pkl'
        df.to_pickle(filename)
        print('Data saved at {}'.format(filename))
    except ValueError:
        print('Failed download, try again.')
        df = None
    return df
    
def sp_ticker_data():
    assets = get_sp_tickers
    ticker_info_lst = list(map(lambda t: yf.Ticker(t).info, assets))
    try:
        df = pd.DataFrame(ticker_info_lst,
                  columns=
                    ['symbol', 'sector', 'currentPrice', 'ebitdaMargins', 'profitMargins','grossMargins', 
                     'revenueGrowth', 'operatingMargins', 'recommendationKey', 'earningsGrowth', 'currentRatio', 
                     'returnOnAssets','debtToEquity', 'returnOnEquity', 'totalCashPerShare', 'revenuePerShare',
                     'quickRatio','recommendationMean', 'enterpriseToRevenue',  'enterpriseToEbitda','52WeekChange', 
                     'forwardEps','bookValue', 'sharesPercentSharesOut','heldPercentInstitutions', 
                     'netIncomeToCommon', 'trailingEps','lastDividendValue', 'SandP52WeekChange', 'priceToBook', 
                     'heldPercentInsiders','shortRatio','beta', 'enterpriseValue','earningsQuarterlyGrowth', 
                     'priceToSalesTrailing12Months','pegRatio', 'forwardPE', 'shortPercentOfFloat',
                     'sharesShortPriorMonth','previousClose', 'regularMarketOpen', 'twoHundredDayAverage',
                     'trailingAnnualDividendYield', 'payoutRatio','averageDailyVolume10Day',
                     'trailingAnnualDividendRate', 'averageVolume10days', 'dividendRate','trailingPE', 
                     'regularMarketVolume','averageVolume','volume', 'fiveYearAvgDividendYield','dividendYield',
                     'trailingPegRatio']
                )
        filename = 'sp_ticker_data.pkl'
        df.to_pickle(filename)
        print('Data saved at {}'.format(filename))
    except ValueError:
        print('Failed download, try again.')
        df = None
    return df

def sp_rating_data():
    assets = get_sp_tickers()
    buckets = np.array_split(assets, 100)
    l = []
    for b in buckets:
        l += list(map(lambda x:cv.rating(x, history = 'today').set_index('date'), b.tolist()))
        time.sleep(1) 
    try:
        df = pd.concat(l)
        filename = 'sp_rating_data.pkl'
        df.to_pickle(filename)
        print('Data saved at {}'.format(filename))
    except ValueError:
        print('Failed download, try again.')
        df = None
    return df

def sp_sector_weights():
    # Fetch S&P 500 tickers (you might need a different source for tickers)
    sp500_tickers = get_sp_tickers()

    # Initialize dictionaries
    sectors = {}
    market_caps = {}

    for ticker in sp500_tickers:
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            sector = info.get('sector', 'Unknown')
            market_cap = info.get('marketCap', 0)  # Default to 0 if marketCap is not available

            if sector in sectors:
                sectors[sector].append(market_cap)
            else:
                sectors[sector] = [market_cap]

            market_caps[ticker] = market_cap

        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")

    # Calculate total market cap for each sector
    sector_market_caps = {sector: sum(mcaps) for sector, mcaps in sectors.items()}

    # Calculate sector weights
    total_market_cap = sum(sector_market_caps.values())
    sector_weights = [(sector, market_cap / total_market_cap) for sector, market_cap in sector_market_caps.items()]
    df = pd.DataFrame(sector_weights, columns = ['Sector', 'Weight'])
    return df

if __name__ == '__main__':
    sp_data = get_sp_data()