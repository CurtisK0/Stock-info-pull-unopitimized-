import yfinance as yf
import pandas as pd
from datetime import datetime
import concurrent.futures

class StockAnalyzer:
  
    def __init__(self, symbol):
        self.symbol = symbol
        # Establish API per object
        self.ticker = yf.Ticker(symbol)
        
        # Pre-fetch basic info
        self.info = self.ticker.info
        self.sector = self.info.get('sector', 'Unknown')
        
        # Initialize state variables for calculated metrics
        self.ebitda_growth_qoq = 0.0
        self.ebitda_growth_yoy = 0.0
        self.capex_intensity_current = 0.0
        self.capex_intensity_yoy= 0.0
        self.lifecycle_stage_qoq = "Unknown"
        self.lifecycle_stage_yoy = "Unknown"
        self.put_call_ratio = None

    def calculate_financial_metrics(self):
        """Pulls and calculates EBITDA and CapEx data (YoY and Quarterly)."""
        try:
            q_fin = self.ticker.quarterly_financials
            q_cf = self.ticker.quarterly_cashflow
            
            # YoY Quarterly EBITDA Growth
            recent_ebitda = q_fin.loc['EBITDA'].iloc[0]
            prev_ebitda_qoq=q_fin.loc['EBITDA'].iloc[1]
            prev_ebitda_yoy = q_fin.loc['EBITDA'].iloc[4]
            self.ebitda_growth_yoy = ((recent_ebitda - prev_ebitda_yoy) / abs(prev_ebitda_yoy)) * 100

            # QoQ EBITDA Growth
            self.ebitda_growth_qoq = ((recent_ebitda - prev_ebitda_qoq) / abs(prev_ebitda_qoq)) * 100
            
            # Quarterly CapEx Intensity
            recent_capex = abs(q_cf.loc['Capital Expenditure'].iloc[0])
            recent_rev = q_fin.loc['Total Revenue'].iloc[0]
            self.capex_intensity_current = (recent_capex / recent_rev) * 100
            
            # YoY CapEx Intersity 
            capex_growth_yoy = abs(q_cf.loc['Capital Expenditure'].iloc[0])-abs(q_cf.loc['Capital Expenditure'].iloc[4])
            rev_growth_yoy = abs(q_fin.loc['Total Revenue'].iloc[0])-abs(q_fin.loc['Total Revenue'].iloc[4])
            self.capex_intensity_yoy = (capex_growth_yoy / rev_growth_yoy) * 100

        except Exception:
            pass

    def determine_lifecycle(self):
        """Categorizes lifecycle based on the instance's stored metrics."""
        growth_yoy = self.ebitda_growth_yoy
        growth_qoq = self.ebitda_growth_qoq
        capex_recent = self.capex_intensity_current
        capex_yoy= self.capex_intensity_yoy
        
        if growth_qoq > 15.0 or (growth_qoq > 8.0 and capex_recent > 8.0):
            self.lifecycle_stage_qoq = "Expansionary"
        elif growth_qoq < 0.0 or (growth_qoq < 5.0 and capex_recent > 8.0) or (growth_qoq < 8.0 and capex_recent > 15.0):
            self.lifecycle_stage_qoq = "Stagnation"
        else:
            self.lifecycle_stage_qoq = "Stability"

        

    def calculate_put_call_ratio(self):
        """Pulls the Put/Call volume ratio for the nearest expiration."""
        try:
            options_dates = self.ticker.options
            if options_dates:
                chain = self.ticker.option_chain(options_dates[0])
                calls_vol = chain.calls['volume'].sum()
                puts_vol = chain.puts['volume'].sum()
                self.put_call_ratio = round(puts_vol / calls_vol, 2) if calls_vol > 0 else 0
        except Exception:
            pass

    def generate_report(self):
        """Executes all analysis and returns a packaged dictionary."""
        print(f"Analyzing {self.symbol}...")
        
        # Execute the internal methods to update the object's state
        self.calculate_financial_metrics()
        self.determine_lifecycle()
        self.calculate_put_call_ratio()

        # Return the final snapshot
        return {
            'Date': datetime.now().strftime('%Y-%m-%d'),
            'Ticker': self.symbol,
            'Sector': self.sector,
            'Life_cycle': self.lifecycle_stage_qoq,
            'Trailing PE': self.info.get('trailingPE'),
            'Forward PE': self.info.get('forwardPE'),
            'Beta': self.info.get('beta'),
            'QOQ Growth (EBITDA)': round(self.ebitda_growth_qoq, 2),
            'YOY Growth (EBITDA)': round(self.ebitda_growth_yoy, 2),
            'Capex QOQ Intensity': round(self.capex_intensity_current, 2),
            'Capex YOY Intensity': round(self.capex_intensity_yoy, 2),
            'Put/Call Ratio': self.put_call_ratio,
        }

def get_market_cap(symbol):
    """Helper function to quickly grab just the market cap for sorting."""
    try:
        # .fast_info is a lightweight property in yfinance that bypasses 
        # the heavy .info dictionary, drastically speeding up the pull.
        cap = yf.Ticker(symbol).fast_info.market_cap
        return symbol, cap
    except Exception:
        return symbol, 0

def process_single_stock(symbol):
    """Wrapper function to handle a single stock for multithreading."""
    try:
        analyzer = StockAnalyzer(symbol)
        return analyzer.generate_report()
    except Exception as e:
        # Silently fail or log it to keep the console clean
        return None
"""def get_top_400_sp500():
    
    print("Scraping S&P 500 base list from Wikipedia...")
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    tables = pd.read_html(url)
    tickers = tables[0]['Symbol'].tolist()
    
    # Clean tickers for Yahoo Finance compatibility
    clean_tickers = [t.replace('.', '-') for t in tickers]
    
    print("Multithreading market cap retrieval for accurate sorting...")
    market_caps = {}
    
    # Spin up 20 parallel threads to blast through the API limits safely
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        results = executor.map(get_market_cap, clean_tickers)
        
        for symbol, cap in results:
            # Handle potential NoneType returns from fast_info
            market_caps[symbol] = cap if cap is not None else 0
            
    # Sort the dictionary keys (tickers) based on their values (market caps) in descending order
    sorted_tickers = sorted(market_caps.keys(), key=lambda x: market_caps[x], reverse=True)
    
    print("List successfully sorted. Slicing top 400...")
    # Return exactly index 0 through 399
    return sorted_tickers[:400]
"""
# --- Main Execution Block ---
my_tickers = ['MSFT', 'AAPL', 'NVDA', 'AMZN', 'GOOGL', 'META', 'GOOG', 'BRK-B', 'LLY', 'AVGO', 'JPM', 'TSLA', 'UNH', 'V', 'XOM', 'MA', 'JNJ', 'PG', 'HD', 'COST', 'ABBV', 'MRK', 'CRM', 'CVX', 'NFLX', 'AMD', 'WMT', 'PEP', 'KO', 'BAC', 'LIN', 'TMO', 'MCD', 'ADBE', 'ACN', 'CSCO', 'DIS', 'ABT', 'INTU', 'QCOM', 'ORCL', 'IBM', 'WFC', 'DHR', 'CAT', 'TXN', 'PM', 'VZ', 'NOW', 'INTC', 'COP', 'BA', 'AMGN', 'GE', 'SPGI', 'HON', 'UNP', 'RTX', 'AXP', 'LOW', 'BKNG', 'SYK', 'ELV', 'NKE', 'PLD', 'GS', 'TJX', 'BLK', 'MDT', 'PGR', 'MMC', 'MS', 'CB', 'C', 'SCHW', 'AMT', 'ADP', 'ISRG', 'DE', 'LMT', 'GILD', 'VRTX', 'CI', 'BSX', 'REGN', 'ZTS', 'MO', 'FI', 'ADI', 'MU', 'LRCX', 'KLAC', 'SNPS', 'CDNS', 'PANW', 'FTNT', 'MCO', 'AON', 'CME', 'ICE', 'T', 'DUK', 'SO', 'NEE', 'SRE', 'AEP', 'D', 'EXC', 'XEL', 'ED', 'PEG', 'TGT', 'DG', 'DLTR', 'ROST', 'ORLY', 'AZO', 'TSCO', 'GPC', 'KR', 'SYY', 'GIS', 'K', 'HSY', 'MDLZ', 'STZ', 'MNST', 'CHD', 'CL', 'CLX', 'KMB', 'SJM', 'CAG', 'CPB', 'MKC', 'TAP', 'KDP', 'WBA', 'CVS', 'HUM', 'CNC', 'MOH', 'CAH', 'MCK', 'COR', 'ALGN', 'BDX', 'BIIB', 'TECH', 'ILMN', 'MTD', 'RMD', 'STE', 'WAT', 'WST', 'ZBH', 'A', 'BAX', 'COO', 'DXCM', 'EW', 'HOLX', 'IDXX', 'IQV', 'OGN', 'XRAY', 'MMM', 'AAL', 'ALK', 'DAL', 'LUV', 'UAL', 'CHRW', 'EXPD', 'FDX', 'UPS', 'JBHT', 'CSX', 'NSC', 'CPRT', 'ODFL', 'PCAR', 'PNR', 'RSG', 'WM', 'AOS', 'DOV', 'ETN', 'EMR', 'ITW', 'PH', 'ROK', 'SNA', 'SWK', 'TT', 'XYL', 'CMI', 'DE', 'PCAR', 'TXT', 'MAS', 'CARR', 'URI', 'GWW', 'FAST', 'NDSN', 'AME', 'APH', 'GLW', 'TEL', 'HPQ', 'HPE', 'NTAP', 'STX', 'WDC', 'AKAM', 'ANET', 'FFIV', 'JNPR', 'MSI', 'KEYS', 'TRMB', 'TYL', 'ZBRA', 'DXC', 'EPAM', 'IT', 'CTSH', 'CDW', 'PAYX', 'BR', 'LDOS', 'VRSK', 'CSGP', 'PAYC', 'PTC', 'FICO', 'CPAY', 'GPN', 'JKHY', 'CTAS', 'ROL', 'EFX', 'TRV', 'ALL', 'AFL', 'PRU', 'MET', 'HIG', 'CINF', 'PFG', 'LNC', 'AMP', 'DFS', 'SYF', 'COF', 'FITB', 'MTB', 'HBAN', 'KEY', 'CFG', 'RF', 'CMA', 'ZION', 'TFC', 'PNC', 'USB', 'WMB', 'KMI', 'OKE', 'HAL', 'BKR', 'SLB', 'EOG', 'PXD', 'OXY', 'HES', 'DVN', 'MPC', 'VLO', 'PSX', 'FCX', 'NEM', 'NUE', 'STLD', 'SHW', 'PPG', 'ECL', 'APD', 'DD', 'CE', 'EMN', 'LYB', 'DOW', 'CTVA', 'FMC', 'MOS', 'CF', 'ALB', 'VMC', 'MLM', 'EXR', 'PSA', 'O', 'NNN', 'SPG', 'KIM', 'MAC', 'BXP', 'VNO', 'ARE', 'EQIX', 'DLR', 'AMT', 'CCI', 'SBAC', 'PLD', 'DRE', 'HST', 'MAR', 'HLT', 'RCL', 'CCL', 'NCLH', 'EXPE', 'ABNB', 'CMG', 'YUM', 'DRI', 'DPZ', 'SBUX', 'LVS', 'MGM', 'WYNN', 'CZR', 'PENN', 'HAS', 'MAT', 'EA', 'TTWO', 'ATVI', 'LYV', 'NWSA', 'NWS', 'FOXA', 'FOX', 'PARA', 'WBD', 'OMC', 'IPG', 'DISCK', 'UHS', 'HCA', 'THC', 'DVA', 'CYH', 'LH', 'DGX', 'PKI', 'CRL', 'BIO', 'CTLT', 'RGEN', 'CZR', 'GNRC', 'SEDG', 'ENPH', 'FSLR', 'TER', 'QRVO', 'SWKS', 'MPWR', 'NXPI', 'ON', 'MCHP', 'CDW', 'GL', 'WHR', 'LEG', 'MHK', 'PHM', 'DHI', 'LEN', 'NVR', 'TOL', 'HD', 'LOW', 'BBY', 'KMX', 'AAP', 'GPS', 'LB', 'URBN', 'AEO', 'ANF', 'GES', 'M', 'JWN', 'KSS', 'DDS', 'RL', 'VFC', 'UAA', 'UA', 'PVH', 'TPR', 'CPRI', 'HBI', 'SEE', 'WRK', 'IP', 'PKG', 'AMCR', 'BLL', 'AVY', 'FDS', 'MSCI', 'CBOE', 'NDAQ', 'CINF', 'RE', 'WRB', 'BRO', 'GL', 'L', 'PRU', 'AIG', 'AFL', 'MET', 'PFG', 'HIG', 'LNC', 'CMA', 'ZION', 'FITB', 'MTB', 'HBAN', 'KEY', 'CFG', 'RF', 'TFC', 'PNC', 'USB', 'STT', 'BK', 'NTRS', 'TROW', 'IVZ', 'BEN', 'AMG']

if __name__ == "__main__":
    results = []
    
    for symbol in my_tickers:
        # Instantiate a new StockAnalyzer object for the current ticker
        analyzer = StockAnalyzer(symbol)
        
        # Generate the dictionary report and append it to our list
        data_dict = analyzer.generate_report()
        results.append(data_dict)

    # Convert the list of dictionaries to a Pandas DataFrame
    df = pd.DataFrame(results)
    
    # Save to CSV
    file_name = f"market_screener_{datetime.now().strftime('%Y_%m_%d')}.csv"
    df.to_csv(file_name, index=False)
    
    print(f"\nSuccess! Data saved to {file_name}")
    print(df[['Ticker', 'Sector','Life_cycle', 'Trailing PE','Forward PE','Beta','QOQ Growth (EBITDA)','YOY Growth (EBITDA)','Capex QOQ Intensity','Capex YOY Intensity','Put/Call Ratio']])
    

    # --- Main Execution Block ---


"""if __name__ == "__main__":
    
    # 1. Generate the Top 400 list dynamically
    target_tickers = get_top_400_sp500()
    
    results = []
    
    print("\nBeginning heavy fundamental analysis on Top 400...")
    # 2. Loop through the optimized list
    for i, symbol in enumerate(target_tickers, 1):
        try:
            print(f"[{i}/400] Processing {symbol}...")
            analyzer = StockAnalyzer(symbol)
            data_dict = analyzer.generate_report()
            results.append(data_dict)
        except Exception as e:
            print(f"Failed to pull fundamental data for {symbol}: {e}")
            continue

    # 3. Compile and export
    df = pd.DataFrame(results)
    
    file_name = f"sp500_top400_screener_{datetime.now().strftime('%Y_%m_%d')}.csv"
    df.to_csv(file_name, index=False)
    
    print(f"\nSuccess! Data saved to {file_name}")"""

if __name__ == "__main__":
    
    # 1. Generate the Top 400 list dynamically
    target_tickers = my_tickers
    
    results = []
    
    print("\nBeginning heavy fundamental analysis on Top 400...")
    print("Multithreading the API requests. Please wait...")
    
    # 2. Multithread the heavy fundamental pulls
    # WARNING: Keep max_workers between 5 and 10. If you go too high (like 20+), 
    # Yahoo Finance will flag you as a DDoS attack and temporarily IP ban you (HTTP 429 Error).
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        # Submit all tasks to the executor
        future_to_symbol = {executor.submit(process_single_stock, symbol): symbol for symbol in target_tickers}
        
        # As each thread finishes downloading a stock's data, process it
        for i, future in enumerate(concurrent.futures.as_completed(future_to_symbol), 1):
            symbol = future_to_symbol[future]
            data_dict = future.result()
            
            if data_dict:
                results.append(data_dict)
                
            # Print a progress tracker that overwrites the same line to keep the terminal clean
            print(f"\r[{i}/400] Processed {symbol}".ljust(40), end="")

    # 3. Compile and export
    df = pd.DataFrame(results)
    
    file_name = f"sp500_top400_screener_{datetime.now().strftime('%Y_%m_%d')}.csv"
    df.to_csv(file_name, index=False)
    
    print(f"\n\nSuccess! Data saved to {file_name}")
