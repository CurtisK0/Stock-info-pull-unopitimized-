import yfinance as yf
import pandas as pd
from datetime import datetime

def analyze_stock(ticker_symbol):
    print(f"Pulling data for {ticker_symbol}...")
    stock = yf.Ticker(ticker_symbol)
    info = stock.info

    # 1. Pulling the Fundamentals
    trailing_pe = info.get('trailingPE', None)
    forward_pe = info.get('forwardPE', None)
    volume = info.get('averageVolume', None)
    beta = info.get('beta', None) # Measures volatility/cyclicality compared to the market
    sector = info.get('sector', 'Unknown')
    q_financials = stock.quarterly_financials

    revenue_growth = info.get('revenueGrowth', 0)
    q_cash_flow = stock.quarterly_cashflow
    recent_EBITDA = q_financials.loc['EBITDA'].iloc[0]
    prev_EBITDA = q_financials.loc['EBITDA'].iloc[1]
    recent_capex = q_cash_flow.loc['Capital Expenditure'].iloc[0]
    recent_revenue = stock.quarterly_financials.loc['Total Revenue'].iloc[0]
    EBITDA_growth_qoq = ((recent_EBITDA - prev_EBITDA) / abs(prev_EBITDA)) * 100
    capex_intensity_q = (abs(recent_capex) / recent_revenue) * 100

 
    # 2. Calculating Stage of Development (Custom Logic)
def dev_stage(ticker_symbol):
    if EBITDA_growth_qoq>15.0 or(EBITDA_growth_qoq>8.0 and capex_intensity_q>8.0):
        Life_cycle= "Expansionary"
    elif EBITDA_growth_qoq < 0.0 or (EBITDA_growth_qoq < 5.0 and capex_intensity_q > 8.0) or (fcf_growth_qoq < 8.0 and capex_intensity_q > 15.0):
        Life_cycle="Stagnation"
        if Life_cycle=="Stagnation":


    else:
        Life_cycle="Stability"



    # 3. Calculating Call/Put Ratio (Using the nearest expiration date)
def cnp(ticker_symbol):
    try:
        options_dates = stock.options
        if options_dates:
            chain = stock.option_chain(options_dates[0])
            calls_vol = chain.calls['volume'].sum()
            puts_vol = chain.puts['volume'].sum()
            put_call_ratio = puts_vol / calls_vol if calls_vol > 0 else 0
        else:
            put_call_ratio = None
    except Exception:
        put_call_ratio = None

    # 4. Package it into a dictionary
    return {
        'Date': datetime.now().strftime('%Y-%m-%d'),
        'Ticker': ticker_symbol,
        'Sector': sector,
        'Life_cycle': Life_cycle,
        'Trailing PE': trailing_pe,
        'Forward PE': forward_pe,
        'Volume': volume,
        'Beta': beta,
        'QOQ Growth':EBITDA_growth_qoq,
        'Capex ussage': capex_intensity_q,
        'Put/Call Ratio': round(put_call_ratio, 2) if put_call_ratio else None
    }

# --- Main Execution ---

# List the tickers you want to track
my_tickers = ['ASML', 'META', 'GOOGL', 'MA', 'TSLA']

# Run the function for each ticker
results = []
for ticker in my_tickers:
    data = analyze_stock(ticker) + dev_stage(ticker)
    results.append(data)

# Convert to a Pandas DataFrame (which is essentially a Python spreadsheet)
df = pd.DataFrame(results)
filtered_df = df

# 5. The Filtering Equation!
# Example: Create a custom "Undervalued Score". 
# If Forward PE is less than Trailing PE, the company is expected to grow earnings.
"""df['Custom_Score'] = df['Trailing PE'] - df['Forward PE']

# Filter the sheet to ONLY show stocks with a Put/Call ratio less than 1 (Bullish sentiment)
# and where our Custom Score is positive.
filtered_df = df[(df['Put/Call Ratio'] < 1) and (df['Custom_Score'] > 0)]"""

# 6. Output to CSV (can be opened in Excel or Google Sheets)
file_name = f"market_screener_{datetime.now().strftime('%Y_%m_%d')}.csv"
filtered_df.to_csv(file_name, index=False)

print(f"\nSuccess! Data saved to {file_name}")
print(filtered_df)