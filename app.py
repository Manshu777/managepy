import yfinance as yf
import pandas as pd
import numpy as np
import ccxt
import sqlite3
import mysql.connector
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import plotly.graph_objs as go
from dash import Dash, dcc, html, Input, Output, dash_table, State
import logging
from urllib.parse import quote_plus
import schedule
import time
import threading
import ta
from retrying import retry
from pytz import timezone
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler('market_dashboard.log'),
    logging.StreamHandler()
])

# Email configuration
EMAIL_CONFIG = {
    'smtp_server': 'smtp.gmail.com',
    'smtp_port': 587,
    'sender_email': 'manshusmartboy@gmail.com',  # Replace with your email
    'sender_password': 'gkuzchocgvdjxcvb',  # Replace with your app-specific password
    'recipient_email': ['ap2290731@gmail.com', 'manshu.developer@gmail.com']  # Updated to list of recipients
}

# Configuration
TICKERS = {
    'stocks': ['AAPL', 'RELIANCE.NS'],
    'indices': ['^NSEI'],
    'crypto': ['BTC/USDT'],
    'commodities': ['GC=F']
}
START_DATE = '2020-01-01'
END_DATE = datetime.now().strftime('%Y-%m-%d')
MYSQL_CONFIG = {
    'user': 'root',
    'password': 'root1234',
    'host': 'localhost',
    'database': 'trading',
    'port': '3306'
}
UPDATE_INTERVAL = 60  # Seconds for real-time updates
MYSQL_ENABLED = True
RETRY_ATTEMPTS = 3
RETRY_DELAY = 5000  # ms
IST = timezone('Asia/Kolkata')
MARKET_HOURS = {
    'stocks': {'open': '09:15', 'close': '15:30'},  # NSE hours
    'indices': {'open': '09:15', 'close': '15:30'},
    'commodities': {'open': '09:00', 'close': '17:00'},  # COMEX hours (approx)
    'crypto': {'open': '00:00', 'close': '23:59'}  # 24/7
}

# Initialize Dash app
app = Dash(__name__)
server = app.server  # Required for Gunicorn

# Initialize ccxt exchange
crypto_exchange = ccxt.binance()

# Global data stores
historical_data = {}
realtime_data = {}
option_chains = {}
financials = {}
lock = threading.Lock()

def is_market_open(category):
    """Check if market is open for the given category."""
    now = datetime.now(IST)
    if category not in MARKET_HOURS:
        return False
    open_time = datetime.strptime(MARKET_HOURS[category]['open'], '%H:%M').replace(
        year=now.year, month=now.month, day=now.day, tzinfo=IST)
    close_time = datetime.strptime(MARKET_HOURS[category]['close'], '%H:%M').replace(
        year=now.year, month=now.month, day=now.day, tzinfo=IST)
    is_weekday = now.weekday() < 5  # Monday to Friday
    return is_weekday and open_time <= now <= close_time

def test_mysql_connection():
    """Test MySQL connection."""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        conn.close()
        logging.info("MySQL connection test successful")
        return True
    except Exception as e:
        logging.error(f"MySQL connection test failed: {e}")
        return False

@retry(stop_max_attempt_number=RETRY_ATTEMPTS, wait_fixed=RETRY_DELAY)
def fetch_historical_stock_data(ticker, start_date, end_date):
    """Fetch historical data using yfinance."""
    try:
        stock = yf.Ticker(ticker)
        data = stock.history(start=start_date, end=end_date).reset_index()
        data['Ticker'] = ticker
        data = data[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
        logging.info(f"Fetched historical stock data for {ticker}")
        return data
    except Exception as e:
        logging.error(f"Error fetching historical data for {ticker}: {e}")
        raise

@retry(stop_max_attempt_number=RETRY_ATTEMPTS, wait_fixed=RETRY_DELAY)
def fetch_historical_crypto_data(symbol, start_date, end_date):
    """Fetch historical OHLCV data using ccxt."""
    try:
        since = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp() * 1000)
        timeframe = '1d'
        ohlcv = crypto_exchange.fetch_ohlcv(symbol, timeframe, since)
        data = pd.DataFrame(ohlcv, columns=['Date', 'Open', 'High', 'Low', 'Closekeletal', 'Volume'])
        data['Date'] = pd.to_datetime(data['Date'], unit='ms')
        data['Ticker'] = symbol
        data = data[['Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume']]
        logging.info(f"Fetched historical crypto data for {symbol}")
        return data
    except Exception as e:
        logging.error(f"Error fetching historical crypto data for {symbol}: {e}")
        raise

@retry(stop_max_attempt_number=RETRY_ATTEMPTS, wait_fixed=RETRY_DELAY)
def fetch_real_time_stock_data(ticker):
    """Fetch real-time stock data."""
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        return {
            'ticker': ticker,
            'price': info.get('currentPrice', np.nan),
            'timestamp': datetime.now(IST)
        }
    except Exception as e:
        logging.error(f"Error fetching real-time data for {ticker}: {e}")
        raise

@retry(stop_max_attempt_number=RETRY_ATTEMPTS, wait_fixed=RETRY_DELAY)
def fetch_crypto_data(symbol):
    """Fetch real-time crypto data."""
    try:
        ticker = crypto_exchange.fetch_ticker(symbol)
        return {
            'ticker': symbol,
            'price': ticker['last'],
            'timestamp': datetime.fromtimestamp(ticker['timestamp'] / 1000, tz=IST)
        }
    except Exception as e:
        logging.error(f"Error fetching crypto data for {symbol}: {e}")
        raise

@retry(stop_max_attempt_number=RETRY_ATTEMPTS, wait_fixed=RETRY_DELAY)
def fetch_option_chain(ticker):
    """Fetch option chain data."""
    try:
        stock = yf.Ticker(ticker)
        expiration_dates = stock.options
        if not expiration_dates:
            return None
        option_chain = stock.option_chain(expiration_dates[0])
        calls = option_chain.calls[['contractSymbol', 'lastPrice', 'strike', 'volume']]
        calls['Type'] = 'Call'
        puts = option_chain.puts[['contractSymbol', 'lastPrice', 'strike', 'volume']]
        puts['Type'] = 'Put'
        options = pd.concat([calls, puts]).reset_index(drop=True)
        options['Ticker'] = ticker
        logging.info(f"Fetched option chain for {ticker}")
        return options
    except Exception as e:
        logging.error(f"Error fetching option chain for {ticker}: {e}")
        return None

@retry(stop_max_attempt_number=RETRY_ATTEMPTS, wait_fixed=RETRY_DELAY)
def fetch_financial_statements(ticker):
    """Fetch financial statements."""
    try:
        stock = yf.Ticker(ticker)
        return {
            'income_statement': stock.financials,
            'balance_sheet': stock.balance_sheet,
            'cash_flow': stock.cashflow
        }
    except Exception as e:
        logging.error(f"Error fetching financial statements for {ticker}: {e}")
        return None

def save_to_csv(data, filename, mode='w'):
    """Save data to CSV."""
    if data is None:
        logging.error(f"Cannot save to CSV {filename}: Data is None")
        return
    try:
        with lock:
            data.to_csv(filename, index=False, mode=mode)
        logging.info(f"Saved data to {filename} (mode: {mode})")
    except Exception as e:
        logging.error(f"Error saving to CSV {filename}: {e}")

def save_to_sqlite(data, table_name, db_name='stock_data.db', if_exists='replace'):
    """Save data to SQLite."""
    if data is None:
        logging.error(f"Cannot save to SQLite {table_name}: Data is None")
        return
    try:
        with lock:
            conn = sqlite3.connect(db_name)
            data.to_sql(table_name, conn, if_exists=if_exists, index=False)
            conn.commit()
            conn.close()
        logging.info(f"Saved data to SQLite table {table_name}")
    except Exception as e:
        logging.error(f"Error saving to SQLite {table_name}: {e}")

def save_to_mysql(data, table_name, if_exists='replace'):
    """Save data to MySQL."""
    global MYSQL_ENABLED
    if not MYSQL_ENABLED:
        logging.warning(f"MySQL disabled, skipping save to {table_name}")
        return
    if data is None:
        logging.error(f"Cannot save to MySQL {table_name}: Data is None")
        return
    try:
        with lock:
            encoded_password = quote_plus(MYSQL_CONFIG['password'])
            connection_string = (
                f"mysql+mysqlconnector://{MYSQL_CONFIG['user']}:{encoded_password}@"
                f"{MYSQL_CONFIG['host']}:{MYSQL_CONFIG['port']}/{MYSQL_CONFIG['database']}"
            )
            logging.debug(f"Attempting MySQL connection with string: {connection_string.replace(encoded_password, '****')}")
            engine = create_engine(connection_string)
            data.to_sql(table_name, engine, if_exists=if_exists, index=False)
        logging.info(f"Saved data to MySQL table {table_name}")
    except Exception as e:
        logging.error(f"Error saving to MySQL {table_name}: {e}")
        MYSQL_ENABLED = False
        logging.warning("MySQL disabled due to connection error")

def save_to_parquet(data, filename):
    """Save data to Parquet, partitioned by date."""
    if data is None:
        logging.error(f"Cannot save to Parquet {filename}: Data is None")
        return
    try:
        with lock:
            data['Date'] = pd.to_datetime(data.get('Date', data.get('Timestamp')))
            data['Year'] = data['Date'].dt.year
            data['Month'] = data['Date'].dt.month
            data.to_parquet(filename, index=False, partition_cols=['Year', 'Month'])
        logging.info(f"Saved data to {filename}")
    except Exception as e:
        logging.error(f"Error saving to Parquet {filename}: {e}")

def calculate_analytics(data):
    """Calculate advanced analytics."""
    if data is None or len(data) <  Camden:
        logging.warning(f"Cannot calculate analytics: Data is None or too short")
        return None
    data = data.copy()
    data.loc[:, 'SMA20'] = data['Close'].rolling(window=20).mean()
    data.loc[:, 'Volatility'] = data['Close'].pct_change().std() * np.sqrt(252)
    data.loc[:, 'RSI'] = ta.momentum.RSIIndicator(data['Close'], window=14).rsi()
    data.loc[:, 'MACD'] = ta.trend.MACD(data['Close']).macd()
    bb = ta.volatility.BollingerBands(data['Close'], window=20)
    data.loc[:, 'BB_High'] = bb.bollinger_hband()
    data.loc[:, 'BB_Low'] = bb.bollinger_lband()
    return data

def send_email_report():
    """Send email with real-time data summary."""
    try:
        with lock:
            summary = []
            for ticker, df in realtime_data.items():
                if not df.empty:
                    latest = df.iloc[-1]
                    summary.append(f"{ticker}: ${latest['Price']:.2f} at {latest['Timestamp']}")
            body = "\n".join(summary) if summary else "No real-time data available."

        msg = MIMEMultipart()
        msg['From'] = EMAIL_CONFIG['sender_email']
        msg['To'] = ', '.join(EMAIL_CONFIG['recipient_email'])
        msg['Subject'] = f"Market Dashboard Data Report - {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')}"
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(EMAIL_CONFIG['smtp_server'], EMAIL_CONFIG['smtp_port']) as server:
            server.starttls()
            server.login(EMAIL_CONFIG['sender_email'], EMAIL_CONFIG['sender_password'])
            server.send_message(msg)
        logging.info(f"Email report sent to {', '.join(EMAIL_CONFIG['recipient_email'])}")
    except Exception as e:
        logging.error(f"Error sending email report: {e}")

def fetch_and_store_data():
    """Fetch and store historical and real-time data."""
    global historical_data, realtime_data, option_chains, financials
    now = datetime.now(IST)
    
    # Fetch historical data (once daily at startup or midnight)
    if not historical_data or now.hour == 0 and now.minute == 0:
        for category, tickers in TICKERS.items():
            for ticker in tickers:
                if category == 'crypto':
                    data = fetch_historical_crypto_data(ticker, START_DATE, END_DATE)
                else:
                    data = fetch_historical_stock_data(ticker, START_DATE, END_DATE)
                if data is not None:
                    data = calculate_analytics(data)
                    safe_ticker = ticker.replace("/", "_").replace("^", "")
                    with lock:
                        historical_data[ticker] = data
                    save_to_csv(data, f'historical_{safe_ticker}.csv')
                    save_to_sqlite(data, f'historical_{safe_ticker}')
                    save_to_mysql(data, f'historical_{safe_ticker}')
                    save_to_parquet(data, f'historical_{safe_ticker}.parquet')
                if category == 'stocks':
                    option_chain = fetch_option_chain(ticker)
                    if option_chain is not None:
                        with lock:
                            option_chains[ticker] = option_chain
                        save_to_csv(option_chain, f'option_chain_{ticker}.csv')
                        save_to_sqlite(option_chain, f'option_chain_{ticker}')
                        save_to_mysql(option_chain, f'option_chain_{ticker}')
                        save_to_parquet(option_chain, f'option_chain_{ticker}.parquet')
                    financial = fetch_financial_statements(ticker)
                    if financial is not None:
                        with lock:
                            financials[ticker] = financial
                        for statement_type, df in financial.items():
                            safe_ticker = ticker.replace(".", "_")
                            save_to_csv(df, f'{statement_type}_{safe_ticker}.csv')
                            save_to_sqlite(df, f'{statement_type}_{safe_ticker}')
                            save_to_mysql(df, f'{statement_type}_{safe_ticker}')
                            save_to_parquet(df, f'{statement_type}_{safe_ticker}.parquet')

    # Fetch real-time data
    for category, tickers in TICKERS.items():
        if not is_market_open(category) and category != 'crypto':
            logging.info(f"Market closed for {category}, skipping real-time data fetch")
            continue
        for ticker in tickers:
            if category == 'crypto':
                data = fetch_crypto_data(ticker)
            else:
                data = fetch_real_time_stock_data(ticker)
            if data and not np.isnan(data['price']):
                df = pd.DataFrame([{
                    'Timestamp': data['timestamp'],
                    'Ticker': data['ticker'],
                    'Price': data['price']
                }])
                safe_ticker = ticker.replace("/", "_").replace("^", "")
                with lock:
                    if ticker not in realtime_data:
                        realtime_data[ticker] = df
                    else:
                        # Avoid duplicates
                        existing = realtime_data[ticker]
                        if not existing.empty and (existing['Timestamp'] == data['timestamp']).any():
                            continue
                        realtime_data[ticker] = pd.concat([existing, df], ignore_index=True)
                save_to_csv(df, f'realtime_{safe_ticker}.csv', mode='a')
                save_to_sqlite(df, f'realtime_{safe_ticker}', if_exists='append')
                save_to_mysql(df, f'realtime_{safe_ticker}', if_exists='append')
                save_to_parquet(df, f'realtime_{safe_ticker}.parquet')

def run_scheduler():
    """Run scheduled tasks in a background thread."""
    schedule.every(UPDATE_INTERVAL).seconds.do(fetch_and_store_data)
    schedule.every().hour.at(":00").do(send_email_report)  # Schedule email every hour
    schedule.every().day.at("00:00").do(fetch_and_store_data)  # Ensure historical data update at midnight
    # Schedule one-time email after 1 hour from script start
    schedule.every(1).hours.do(send_email_report).tag('one-time-email').next_run = datetime.now(IST) + timedelta(hours=1)
    while True:
        schedule.run_pending()
        time.sleep(1)

# Start scheduler in a separate thread
scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
scheduler_thread.start()

# Dash app layout
app.layout = html.Div([
    html.H1('Advanced Market Dashboard', style={'textAlign': 'center'}),
    html.Div(id='status', style={'textAlign': 'center', 'color': 'green'}),
    dcc.Tabs([
        dcc.Tab(label='Real-Time Prices', children=[
            dcc.Dropdown(
                id=' ticker-dropdown',
                options=[{'label': ticker, 'value': ticker} for ticker in TICKERS['stocks'] + TICKERS['indices'] + TICKERS['crypto'] + TICKERS['commodities']],
                value=TICKERS['stocks'][0],
                style={'width': '50%'}
            ),
            html.Div(id='real-time-prices'),
            dcc.Interval(id='price-update', interval=UPDATE_INTERVAL*1000, n_intervals=0)
        ]),
        dcc.Tab(label='Historical Charts', children=[
            dcc.Graph(id='historical-candle-chart')
        ]),
        dcc.Tab(label='Option Chain (AAPL)', children=[
            html.Div(id='option-chain-table')
        ]),
        dcc.Tab(label='Analytics', children=[
            dash_table.DataTable(id='analytics-table'),
            dcc.Graph(id='analytics-chart')
        ])
    ])
])

@app.callback(
    Output('status', 'children'),
    Input('price-update', 'n_intervals')
)
def update_status(n_intervals):
    """Update dashboard status."""
    mysql_status = "MySQL: Connected" if MYSQL_ENABLED else "MySQL: Disconnected"
    return f"Last update: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S')} | {mysql_status}"

@app.callback(
    Output('real-time-prices', 'children'),
    [Input('price-update', 'n_intervals'), Input('ticker-dropdown', 'value')]
)
def update_real_time_prices(n_intervals, selected_ticker):
    """Update real-time prices."""
    with lock:
        prices = []
        for ticker, df in realtime_data.items():
            if not df.empty and (ticker == selected_ticker or not selected_ticker):
                latest = df.iloc[-1]
                prices.append(f"{ticker}: ${latest['Price']:.2f} ({latest['Timestamp']})")
        return [html.P(price) for price in prices]

@app.callback(
    Output('historical-candle-chart', 'figure'),
    Input('ticker-dropdown', 'value')
)
def update_historical_candle_chart(selected_ticker):
    """Update candlestick chart with indicators."""
    with lock:
        data = historical_data.get(selected_ticker, pd.DataFrame())
    if data.empty:
        return {}
    traces = [
        go.Candlestick(
            x=data['Date'],
            open=data['Open'],
            high=data['High'],
            low=data['Low'],
            close=data['Close'],
            name=selected_ticker
        ),
        go.Scatter(x=data['Date'], y=data['SMA20'], name='SMA20', line=dict(color='blue')),
        go.Scatter(x=data['Date'], y=data['BB_High'], name='BB Upper', line=dict(color='purple', dash='dash')),
        go.Scatter(x=data['Date'], y=data['BB_Low'], name='BB Lower', line=dict(color='purple', dash='dash'))
    ]
    return {
        'data': traces,
        'layout': go.Layout(
            title=f'Historical Chart for {selected_ticker}',
            xaxis={'title': 'Date'},
            yaxis={'title': 'Price (USD)'},
            hovermode='closest'
        )
    }

@app.callback(
    Output('option-chain-table', 'children'),
    Input('price-update', 'n_intervals')
)
def update_option_chain_table(n_intervals):
    """Update option chain table for AAPL."""
    with lock:
        if 'AAPL' in option_chains:
            return dash_table.DataTable(
                data=option_chains['AAPL'].to_dict('records'),
                columns=[{'name': col, 'id': col} for col in option_chains['AAPL'].columns],
                style_table={'overflowX': 'auto'}
            )
    return html.P("No option chain data available for AAPL")

@app.callback(
    [Output('analytics-table', 'data'), Output('analytics-chart', 'figure')],
    Input('ticker-dropdown', 'value')
)
def update_analytics(selected_ticker):
    """Update analytics table and chart."""
    with lock:
        data = historical_data.get(selected_ticker, pd.DataFrame())
    if data.empty:
        return [], {}
    
    # Analytics table
    latest = data.iloc[-1] if not data.empty else {}
    table_data = [{
        'Metric': metric,
        'Value': f"{latest.get(metric, 0):.2f}" if metric in latest else 'N/A'
    } for metric in ['Close', 'SMA20', 'Volatility', 'RSI', 'MACD', 'BB_High', 'BB_Low']]
    
    # Analytics chart
    traces = [
        go.Scatter(x=data['Date'], y=data['RSI'], name='RSI', yaxis='y1'),
        go.Scatter(x=data['Date'], y=data['MACD'], name='MACD', yaxis='y2')
    ]
    
    return table_data, {
        'data': traces,
        'layout': go.Layout(
            title=f'Analytics for {selected_ticker}',
            yaxis={'title': 'RSI', 'domain': [0.6, 1]},
            yaxis2={'title': 'MACD', 'domain': [0, 0.4]},
            xaxis={'title': 'Date'},
            hovermode='closest'
        )
    }

# Test MySQL connection
MYSQL_ENABLED = test_mysql_connection()

if __name__ == '__main__':
    logging.info("Starting Dash app...")
    app.run(debug=False, host='0.0.0.0', port=8050)