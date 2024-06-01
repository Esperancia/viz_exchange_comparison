import yfinance as yf
from pymongo import MongoClient
import pandas as pd
from flask import Flask, render_template, request
import matplotlib
import matplotlib.pyplot as plt
from io import BytesIO
import base64


# List of stock tickers and time intervals to retrieve historical data
tickers = ['XOM', 'CL=F']
# mongoServerUrl = 'mongodb://localhost' # if tested outside container. mongodb://mongo from Docker

# Connect to MongoDB
client = MongoClient('mongodb://localhost', 27017)
db = client['stock_data']

app = Flask(__name__, template_folder='templates')

# Fetch and store historical data in respective interval collections
def fetch_and_store_data(ticker):
    interval = '1d'

    try:
        stock = yf.Ticker(ticker)

        # Fetch historical data for the given interval (maximum available data)
        data = stock.history(period='max', interval=interval)

        collection_name = f'stock_data_{ticker}'
        collection = db[collection_name]
        for index, row in data.iterrows():
            timestamp = index.strftime('%Y-%m-%d %H:%M:%S')
            record = {
                "timestamp": timestamp,
                "open": round(row['Open'], 3),
                "high": round(row['High'], 3),
                "low": round(row['Low'], 3),
                "close": round(row['Close'], 3),
                "volume": row['Volume']
            }
            # Insert data into MongoDB, avoiding duplicates
            collection.update_one({"timestamp": timestamp}, {"$set": record}, upsert=True)
    except Exception as e:
        print(f"An error occurred while fetching or storing data for {ticker}: {e}")


def prepare_dataframe_collection():
    df_collection = 'viz_query_for_stock_data'

    xom_collection = 'stock_data_XOM'
    clf_collection = 'stock_data_CL=F'

    db[xom_collection].aggregate([
        { '$unset': ["open", "high", "low", "_id", "volume"] },
        {
            '$set': {
                'close_xom': '$close',
                'close': '$$REMOVE'
            }
        },
        {
            '$lookup':
            {
                'from': f'{clf_collection}',
                'localField': "timestamp",
                'foreignField': "timestamp",
                'as': "clf_data"
            }
        },
        {
            '$unwind': "$clf_data"
        },
        {
            '$project': {
                "__v": 0,
                "clf_data.open": 0,
                "clf_data.high": 0,
                "clf_data.low": 0,
                "clf_data._id": 0,
                "clf_data.volume": 0,
                "clf_data.timestamp": 0
            }
        },
        {
            '$set': {
                'close_clf': '$clf_data.close',
                'clf_data': '$$REMOVE'
            }
        },
        {'$out': f'{df_collection}'}
        ])


# visualisation
@app.route('/visualize', methods=['GET', 'POST'])
def index():
    '''
    matplotlib.use('Agg')

    df_collection = 'viz_query_for_stock_data'
    df = pd.DataFrame(list(db[df_collection].find()))
    df.head()

    if df.empty:
        print('DataFrame is empty!')
    else:
        print('coco')
    '''

    # https://stackoverflow.com/questions/51556953/how-to-plot-time-as-x-axis-in-pandas
    img = BytesIO()

    df_collection = 'viz_query_for_stock_data'
    df = pd.DataFrame(list(db[df_collection].find()))

    df["timestamp"] = pd.to_datetime(df['timestamp'])
    df.plot(x="timestamp", y=["close_xom", "close_clf"])

    plt.savefig(img, format='png')
    plt.close()
    img.seek(0)
    dataframe_url = base64.b64encode(img.getvalue()).decode('utf8')

    return render_template('index.html', dataframe=dataframe_url)



# Execution
if __name__ == '__main__':
    # for ticker in tickers:
        # fetch_and_store_data(ticker)
    # prepare_dataframe_collection()
    app.run(host='0.0.0.0', port=5000, debug=True)
