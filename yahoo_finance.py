import json
import urllib.request, json , time, os
import pandas as pd
from multiprocessing.dummy import Pool
from datetime import datetime
import http.client as httplib

tickers = ['DIXON', 'MINDAIND', 'ROUTE', 'AMBER', 'REDINGTON', 'INDIAMART']

query_urls = [f"https://query1.finance.yahoo.com/v8/finance/chart/{i}.NS?symbol={i}.NS&period1=0&period2=9999999999&interval=1d&includePrePost=true&events=div%2Csplit" for i in tickers]
json_path = os.getcwd()+os.sep+"data"+os.sep+"json"+os.sep
csv_path = os.getcwd()+os.sep+"data"+os.sep+"csv"+os.sep

def get_date(utcformat):
   return datetime.utcfromtimestamp(int(utcformat)).strftime('%d-%m-%Y %H:%M:%S')

def check_internet():
    conn = httplib.HTTPConnection("www.google.com", timeout=5)
    try:
        conn.request("HEAD", "/")
        conn.close()
        return True
    except:
        conn.close()
        return False

def save_stock_data(stock_id,query_url):
    #----------------------------json file------------------------------------------------------
    try:
        with urllib.request.urlopen(query_url) as url:
            parsed = json.loads(url.read().decode())
        with open(json_path + stock_id + '.json', 'w') as outfile:
            json.dump(parsed, outfile, indent=4)
    except Exception as e:
        print(e)
        print("Historical data of "+ stock_id + " doesn't exist")
        return

    #---------------------------------Dataframe--------------------------------------------------
    try: 
        Date=[]
        for i in parsed['chart']['result'][0]['timestamp']:
            Date.append(datetime.utcfromtimestamp(int(i)))   #.strftime('%d-%m-%Y'))

        Low=parsed['chart']['result'][0]['indicators']['quote'][0]['low']
        Open=parsed['chart']['result'][0]['indicators']['quote'][0]['open']
        Volume=parsed['chart']['result'][0]['indicators']['quote'][0]['volume']
        High=parsed['chart']['result'][0]['indicators']['quote'][0]['high']
        Close=parsed['chart']['result'][0]['indicators']['quote'][0]['close']
        
        df=pd.DataFrame(list(zip(Date,Low,High,Open,Close,Volume)),columns =['Date','Low','Open','Volume','High','Close'])
        
        if os.path.exists(csv_path+stock_id+'.csv'):
            os.remove(csv_path+stock_id+'.csv')
        df.to_csv(csv_path+stock_id+'.csv', sep=',', index=None)
        print(">>>  Historical data of "+stock_id+" saved")
    except Exception as e:
        print(e)
        print("Historical data of "+stock_id+" exists but has no trading data")

def main():
    if check_internet() == False:
        print("Check Internet connections :(")
        return
    else:
        with Pool(processes=len(tickers)) as pool:
            pool.starmap(save_stock_data, zip(tickers,query_urls))
        print("All downloads completed !")

if __name__ == "__main__":
    main()

