import requests
import json
import time
import base64
import math
import numpy as np
import pandas as pd
import os
iM,iH,iD = 6*(10**4),36*(10**5),864*(10**5)
intervals = {
            '1m':iM,'3m':iM*3,'5m':iM*5,'15m':iM*15,'30m':iM*30,
            '1h':iH,'2h':iH*2,'4h':iH*4,'6h':iH*6,'8h':iH*8,'12h':iH*12,
            '1d':iD,'3d':iD*3,
            '1w':6048*(10**5),
            '1M':187488*(10**5),
        }
columns = ['date', 'Open','High','Low','Close','Volume','closetime', 'quote_asset_volume','trades','taker_buy_base_asset_volume','taker_buy_quote_asset_volume','ignore']

class KlineCollector:
    
    def __init__(self,sym,interval,saveDir=None,verbose=True):
                
        try:
            self.inter = intervals[interval]
        except KeyError:
            raise KeyError("Invalid Interval: Pick valid intervals are ",intervals.keys())
        
        self.endpoint = 'https://api.binance.com/api/v1/klines?symbol='+sym+'&interval='+interval

        if saveDir is None:
             saveDir = "./FinanceData"
                
        self.filePath = saveDir+"/"+sym+"_"+interval+".csv"
        if 'M' in interval:
            self.filePath = saveDir+"/"+sym+"_"+interval+"onth.csv"
        
        if os.path.exists(self.filePath):
            self.df = pd.read_csv(self.filePath,dtype=float,low_memory=True)
            self.organiseDF()
            self.length = len(self.df)
            if verbose:print("Using file ",self.filePath);
        else:
            self.df = pd.DataFrame(columns=columns,dtype=float)
            self.length = 0 
            if os.path.exists(saveDir):
                if verbose:print("Creating CSV file ",self.filePath)
            else:
                if verbose:
                    print("Creating Directory and CSV file  ",self.filePath)
                os.makedirs(saveDir)
                    
    def getAll(self,verbose=True):
        if verbose:
            print("Collecting all data. Please wait...")
            print("Increased by: %d Current Dataset Size: %d\n"%(self.collect()+self.collect(False),self.length))
        else:
            self.collect()
            self.collect(False)

    def update(self,verbose=True):
        if verbose:
            print("Collecting recent data. Please wait...")
            print("Increased by: %d Current Dataset Size: %d\n"%(self.collect(),self.length))
        else:
            self.collect()

    def collect(self,recent=True):
        initLength = self.length
        
        while True:
            data = self.getData(recent)
            if len(data) > 0:
                self.appendData(data)
                if len(self.df)-self.length == 0:
                    self.df.to_csv(self.filePath,index=False)
                    break

                self.length = len(self.df)  
                time.sleep(.1)
            else:
                break
                
        self.length = len(self.df) 
        return self.length-initLength
        
    def getData(self,recent):
        url = self.endpoint
        if self.length != 0:
            if recent:
                url = self.endpoint+'&startTime='+str(int(self.df['date'].values[-1]))
            else:
                url = self.endpoint+'&startTime='+str(int(self.df['date'].values[0]-self.inter))
                
        resp = requests.get(url)
        respData = json.loads(resp.text)
                  
        if resp.status_code == 200:
            return np.array(respData)
        else:
            self.df.to_csv(self.filePath,index=False)
            raise ValueError("ERROR >> Message: {} Code: {} ".format(respData["msg"],respData["code"]))
    def appendData(self,data):
        self.df = pd.concat([self.df,pd.DataFrame(data,columns=columns,dtype=float)],ignore_index=False)
        self.organiseDF()
    def organiseDF(self,sort=True,dropDuplicates=True):
        self.df.sort_values(['date'],ascending=True,axis=0,inplace=True)
        self.df.drop_duplicates(['date'],keep='last',inplace=True)