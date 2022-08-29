# =================SENDING HIGHEST DELIVERY DATA INFO ON WHATSAPP- TEST SCRIPT=================================

# !pip install pywhatkit


import pywhatkit as kit
from nsepy import get_history
from datetime import date, datetime
import pandas as pd,time as tt
from dateutil.relativedelta import relativedelta
# import concurrent.futures,multiprocessing
from nsepython import *
from nsetools import *
from tqdm import tqdm
import schedule
from functools import lru_cache

# warnings.filterwarnings('ignore')

months = 12
fno_stocks= ['HDFCAMC', 'ESCORTS', 'HONAUT', 'APOLLOTYRE', 'ICICIGI', 'HDFCLIFE',
             'ADANIPORTS', 'BOSCHLTD', 'MOTHERSON', 'IEX', 'HINDPETRO', 'ADANIENT',
             'MRF', 'EICHERMOT', 'BPCL', 'ABB', 'MGL', 'BANDHANBNK', 'AMBUJACEM',
             'MARUTI', 'ICICIPRULI', 'INTELLECT', 'EXIDEIND', 'MCDOWELL-N', 
             'BATAINDIA', 'ZYDUSLIFE', 'GODREJCP', 'GAIL', 'CONCOR', 'TVSMOTOR',
             'SIEMENS', 'INDIAMART', 'MFSL', 'TATAMOTORS', 'CANFINHOME',
             'IBULHSGFIN', 'TORNTPHARM', 'INDIGO', 'SRF', 'IOC', 'ABCAPITAL',
             'TATAPOWER', 'ASIANPAINT', 'PIDILITIND', 'IGL', 'DEEPAKNTR', 'LAURUSLABS',
             'HEROMOTOCO', 'SUNTV', 'TATACONSUM', 'HINDUNILVR', 'ACC',
             'ABBOTINDIA', 'BIOCON', 'OBEROIRLTY', 'NAM-INDIA', 'DIXON', 'BHEL',
             'PETRONET', 'JUBLFOOD', 'IDEA', 'AMARAJABAT', 'AARTIIND', 'SBILIFE',
             'TRENT', 'GRANULES', 'DLF', 'ASHOKLEY', 'LICHSGFIN', 'SAIL', 'ULTRACEMCO',
             'AUBANK', 'BALKRISIND', 'JINDALSTEL', 'GNFC', 'INDHOTEL', 'DALBHARAT',
             'POLYCAB', 'GMRINFRA', 'BAJAJ-AUTO', 'PAGEIND', 'VOLTAS', 'HDFC', 'ASTRAL', 'PIIND', 'LALPATHLAB', 'HDFCBANK', 'SHREECEM', 'HAL', 'FSL', 'TECHM', 'ATUL', 'BAJAJFINSV', 'CUMMINSIND', 'BEL', 'RAMCOCEM', 'DRREDDY', 'RELIANCE', 'CIPLA', 'ICICIBANK', 'TITAN', 'PNB', 'BERGEPAINT', 'BRITANNIA', 'COFORGE', 'APOLLOHOSP', 'NESTLEIND', 'ITC', 'BALRAMCHIN', 'SBICARD', 'CUB', 'UBL', 'GODREJPROP', 'CANBK', 'SYNGENE', 'LTTS', 'CHOLAFIN', 'COROMANDEL', 'HINDCOPPER', 'DIVISLAB', 'COLPAL', 'COALINDIA', 'ONGC', 'POWERGRID', 'DELTACORP', 'MARICO', 'WHIRLPOOL', 'TORNTPOWER', 'GUJGASLTD', 'AXISBANK', 'TATACHEM', 'TATASTEEL', 'PERSISTENT', 'VEDL', 'HAVELLS', 'WIPRO', 'HCLTECH', 'PEL', 'RECLTD', 'INDUSINDBK', 'NMDC', 'IRCTC', 'LT', 'NTPC', 'KOTAKBANK', 'JKCEMENT', 'SUNPHARMA', 'INFY', 'IPCALAB', 'GSPL', 'NAVINFLUOR', 'LUPIN', 'ALKEM', 'GLENMARK', 'IDFCFIRSTB', 'RBLBANK', 'ABFRL', 'NAUKRI', 'INDUSTOWER', 'UPL', 'MINDTREE', 'DABUR', 'TCS', 'BAJFINANCE', 'CROMPTON', 'SRTRANSFIN', 'CHAMBLFERT', 'FEDERALBNK', 'LTI', 'RAIN', 'MCX', 'PFC', 'INDIACEM', 'OFSS', 'SBIN', 'IDFC', 'BHARTIARTL', 'JSWSTEEL', 'MPHASIS', 'NATIONALUM', 'TATACOMM', 'BSOFT', 'BANKBARODA', 'BHARATFORG', 'AUROPHARMA', 'ZEEL','HINDALCO', 'GRASIM', 'METROPOLIS', 'MANAPPURAM', 'MUTHOOTFIN']


startDate = date.today() - relativedelta(months=months)
# startDate = datetime.fromtimestamp(datetime.timestamp(start))
endDate = date.today()

currMonthlyExpiry,nextMonthlyExpiry = expiry_list("RELIANCE")[0],expiry_list("RELIANCE")[1]
farMonthlyExpiry = expiry_list("RELIANCE")[2]

currExpiryMonth,nextExpiryMonth,farExpiryMonth = currMonthlyExpiry[3:6],nextMonthlyExpiry[3:6],farMonthlyExpiry[3:6]
currExpiryDate,nextExpiryDate,farExpiryDate = currMonthlyExpiry[0:-9],nextMonthlyExpiry[0:-9],farMonthlyExpiry[0:-9]
currExpiryYear,nextExpiryYear,farExpiryYear = currMonthlyExpiry[7:],nextMonthlyExpiry[7:],farMonthlyExpiry[7:]

@lru_cache
def MonthToNum(ShortMonth):
    return {
            'Jan':1,'Feb':2,'Mar':3,'Apr': 4,'May': 5,'Jun': 6,
            'Jul': 7,'Aug': 8,'Sep': 9,'Oct': 10,'Nov': 11,'Dec': 12}[ShortMonth]

def FuturesOIData(symbol):
    currMonthData = get_history(symbol=symbol,start=startDate,end=endDate,futures=True,
                                expiry_date=date(int(currExpiryYear),
                                                 MonthToNum(currExpiryMonth),int(currExpiryDate)))
    nextMonthData = get_history(symbol=symbol,start=startDate,end=endDate,futures=True,
                                expiry_date=date(int(nextExpiryYear),
                                                 MonthToNum(nextExpiryMonth),int(nextExpiryDate)))
    farMonthData = get_history(symbol=symbol,start=startDate,end=endDate,futures=True,
                               expiry_date=date(int(farExpiryYear),
                                                MonthToNum(farExpiryMonth),int(farExpiryDate)))
    
    currMonthData.columns = ['Symbol','Expiry','Open','High','Low','Close','Last',
                           'Settl Price','Number of Contracts','Turnover','Open Interest',
                           'Change in OI','Underlying']
    nextMonthData.columns = ['Symbol','Expiry','Open','High','Low','Close','Last',
                           'Settl Price','Number of Contracts','Turnover','Open Interest',
                           'Change in OI','Underlying']
    farMonthData.columns = ['Symbol','Expiry','Open','High','Low','Close','Last',
                           'Settl Price','Number of Contracts','Turnover','Open Interest',
                           'Change in OI','Underlying']
    
    return currMonthData, nextMonthData,farMonthData


def oiPercChange(stock):
#     for stock in fno_stocks:
    currOIData,nextOIData = FuturesOIData(stock)[0],FuturesOIData(stock)[1]
    farOIData = FuturesOIData(stock)[2]
    compositeOIDf = currOIData.join(nextOIData,lsuffix='_curr',rsuffix='_next')
    compositeOIDf['OI %Change'] = 0
    for i in range(1,len(compositeOIDf)):
        if (compositeOIDf['Open Interest_curr'].iloc[i-1] + compositeOIDf['Open Interest_next'].iloc[i-1]) == 0:
            compositeOIDf['OI %Change'].iloc[i]= 0
        else:
            compositeOIDf['OI %Change'].iloc[i]=100*(compositeOIDf['Change in OI_curr'].iloc[i]+compositeOIDf['Change in OI_next'].iloc[i]
                                                    )/(compositeOIDf['Open Interest_curr'].iloc[i-1]+compositeOIDf['Open Interest_next'].iloc[i-1])

    oiPercChang = compositeOIDf[['Symbol_curr','Symbol_next','OI %Change']].iloc[-1:]
#     print(oiPercChang)
    return oiPercChang

def topN_OI():
    compDf = pd.DataFrame(columns=['Symbol_curr','Symbol_next','OI %Change'])
    for stock in fno_stocks:
        indoiPercChange = oiPercChange(stock)
        compDf=pd.concat([compDf,indoiPercChange])
#     compDf = pd.DataFrame(list_df)# columns= ['Symbol','Expiry','Open','High','Low','Close','Last',
#                                             'Settl Price','Number of Contracts','Turnover',
#                                             'Open Interest','Change in OI','Underlying'])
#     print(compDf.head())
    topNLargestOI = compDf.nlargest(10,'OI %Change')
    topNSmallestOI = compDf.nsmallest(10,'OI %Change')
    
#     print(topNLargestOI)
#     print(topNSmallestOI)
    return topNLargestOI, topNSmallestOI
    
topN_OI()   

phone = "+919082417898"

@lru_cache
def deliverydata(symbol):

    data = get_history(symbol=symbol,start=startDate,
                       end=endDate)
#     PercDel = data[-30:]['%Deliverble'].astype(float)
#     PercMaxDelivery=data.loc[PercDel.idxmax()]['%Deliverble']   

    PercMaxDelivery = data[-30:]['%Deliverble'].max()
    CurrPercDelivery = data['%Deliverble'].iloc[-1:]
    
    CurrVolume = data['Volume'].iloc[-1:]
    data['20DAvgVol'] = data['Volume'].rolling(20).mean()
    AvgVolume = data['20DAvgVol'].iloc[-1:]
    RecentClose,prevHigh,prevLow = data['Close'].iloc[-1:],data['High'].iloc[-1:],data['Low'].iloc[-1:]
    
    data['VWAP_Avg']=data['VWAP'].rolling(20).mean()
    VWAP_Avg = data['VWAP_Avg'].iloc[-1:]
    
    data['VWAP_50DAvg']=data['VWAP'].rolling(50).mean()
    VWAP_50DAvg = data['VWAP_50DAvg'].iloc[-1:]
    
    data['VWAP_100DAvg']=data['VWAP'].rolling(100).mean()
    VWAP_100DAvg = data['VWAP_100DAvg'].iloc[-1:]   
    
    
#     if (data[-30:]['%Deliverble'].idxmax() == date.today()):        
#         print(f'Todays %Delivery Data of {round(PercMaxDelivery,2)} for {symbol} is maximum in last 30 days')
#         print(f'Max Perc Delivery for {symbol}-{round(PercMaxDelivery,2)}')
#     else:
#         print(f'Todays %Delivery Data for {symbol} : {round(PercMaxDelivery,2)} is not maximum  in last 30 days')
        
    return symbol,CurrPercDelivery,PercMaxDelivery,CurrVolume,AvgVolume,RecentClose,float(VWAP_Avg),float(VWAP_50DAvg),float(VWAP_100DAvg),float(prevHigh),float(prevLow)


def HighestDelivery():
    cols = ['Symbol','%Del','30D Max %Del','Vol(Lacs)',
            '20D AvgVol(Lacs)','Close','20D AvgVWAP']#,'OI Perc Chg'
    new_list = []
    delivery = 0
    for stock in range(len(fno_stocks)):
        delivery = deliverydata(fno_stocks[stock])[1]
        maxdelivery = deliverydata(fno_stocks[stock])[2]
        currVolume = deliverydata(fno_stocks[stock])[3]
        avgVolume = deliverydata(fno_stocks[stock])[4]
        recentClose = deliverydata(fno_stocks[stock])[5]
        VWAP_Avg = deliverydata(fno_stocks[stock])[6]
        
#         OIPercChange = FuturesOIData(fno_stocks[stock])
        # Results from FuturesOIData Function
        
        
        new_list.append([fno_stocks[stock],float(delivery)*100,float(maxdelivery)*100,
                         round(float(currVolume/100000),0),round(float(avgVolume/100000),0),
                        round(float(recentClose),2),round(float(VWAP_Avg),2)]) #,OIPercChange
    df = pd.DataFrame(new_list, columns=cols)    
    result = df[(df['%Del']==df['30D Max %Del']) & (df['Vol(Lacs)']>df['20D AvgVol(Lacs)'])]
    print(result)
#     print(df.nlargest(50, '%Del', keep='first'))
    
    return result



def VolumeOI(symbol,threshold):
    oi_data,ltp,crontime=oi_chain_builder(symbol,"latest","compact")
    
    oi_data['CALLS_OI_0']=oi_data[(oi_data.CALLS_OI > 0)]['CALLS_OI']
    oi_data['CallVolumeOIRatio']=oi_data['CALLS_Volume']/oi_data['CALLS_OI_0']
    oi_data['PUTS_OI_0']=oi_data[(oi_data['PUTS_OI']) > 0]['PUTS_OI']
    oi_data['PutVolumeOIRatio'] =oi_data['PUTS_Volume']/oi_data['PUTS_OI_0']
    
    filter = oi_data[(oi_data.CallVolumeOIRatio > threshold) | (oi_data.PutVolumeOIRatio> threshold)]
#     StrikePrice,CallsVolume,CallsOI = filter['Strike Price'],filter['CALLS_Volume'],filter['CALLS_OI']    
#     PutsVolume,PutsOI = filter['PUTS_Volume'],filter['PUTS_OI']
#     CallVolumeOIRatio,PutVolumeOIRatio = filter['CallVolumeOIRatio'],filter['PutVolumeOIRatio']
#     CallsLtp,PutsLtp = filter['CALLS_LTP'],filter['PUTS_LTP']
    
    filter_df = filter[['Strike Price','CALLS_Volume','CALLS_OI',
                       'PUTS_Volume','PUTS_OI','CallVolumeOIRatio','PutVolumeOIRatio',
                       'CALLS_LTP','PUTS_LTP']]

    filter_df['Symbol'] = symbol    

    print(filter_df)
    return filter_df


threshold = 5

def VolumeOIScanner():
    cols = ['Symbol','Strike Price','CALLS_Volume','CALLS_OI',
            'PUTS_Volume','PUTS_OI','CallVolumeOIRatio','PutVolumeOIRatio',
            'CALLS_LTP','PUTS_LTP']
#     new_list = pd.DataFrame(columns=cols)
    new_list=[]

    for stock in range(len(fno_stocks)):
        scan = VolumeOI(fno_stocks[stock],threshold=threshold)
        
        if (scan.empty == False):
            new_list.append(scan)
    df = pd.DataFrame([new_list])

    print(df)
    return df


def sendWhatsappMsg():
    kit.sendwhatmsg(phone,f'Whatsapp Message Bot\n Highest Delivery Scrips with High Volume\n {HighestDelivery()}\n',19,45,70)
    kit.sendwhatmsg(phone,f'Whatsapp Message Bot\n Stocks with Highest OI%Change\n {topN_OI()}\n',19,45,70)
    kit.sendwhatmsg(phone,f' Whatsapp Message Bot\n Stock Options- Highest Vol/OI Ratio\n  {VolumeOIScanner()}\n',19,45,70)
#     kit.sendwhatmsg(phone,f' Whatsapp Message Bot\n Stock Options- Highest Vol/OI Ratio\n  {VolumeOIScanner()}\n',23,55,32)

# print(sendWhatsappMsg())

schedule.every().day.at("19:00").do(sendWhatsappMsg)
# schedule.every(10).seconds.do(sendWhatsappMsg)

while True:
    schedule.run_pending()
    tt.sleep(2)
