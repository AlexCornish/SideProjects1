import requests
import datetime
import pandas as pd
import urllib3
import re
import pyarrow as pa
import pyarrow.parquet as pq
import os
 
path = str(os.path.dirname(os.path.realpath(__file__))) 
BLS_BASE_URL = "https://download.bls.gov/pub/time.series/"
urlDict = {
    "pc": "pc",
    "pcCur": "pc/pc.data.0.Current",
    "pcInd": "pc/pc.industry",
    "pcLRef": "pc/pc.product",
    "wp": "wp",
    "wpCur": "wp/wp.data.0.Current",
    "wpLRef": "wp/wp.item",
    "wpGrp": "wp/wp.group"
}

def checkForLatestVersion(wpOrpc,fileNameToCheckFor):
    # wpOrpc: Indicates whether the data to be accessed is from wp (commodity) or pc (industry).
    # fileNameToCheckFor: 
    # Gets the main downloads page from which the time of latest update can be accessed
    url = os.path.join(BLS_BASE_URL,urlDict[wpOrpc])
    # The URL is selected
    page = requests.get(url)
    # The text access through the request gets converted to a string.
    tempString = str(page.text)
    tempString = tempString.split()
    latestDate = ""
    for i in range(1,len(tempString)):
        if fileNameToCheckFor in tempString[i]:
            for j in range(i-5, i-2):
                latestDate += tempString[j] + " "
    return convertToDateObj(latestDate)

def pmConverter(dateTimeStr):
    dateTimeStr = datetime.datetime.strptime(dateTimeStr[:-4], '%m/%d/%Y %H:%M')
    dateTimeStr = dateTimeStr.replace(hour=dateTimeStr.hour+12)
    return dateTimeStr

def convertToDateObj(dateTimeStr):
    if "PM" in dateTimeStr:
        return convertFormat(str(pmConverter(dateTimeStr))[:-3])
    timeStr = str(datetime.datetime.strptime(dateTimeStr[:-4], '%m/%d/%Y %H:%M'))[:-3]
    return convertFormat(timeStr)

def convertFormat(dateTimeStr):
    dateTimeStr = dateTimeStr.replace(" ","_").replace(":","_").replace("-","_")
    return dateTimeStr

def getBLSData(url, wpOrpc):
    http = urllib3.PoolManager()
    r = http.request('GET',url)
    tempInfo = r.data.decode("utf-8")
    tempArr = []
    tempInfo = tempInfo.splitlines()
    for j in tempInfo:
        row = re.split(r'\t+',j)
        for k in range(0,len(row)):
            row[k] = row[k].strip()
        tempArr.append(row)
    return tempArr
  
def compareLatestOnlineVersionWithLatestDownloadedVersion(wpOrpc,fileNameToCheckFor):
    downloadDate, downloadTime = determineLatestVersionDownloaded(getAllFilesInDirectory(wpOrpc))
    if downloadDate != datetime.date.fromtimestamp(0):
        fileName = checkForLatestVersion(wpOrpc[:2],urlDict[wpOrpc][3:]).split("_")
        newVerDate = datetime.date(int(fileName[0]),int(fileName[1]),int(fileName[2]))
        newVerTime = datetime.time(int(fileName[3]),int(fileName[4]))
        if newVerDate == downloadDate and newVerTime == downloadTime:
            print("Latest version is already downloaded.")
        else:
            url = os.path.join(BLS_BASE_URL,urlDict[wpOrpc])
            getAndFormatData(url,wpOrpc,(newVerDate,newVerTime))
    else:
        url = os.path.join(BLS_BASE_URL,urlDict[wpOrpc])
        fileName = checkForLatestVersion(wpOrpc[:2],urlDict[wpOrpc][3:]).split("_")
        newVerDate = datetime.date(int(fileName[0]),int(fileName[1]),int(fileName[2]))
        newVerTime = datetime.time(int(fileName[3]),int(fileName[4]))
        getAndFormatData(url,wpOrpc,(newVerDate,newVerTime))

def checkForIndustryOrCommodity(wpOrpc, newPath): 
    currentPath = ""
    if wpOrpc == "pcCur":
        currentPath = newPath + '\\Industry'
    elif wpOrpc == "pcLRef" or wpOrpc == "pcInd":
        currentPath = newPath + '\\Industry\\Labels'
    elif wpOrpc == "wpCur":
        currentPath = newPath + '\\Commodity'
    elif wpOrpc == "wpLRef" or wpOrpc == "wpGrp":
        currentPath = newPath + '\\Commodity\\Labels'
    if not os.path.exists(currentPath):
        os.makedirs(currentPath)
        return currentPath
    else:
        return currentPath
        
def getAllFilesInDirectory(wpOrpc):
    filesInDirectory = []
    newPath = path + '\\RawData'
    if not os.path.exists(newPath):
        os.makedirs(newPath)
    currentPath = checkForIndustryOrCommodity(wpOrpc,newPath)
    for file in os.listdir(currentPath):
        if file.endswith(".parquet"): 
            if wpOrpc == "pcCur" and "industry" in file:
                filesInDirectory.append(file)
            elif wpOrpc == "wpCur" and "commodity" in file:
                filesInDirectory.append(file)
            elif wpOrpc == "pcLRef" and "labels" in file:
                filesInDirectory.append(file)
            elif wpOrpc == "wpLRef" and "labels" in file:
                filesInDirectory.append(file)
            elif wpOrpc == "wpGrp" and "groupLabels" in file:
                filesInDirectory.append(file)
            elif wpOrpc == "pcInd" and "industryLabels" in file:
                filesInDirectory.append(file)
    return filesInDirectory

def determineLatestVersionDownloaded(filesInDirectory):
    latestTime = datetime.time()
    latestDate = datetime.date.fromtimestamp(0)
    for fileName in filesInDirectory:
        date, time = extractTimeFromFileName(fileName)
        if date > latestDate:
            latestDate = date
            latestTime = time
    return latestDate, latestTime

def extractTimeFromFileName(fileName):
    fileName = fileName[:-8].split("_")
    extractedDate = datetime.date(int(fileName[2]),int(fileName[3]),int(fileName[4]))
    extractedTime = datetime.time(int(fileName[5]),int(fileName[6]))
    return extractedDate, extractedTime
    
def convertRawDataTOPyArrowFormat(rawData, wpOrpc, newVerDateTime):
    tempName = path + '\\RawData'
    fileName = createFileName(newVerDateTime,wpOrpc) + ".parquet"
    tempName = checkForIndustryOrCommodity(wpOrpc,tempName) + "\\" + fileName
    df = pd.DataFrame(rawData)
    table = pa.Table.from_pandas(df)
    pq.write_table(table,tempName)

def getLatestVersionFileName(wpOrpc,filesInDirectory):
    latestTime = datetime.time()
    latestName = ""
    latestDate = datetime.date.fromtimestamp(0)
    if len(filesInDirectory) != 0:
        for fileName in filesInDirectory:
            date, time = extractTimeFromFileName(fileName)
            if date > latestDate:
                latestDate = date
                latestName = fileName
                latestTime = time
        if wpOrpc == "pcCur":
            return "Industry\\" + fileName
        elif wpOrpc == "wpCur":
            return "Commodity\\" + fileName
        elif wpOrpc == "pcLRef":
            return "Industry\\Labels\\" + fileName
        elif wpOrpc == "pcInd":
            return "Industry\\Labels\\" + fileName
        elif wpOrpc == "wpLRef":
            return "Commodity\\Labels\\" + fileName
        elif wpOrpc == "wpGrp":
            return "Commodity\\Labels\\" + fileName
    else:
        print("help")

def createFileName(latestVersionDate,wpOrpc):
    # wp (commodity) and pc (industry)
    dateStr = str(latestVersionDate[0]) + "-" + str(latestVersionDate[1])
    latestVersionDate = dateStr.replace("-","_").replace(":","_")[:-3]
    if wpOrpc == "pcCur":
        return "industry_data_" + latestVersionDate
    elif wpOrpc == "pcLRef":
        return "industry_labels_" + latestVersionDate
    elif wpOrpc == "wpCur":
        return "commodity_data_" + latestVersionDate
    elif wpOrpc == "wpLRef":
        return "commodity_labels_" + latestVersionDate
    elif wpOrpc == "wpGrp":
        return "commodity_groupLabels_" + latestVersionDate
    elif wpOrpc == "pcInd":
        return "industry_industryLabels_" + latestVersionDate

def getAndFormatData(url,wpOrpc,newVerDateTime):
    newBLSData = getBLSData(url, wpOrpc)
    convertRawDataTOPyArrowFormat(newBLSData,wpOrpc,newVerDateTime)
