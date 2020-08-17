import requests
import datetime
import pandas as pd
import urllib3
import re
import pyarrow as pa
import pyarrow.parquet as pq
import os
#path: Dynamic path which is the current directory where the pc.py program is located.
path = str(os.path.dirname(os.path.realpath(__file__))) 
#BLS_BASE_URL: The base url from which all the data will be accessed. 
BLS_BASE_URL = "https://download.bls.gov/pub/time.series/"
# urlDict: A dictionary that contains all of the end url parts that lead to the data that will be used by this program.
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
    # fileNameToCheckFor: Indicates what file name the function is looking for
    # Gets the main downloads page from which the time of latest update can be accessed
    url = os.path.join(BLS_BASE_URL,urlDict[wpOrpc])
    # The URL is selected
    page = requests.get(url)
    # The text access through the request gets converted to a string.
    tempString = str(page.text)
    tempString = tempString.split()
    latestDate = ""
    # Iterates through the main page
    for i in range(1,len(tempString)):
        # Checks for the file name that is being searched for
        if fileNameToCheckFor in tempString[i]:
            for j in range(i-5, i-2):
                # Gets the latest version date of the file being searched for.
                latestDate += tempString[j] + " "
    # Sends the date found to be converted into a dateTime object.
    return convertToDateObj(latestDate)

# pmConverter: Converts all the time string parameters into 24hr format
def pmConverter(dateTimeStr):
    dateTimeStr = datetime.datetime.strptime(dateTimeStr[:-4], '%m/%d/%Y %H:%M')
    dateTimeStr = dateTimeStr.replace(hour=dateTimeStr.hour+12)
    return dateTimeStr

# convertToDateObj: Converts string to dateTime object.
def convertToDateObj(dateTimeStr):
    if "PM" in dateTimeStr:
        return convertFormat(str(pmConverter(dateTimeStr))[:-3])
    timeStr = str(datetime.datetime.strptime(dateTimeStr[:-4], '%m/%d/%Y %H:%M'))[:-3]
    return convertFormat(timeStr)

# convertFormat: Replaces all symbols that can't be used in file names with underscores.
def convertFormat(dateTimeStr):
    dateTimeStr = dateTimeStr.replace(" ","_").replace(":","_").replace("-","_")
    return dateTimeStr

# getBLSData: Performs a GET request to get the BLS data from the specific URL.
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

# compareLatestOnlineVersionWithLatestDownloadedVersion: Compares the latest online version with the latest downloaded version.
def compareLatestOnlineVersionWithLatestDownloadedVersion(wpOrpc,fileNameToCheckFor):
    # Gets the date and time of the latest downloaded version
    downloadDate, downloadTime = determineLatestVersionDownloaded(getAllFilesInDirectory(wpOrpc))
    # Checks if the latest date of the downloaded version is not the default value for a datetime object, which would indicate that no downloaded version exists.
    if downloadDate != datetime.date.fromtimestamp(0):
        # wpOrpc[:2]: produces either a wp or pc
        # urlDict[wpOrpc][3:]: produces the rest of the string
        fileName = checkForLatestVersion(wpOrpc[:2],urlDict[wpOrpc][3:]).split("_")
        # newVerDate: extracts the date from the filename
        newVerDate = datetime.date(int(fileName[0]),int(fileName[1]),int(fileName[2]))
        # newVerTime: extracts the time from the filename
        newVerTime = datetime.time(int(fileName[3]),int(fileName[4]))
        if newVerDate == downloadDate and newVerTime == downloadTime:
            print("No new data available (current version: " + str(downloadDate) + ")")
        else:
            # Constructs the url with whichever wpOrPc url extract is needed.
            print("Downloading new data (previous version: " + str(downloadDate) + "; new version: " + str(newVerDate) + ")")
            url = os.path.join(BLS_BASE_URL,urlDict[wpOrpc])
            getAndFormatData(url,wpOrpc,(newVerDate,newVerTime))
    else:
        # Constructs the url with whichever wpOrPc url extract is needed.
        url = os.path.join(BLS_BASE_URL,urlDict[wpOrpc])
        # wpOrpc[:2]: produces either a wp or pc
        # urlDict[wpOrpc][3:]: produces the rest of the string
        fileName = checkForLatestVersion(wpOrpc[:2],urlDict[wpOrpc][3:]).split("_")
        # newVerDate: extracts the date from the filename
        newVerDate = datetime.date(int(fileName[0]),int(fileName[1]),int(fileName[2]))
        # newVerTime: extracts the time from the filename
        newVerTime = datetime.time(int(fileName[3]),int(fileName[4]))
        print("Downloading new data (previous version: " + str(downloadDate) + "; new version: " + str(newVerDate) + ")")
        getAndFormatData(url,wpOrpc,(newVerDate,newVerTime))

# checkForIndustryOrCommodity: Determines the path for new file based on the wpOrpc
def checkForIndustryOrCommodity(wpOrpc, newPath): 
    currentPath = ""
    if wpOrpc == "pcCur":
        currentPath = os.path.join(newPath,'Industry')
    elif wpOrpc == "pcLRef" or wpOrpc == "pcInd":
        currentPath = os.path.join(newPath,'Industry','Labels')
    elif wpOrpc == "wpCur":
        currentPath = os.path.join(newPath,'Commodity')
    elif wpOrpc == "wpLRef" or wpOrpc == "wpGrp":
        currentPath = os.path.join(newPath,'Commodity','Labels')
    # Checks if the current path exists.
    if not os.path.exists(currentPath):
        # Creates the file at the current path if it doesn't exist.
        os.makedirs(currentPath)
        return currentPath
    else:
        return currentPath

# Gets all of the files in a directory based on certain criteria. 
def getAllFilesInDirectory(wpOrpc):
    filesInDirectory = []
    newPath = os.path.join(path,'RawData')
    # Checks if "newPath" exists and creates it if it doesn't
    if not os.path.exists(newPath):
        os.makedirs(newPath)
    # Returns a modified path using the checkForIndustryOrCommodity function.
    currentPath = checkForIndustryOrCommodity(wpOrpc,newPath)
    # Iterates through the list of entries contained within the directory specified by the path.
    for file in os.listdir(currentPath):
        # Checks if the file ends in .parquet
        if file.endswith(".parquet"): 
            # Compares using wpOrPC and adds it to the array of filesInDirectory if the condition is true.
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
    # Returns an array of filenames in an array of filesInDirectory.
    return filesInDirectory

# determineLatestVersionDownloaded: Returns the latest downloaded version's time and date 
def determineLatestVersionDownloaded(filesInDirectory):
    # Initialises the datetime.time and datetime.date objects.
    latestTime = datetime.time()
    latestDate = datetime.date.fromtimestamp(0)
    # Iterates through the filesInDirectory array
    for fileName in filesInDirectory:
        # Extracts the time and date from the current fileName
        date, time = extractTimeFromFileName(fileName)
        # Checks if the date of the latests downloaded file is newer than the previous newest date.
        if date > latestDate:
            latestDate = date
            latestTime = time
    # Returns the latestDate and latestTime value.
    return latestDate, latestTime

# extractTimeFromFileName: Extracts time and date from a file name.
def extractTimeFromFileName(fileName):
    # removes the .parquet ending from the file name
    fileName = fileName[:-8].split("_")
    extractedDate = datetime.date(int(fileName[2]),int(fileName[3]),int(fileName[4]))
    extractedTime = datetime.time(int(fileName[5]),int(fileName[6]))
    return extractedDate, extractedTime

# convertRawDataTOPyArrowFormat: Converts the raw data to pyarrow parquet format
def convertRawDataTOPyArrowFormat(rawData, wpOrpc, newVerDateTime):
    # Creates the expanded path
    tempName = os.path.join(path,'RawData')
    # Adds the .parquet ending to the file name
    fileName = createFileName(newVerDateTime,wpOrpc) + ".parquet"
    tempName = os.path.join(checkForIndustryOrCommodity(wpOrpc,tempName),fileName)
    # Creates a dataframe out of the raw data
    df = pd.DataFrame(rawData)
    # Converts a dataframe to pyarrow
    table = pa.Table.from_pandas(df)
    # Writes the table.
    pq.write_table(table,tempName)

# getLatestVersionFileName: Gets the latest version's path.
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
            return os.path.join("Industry",fileName)
        elif wpOrpc == "wpCur":
            return os.path.join("Commodity",fileName)
        elif wpOrpc == "pcLRef":
            return os.path.join("Industry","Labels",fileName)
        elif wpOrpc == "pcInd":
            return os.path.join("Industry","Labels",fileName)
        elif wpOrpc == "wpLRef":
            return os.path.join("Commodity","Labels",fileName)
        elif wpOrpc == "wpGrp":
            return os.path.join("Commodity","Labels",fileName)

# Creates the file name for a file based on the parameters passed in.
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
