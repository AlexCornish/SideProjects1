import BLS_Request
import os
import pyarrow.parquet as pq
import csv
import pandas as pd
import pyarrow as pa
path = str(os.path.dirname(os.path.realpath(__file__)))

class dataRow:
    def __init__(self,seriesID, year, period, value, surveyAbbr, industry_code, industry_name, product_code, product_name, seasonal, timePeriod):
        self.seriesID = seriesID # The full code that comes as default in both of the Current files. Usually looks like PCU1133--1133
        self.year = year # The year of the report.
        self.period = period # The month the report came out, usually in MXX format, with XX being the number of the month.
        self.surveyAbbr = surveyAbbr # The first two letters of series ID that indicates if it is industry (pc) or commodity (wp) data.
        self.value = value
        self.seasonal = seasonal # A single letter, either 'S' and 'U' that indicates whether it is seasonally adjusted (S) or not seasonally adjusted (U).
        self.industry_code = industry_code # The first 6 numbers of the seriesID after the seasonal indicator, represents industry or group that the product or item belongs to.
        self.industry_name = industry_name # Name of the industry/group that corresponds to the industry_group_code.
        self.product_code = product_code # The next 6 digits following the industry_group_code, represents the product or item code for the item.
        self.product_name = product_name # Name of the product/item that corresponds to the product_item_code.
        self.timePeriod = timePeriod # A formatted publication date, in the format of YYYY-MM-01
    def __str__(self):
         return "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (self.seriesID,self.year,self.period,self.value,self.surveyAbbr,self.seasonal,self.industry_code,self.industry_name,self.product_code, self.product_name, self.timePeriod)

class labelStorage:
    def __init__(self, item_name, item_Dict):
        self.item_name = item_name
        self.item_Dict = item_Dict
    def __str__(self):
        return "Test: Item Name:%s Dict:%s" % (self.item_name,self.item_Dict)

def checkForLatestVersion():
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("pcCur","Current")

def readParquet(fileName):
    return pq.read_table(fileName).to_pandas()

def writeToCSV(fileName,data):
    tempName = fileName[:-8] + ".csv"
    with open(tempName,'w',newline='') as newFile:
        wr = csv.writer(newFile,delimiter=',')
        wr.writerows(formatString(x).split(',') for x in data)
        newFile.close()

def formatTimePeriod(year,monthPeriod):
    # This should be formatted yyyy-mm-01
    return year + "-" + monthPeriod[1:] + "-01"

def createCustomFormattedDataFrame(dataFrame):
    columnTitlesSet = False
    newDataFrame = []
    outputFrame = []
    print("For each of these options type 0 for yes or 1 for no:")
    timeFormat = int(input("Would you like the dates converted to yyyy-mm-01 format?: "))
    m13Drop = int(input("Would you like to drop all M13 periods?: "))
    labelAdd = int(input("Would you like to add labels for each level?: "))
    indCom = {}
    if labelAdd == 1:
        newPath = path + '\\RawData\\' + BLS_Request.getLatestVersionFileName("pcLRef",BLS_Request.getAllFilesInDirectory("pcLRef"))
        newDataFrame = readParquet(newPath)
        newDfList = newDataFrame.values.tolist()
        
        newIterList = iter(newDfList)
        next(newIterList)
        for row in newIterList:
            print(row)
            if row[0] == row[1]:
                indCom[row[0]] = labelStorage(formatLabels(row[2]),{})
                indCom.get(row[0]).item_Dict[row[1]] = formatLabels(row[2])
            elif row[0] not in indCom.keys():
                indCom[row[0]] = labelStorage(formatLabels(row[2]),{})
                indCom.get(row[0]).item_Dict[row[1]] = formatLabels(row[2])
            else:
                indCom.get(row[0]).item_Dict[row[1]] = formatLabels(row[2])
    codeSplit = int(input("Would you like to split all the id codes?: "))
    seasonColumn = int(input("Would you like to add a column for seasonal codes?: "))
    dfList = dataFrame.values.tolist()
    iterList = iter(dfList)
    titleRow = dataRow("seriesID","year","period","value","","","","","","","")
    next(iterList)
    for i in iterList:
        newRow = dataRow(i[0],i[1],i[2],i[3],"","","","","","","")
        if timeFormat == 1:
            newRow.timePeriod = formatTimePeriod(newRow.year,newRow.period)
            if columnTitlesSet == False:
                titleRow.timePeriod = "timePeriod"
        if labelAdd == 1:
            for i in indCom.keys():
                if i in newRow.seriesID:
                    newRow.industry_name = indCom.get(i).item_name
                    for j in indCom.get(i).item_Dict.keys():
                        if j in newRow.seriesID:
                            newRow.product_name = indCom.get(i).item_Dict[j]
            if columnTitlesSet == False:
                titleRow.industry_name = "industry_name"
                titleRow.product_name = "product_name"
        if codeSplit == 1:
            newRow.surveyAbbr = newRow.seriesID[:2]
            newRow.industry_code = newRow.seriesID[3:9]
            newRow.product_code = newRow.seriesID[9:]
            if columnTitlesSet == False:
                titleRow.surveyAbbr = "surveyAbbr"
                titleRow.industry_code = "industry_code"
                titleRow.product_code = "product_code"
        if seasonColumn == 1:
            newRow.seasonal = newRow.seriesID[2:3]
            if columnTitlesSet == False:
                titleRow.seasonal = "seasonal"
        if m13Drop == 1:
            if newRow.period != "M13":
                if columnTitlesSet == False:
                    outputFrame.append(str(titleRow))
                    columnTitlesSet = True
                outputFrame.append(str(newRow))
        else:
            if columnTitlesSet == False:
                    outputFrame.append(str(titleRow))
                    columnTitlesSet = True
            outputFrame.append(str(newRow))
    return outputFrame

def formatString(stringToChange):
    while ",," in stringToChange:
        stringToChange = stringToChange.replace(",,",",")
    if stringToChange[len(stringToChange)-1] == ",":
        stringToChange = stringToChange[:-1]
    return stringToChange

def formatLabels(labelToFormat):
    return labelToFormat.replace(",","_")

def pcProcessing():
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("pcCur","Current")
    newPath = path + '\\RawData\\' + BLS_Request.getLatestVersionFileName("pcCur",BLS_Request.getAllFilesInDirectory("pcCur"))
    dataFrame = readParquet(newPath)
    data = createCustomFormattedDataFrame(dataFrame)
    writeToCSV(newPath,data) 

pcProcessing()