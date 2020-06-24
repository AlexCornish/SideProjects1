import BLS_Request
import os
import pyarrow.parquet as pq
import csv
import pandas as pd
import pyarrow as pa
path = str(os.path.dirname(os.path.realpath(__file__)))

class dataRow:
    def __init__(self,seriesID, year, period, surveyAbbr, industry_group_code, industry_group_name, product_item_code, product_item_name, seasonal, timePeriod):
        self.seriesID = seriesID # The full code that comes as default in both of the Current files. Usually looks like PCU1133--1133
        self.year = year # The year of the report.
        self.period = period # The month the report came out, usually in MXX format, with XX being the number of the month.
        self.surveyAbbr = surveyAbbr # The first two letters of series ID that indicates if it is industry (pc) or commodity (wp) data.
        self.seasonal = seasonal # A single letter, either 'S' and 'U' that indicates whether it is seasonally adjusted (S) or not seasonally adjusted (U).
        self.industry_group_code = industry_group_code # The first 6 numbers of the seriesID after the seasonal indicator, represents industry or group that the product or item belongs to.
        self.industry_group_name = industry_group_name # Name of the industry/group that corresponds to the industry_group_code.
        self.product_item_code = product_item_code # The next 6 digits following the industry_group_code, represents the product or item code for the item.
        self.product_item_name = product_item_name # Name of the product/item that corresponds to the product_item_code.
        self.timePeriod = timePeriod # A formatted publication date, in the format of YYYY-MM-01
    def __str__(self):
        return "Test: Series_ID:%s Year:%s Period:%s" % (self.seriesID,self.year,self.period)

def checkForLatestVersion():
    print("Choose an option:")
    print("1: pc        (Industry)")
    print("2: wp        (Commodity)")
    wpORpc = str(input("Type either pc or wp: "))
    if wpORpc == "pc":
        BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("pcCur","Current")
    elif wpORpc == "wp":
        BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpCur","Current")

def readParquet(fileName):
    tempDF = pq.read_table(fileName).to_pandas()
    return tempDF

def writeToCSV(fileName,data):
    tempName = fileName[:-8] + ".csv"
    with open(tempName,'w',newline='') as newFile:
        wr = csv.writer(newFile,delimiter=',')
        wr.writerows(data)

def formatTimePeriod(year,monthPeriod):
    # This should be formatted yyyy-mm-01
    formattedTime = year + "-" + monthPeriod[1:] + "-01"
    return formattedTime

def createCustomFormattedDataFrame(dataFrame, wpORpc):
    columnTitlesSet = False
    newDataFrame = []
    print("For each of these options type 0 for yes or 1 for no:")
    timeFormat = int(input("Would you like the dates converted to yyyy-mm-01 format?: "))
    m13Drop = int(input("Would you like to drop all M13 periods?: "))
    labelAdd = int(input("Would you like to add labels for each level?: "))
    if labelAdd == 1:
        filePart = wpORpc[:2] + "LRef"
        newPath = path + '\\RawData\\' + BLS_Request.getLatestVersionFileName(filePart,BLS_Request.getAllFilesInDirectory(filePart))
        newDataFrame = readParquet(newPath)
        newDfList = newDataFrame.values.tolist()
        indCom = {}
        for row in newDfList:
            if row[1] == "-":
                indCom[row[0]] = row[2]
        print(indCom)

    codeSplit = int(input("Would you like to split all the id codes?: "))
    seasonColumn = int(input("Would you like to add a column for seasonal codes?: "))
    dfList = dataFrame.values.tolist()
    #Figure out how to get from pandas data frame to 2d list
    #______________Iterating through the list_____________________
    for i in dfList:
        newRow = dataRow(i[0],i[1],i[2],"","","","","","","")
        # def __init__(self,seriesID, year, period, surveyAbbr, industry_group_code, 
        # industry_group_name, product_item_code, product_item_name, seasonal, timePeriod):
        if timeFormat == 1:
            newRow.timePeriod = formatTimePeriod(newRow.year,newRow.period)
        elif labelAdd == 1:
            print("help")
        elif codeSplit == 1:
            newRow.surveyAbbr = newRow.seriesID[:2]
            newRow.industry_group_code = newRow.seriesID[3:9]
            newRow.product_item_code = newRow.seriesID[9:]
        elif seasonColumn == 1:
            newRow.seasonal = newRow.seriesID[2:3]
        #elif m13Drop == 1:
    #______________Splitting the ID code__________________________
    print(newDataFrame)
    return newDataFrame
             
#checkForLatestVersion()
print("Choose an option:")
print("1: pc        (Industry)")
print("2: wp        (Commodity)")
wpORpc = str(input("Type either pc or wp: ")) + "Cur"
BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion(wpORpc,"Current")
newPath = path + '\\RawData\\' + BLS_Request.getLatestVersionFileName(wpORpc,BLS_Request.getAllFilesInDirectory(wpORpc))
#print(BLS_Request.checkForIndustryOrCommodity(wpORpc, newPath))
dataFrame = readParquet(newPath)
data = createCustomFormattedDataFrame(dataFrame, wpORpc)
writeToCSV(newPath,data)