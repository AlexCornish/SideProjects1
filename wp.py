import BLS_Request
import os
import pyarrow.parquet as pq
import pandas as pd
import csv
path = str(os.path.dirname(os.path.realpath(__file__)))
quartersArr = ["M01M02M03","M04M05M06","M07M08M09","M10M11M12"]

class dataRow:
    def __init__(self,seriesID, year, period, value, percentageChange,surveyAbbr, group_code, group_name, item_code, item_name, seasonal, timePeriod):
        self.seriesID = seriesID # The full code that comes as default in both of the Current files. Usually looks like PCU1133--1133
        self.year = year # The year of the report.
        self.period = period # The month the report came out, usually in MXX format, with XX being the number of the month.
        self.value = value
        self.percentageChange = percentageChange
        self.surveyAbbr = surveyAbbr # The first two letters of series ID that indicates if it is industry (pc) or commodity (wp) data.
        self.seasonal = seasonal # A single letter, either 'S' and 'U' that indicates whether it is seasonally adjusted (S) or not seasonally adjusted (U).
        self.group_code = group_code # The first 6 numbers of the seriesID after the seasonal indicator, represents industry or group that the product or item belongs to.
        self.group_name = group_name # Name of the industry/group that corresponds to the industry_group_code.
        self.item_code = item_code # The next 6 digits following the industry_group_code, represents the product or item code for the item.
        self.item_name = item_name # Name of the product/item that corresponds to the product_item_code.
        self.timePeriod = timePeriod # A formatted publication date, in the format of YYYY-MM-01
    def __str__(self):
        return "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (self.seriesID,self.year,self.period,self.value,self.surveyAbbr,self.seasonal,self.group_code,self.group_name,self.item_code, self.item_name, self.percentageChange, self.timePeriod)

class labelStorage:
    def __init__(self, item_name, item_Dict):
        self.item_name = item_name
        self.item_Dict = item_Dict
    def __str__(self):
        return "Test: Item Name:%s Dict:%s" % (self.item_name,self.item_Dict)

class quarters:
    def __init__(self, q1, q2, q3, q4):
        self.q1 = q1
        self.q2 = q2
        self.q3 = q3
        self.q4 = q4
    def __str__(self):
        return "Q1:%s Q2:%s Q3:%s Q4:%s" % (self.q1,self.q2,self.q3,self.q4)

def checkForLatestVersion():
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpCur","Current")

def readParquet(fileName):
    return pq.read_table(fileName).to_pandas()

def writeToCSV(fileName,data):
    tempName = fileName[:-8] + ".csv"
    with open(tempName,'w',newline='') as newFile:
        wr = csv.writer(newFile,delimiter=',')
        wr.writerows(formatString(str(x)).split(',') for x in data)
        newFile.close()

def formatTimePeriod(year,monthPeriod):
    # This should be formatted yyyy-mm-01
    return year + "-" + monthPeriod[1:] + "-01"

def specialRounding(currentNum, previousNum):
    return round((currentNum-previousNum),1)

def quarteriseDataFrame(dataFrame):
    newDF = []
    dfList = dataFrame.values.tolist()
    iterList = iter(dfList)
    next(iterList)
    quarterDict = {}
    for j in iterList:
        newRowQrt = dataRow(j[0],j[1],j[2],j[3],"","","","","","","","")
        if newRowQrt.seriesID not in quarterDict:
            quarterDict[newRowQrt.seriesID] = {}
        if newRowQrt.year not in quarterDict[newRowQrt.seriesID]:
            quarterDict[newRowQrt.seriesID][newRowQrt.year] = quarters([],[],[],[])
        for m in range(0,len(quartersArr)):
            if newRowQrt.period in quartersArr[m]:
                if m == 0:
                    quarterDict[newRowQrt.seriesID][newRowQrt.year].q1.append(float(newRowQrt.value))
                elif m == 1:
                    quarterDict[newRowQrt.seriesID][newRowQrt.year].q2.append(float(newRowQrt.value))
                elif m == 2:
                    quarterDict[newRowQrt.seriesID][newRowQrt.year].q3.append(float(newRowQrt.value))
                elif m == 3:
                    quarterDict[newRowQrt.seriesID][newRowQrt.year].q4.append(float(newRowQrt.value))
    for x in quarterDict:
        for k in quarterDict[x]:
            newDF.append([x,k,"Q1",arrayAvg(quarterDict[x][k].q1),"","","","","","","",""])
            newDF.append([x,k,"Q2",arrayAvg(quarterDict[x][k].q2),"","","","","","","",""])
            newDF.append([x,k,"Q3",arrayAvg(quarterDict[x][k].q3),"","","","","","","",""])
            newDF.append([x,k,"Q4",arrayAvg(quarterDict[x][k].q4),"","","","","","","",""])
    return newDF

def arrayAvg(arr):
    if len(arr) == 0:
        return "-"
    return round(sum(arr)/len(arr),2)

def periodOverPeriodCalculation(dataFrame):
    print("period over period ")
    labelDict = []
    for i in range(0,len(dataFrame)):
        if dataFrame[i].seriesID not in labelDict:
            labelDict.append(dataFrame[i].seriesID)
            dataFrame[i].percentageChange = "-"
        elif dataFrame[i].value == "-" or dataFrame[i-1].value == "-":
            dataFrame[i].percentageChange = "-"
        else:
            dataFrame[i].percentageChange = specialRounding(float(dataFrame[i].value),float(dataFrame[i-1].value))
    return dataFrame
        

def createCustomFormattedDataFrame(dataFrame):
    columnTitlesSet = False
    newDataFrame = []
    outputFrame = []
    dfList = dataFrame.values.tolist()
    titleRow = dataRow("seriesID","year","period","value","","","","","","","","")

    print("For each of these options type 0 for yes or 1 for no:")
    avgOverQrt = int(input("Would you like the values averaged over quarters?: "))
    if avgOverQrt == 1:
        dfList = quarteriseDataFrame(dataFrame)
        titleRow.period = "quarter"
        titleRow.value = "quarterly average value"
    else:
        timeFormat = int(input("Would you like the dates converted to yyyy-mm-01 format?: "))
        m13Drop = int(input("Would you like to drop all M13 periods?: "))
    percentageChg = int(input("Would you like to add the percentage change between periods?: "))
    labelAdd = int(input("Would you like to add labels for each level?: "))
    indCom = {}
    #___________________________________Label creation______________________________________
    if labelAdd == 1:
        newPath = path + '\\RawData\\' + BLS_Request.getLatestVersionFileName("wpLRef",BLS_Request.getAllFilesInDirectory("wpLRef"))
        newDataFrame = readParquet(newPath)
        newDfList = newDataFrame.values.tolist()
        for row in newDfList:
            if row[1] == "-":
                indCom[row[0]] = labelStorage(row[2],{})
            elif row[0] not in indCom.keys():
                indCom[row[0]] = labelStorage(row[2],{})
                indCom.get(row[0]).item_Dict[row[1]] = row[2]
            else:
                if row[0] not in indCom:
                    indCom[row[0]] = labelStorage(row[2],{})
                else:
                    indCom.get(row[0]).item_Dict[row[1]] = row[2]


    codeSplit = int(input("Would you like to split all the id codes?: "))
    seasonColumn = int(input("Would you like to add a column for seasonal codes?: "))
    iterList = iter(dfList)
    
    next(iterList)
    for i in iterList: 
        newRow = dataRow(i[0],i[1],i[2],i[3],"","","","","","","","")
        if labelAdd == 1:
            for i in indCom.keys():
                if i in newRow.seriesID:
                    newRow.group_name = indCom.get(i).item_name
                    for j in indCom.get(i).item_Dict.keys():
                        if j in newRow.seriesID:
                            newRow.item_name = indCom.get(i).item_Dict[j]
            if columnTitlesSet == False:
                titleRow.group_name = "group_name"
                titleRow.item_name = "item_name"
        if codeSplit == 1:
            newRow.surveyAbbr = newRow.seriesID[:2]
            newRow.group_code = newRow.seriesID[3:5]
            newRow.item_code = newRow.seriesID[5:]
            if columnTitlesSet == False:
                titleRow.surveyAbbr = "surveyAbbr"
                titleRow.group_code = "group_code"
                titleRow.item_code = "item_code"
            if seasonColumn == 1:
                newRow.seasonal = newRow.seriesID[2:3]
            if columnTitlesSet == False:
                titleRow.seasonal = "seasonal"
        if avgOverQrt == 0:
            if timeFormat == 1:
                newRow.timePeriod = formatTimePeriod(newRow.year,newRow.period)
                if columnTitlesSet == False:
                    titleRow.timePeriod = "timePeriod"
            if m13Drop == 1:
                if newRow.period != "M13":
                    if columnTitlesSet == False:
                        outputFrame.append(titleRow)
                        columnTitlesSet = True
                    outputFrame.append(newRow)
            else:
                if columnTitlesSet == False:
                        outputFrame.append(titleRow)
                        columnTitlesSet = True
                outputFrame.append(newRow)
        else:
            if columnTitlesSet == False:
                outputFrame.append(titleRow)
                columnTitlesSet = True
            outputFrame.append(newRow)
    if percentageChg == 1:
        outputFrame = periodOverPeriodCalculation(outputFrame)
        titleRow.percentageChange = "Percentage Change"
    return outputFrame

def formatString(stringToChange):
    while ",," in stringToChange:
        stringToChange = stringToChange.replace(",,",",")
    if stringToChange[len(stringToChange)-1] == ",":
        stringToChange = stringToChange[:-1]
    return stringToChange

def wpProcessing():
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpCur","Current")
    newPath = path + '\\RawData\\' + BLS_Request.getLatestVersionFileName("wpCur",BLS_Request.getAllFilesInDirectory("wpCur"))
    dataFrame = readParquet(newPath)
    data = createCustomFormattedDataFrame(dataFrame) 
    writeToCSV(newPath,data)

wpProcessing()
