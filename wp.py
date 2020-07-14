import BLS_Request
import os
import pyarrow.parquet as pq
import time 
import pandas as pd
import csv
path = str(os.path.dirname(os.path.realpath(__file__)))
quartersArr = ["M01M02M03","M04M05M06","M07M08M09","M10M11M12"]

class dataRow:
    def __init__(self,seriesID, year, period, value, percentageChange,surveyAbbr, group_code, group_name, item_code, item_name, seasonal, timePeriod, yearOverYear):
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
        self.yearOverYear = yearOverYear
    def __str__(self):
        return "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (self.seriesID,self.year,self.period,self.value,self.surveyAbbr,self.seasonal,self.group_code,self.group_name,self.item_code, self.item_name, self.percentageChange, self.timePeriod,self.yearOverYear)

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
    data.to_csv(tempName,index=False)

def formatTimePeriod(year,monthPeriod):
    # This should be formatted yyyy-mm-01
    return year + "-" + monthPeriod[1:] + "-01"

def specialRounding(currentNum, previousNum):
    return round((currentNum-previousNum),1)

def yearOverYearCalculation(dataFrame,dropM13):
    newDF = []
    labelDict = []
    dfList = dataFrame.values.tolist()
    for j in range(0,len(dfList)):
        newRowQrt = dataRow(dfList[j][0],dfList[j][1],dfList[j][2],dfList[j][3],"","","","","","","","","")
        newDF.append(newRowQrt)
    print(len(newDF))
    for i in range(0,len(newDF)):
        if newDF[i].seriesID not in labelDict:
            labelDict.append(newDF[i].seriesID)
            newDF[i].yearOverYear = "-"
            if dropM13 == 1:
                for k in range(1,12):
                    print(str(newDF[int(i+k)]))
                    newDF[int(i+k)].yearOverYear = "-"
            else:
                for k in range(1,13):
                    print(str(newDF[int(i+k)]))
                    newDF[int(i+k)].yearOverYear = "-"
    for l in newDF:
        print(str(l))

def quarteriseDataFrame(dataFrame):
    newDF = []
    dfList = dataFrame.values.tolist()
    iterList = iter(dfList)
    quarterDict = {}
    for j in iterList:
        newRowQrt = dataRow(j[0],j[1],j[2],j[3],"","","","","","","","","")
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
            newDF.append([x,k,"Q1",arrayAvg(quarterDict[x][k].q1)])
            newDF.append([x,k,"Q2",arrayAvg(quarterDict[x][k].q2)])
            newDF.append([x,k,"Q3",arrayAvg(quarterDict[x][k].q3)])
            newDF.append([x,k,"Q4",arrayAvg(quarterDict[x][k].q4)])
    newDataFrame = pd.DataFrame(newDF, columns=["series_id","year","quarter","value"])
    newDataFrame = newDataFrame[newDataFrame["value"]!="X"]
    return newDataFrame

def arrayAvg(arr):
    if len(arr) == 0:
        return "X"
    return round(sum(arr)/len(arr),1)

def periodOverPeriodCalculation(dataFrame):
    dfList = dataFrame.values.tolist()
    percentageColumn = []
    labelDict = []
    for row in range(0,len(dfList)):
        if dfList[row][0] not in labelDict:
            labelDict.append(dfList[row][0])
            percentageColumn.append("X")
        elif dfList[row][3] == "X" or dfList[row-1][3] == "X":
            percentageColumn.append("X")
        else:
            percentageColumn.append(specialRounding(float(dfList[row][3]),float(dfList[row-1][3])))
    dataFrame.insert((dataFrame.columns.get_loc("value")+1),"percent_change",percentageColumn,True)
    return dataFrame

# Have a look over this. 
def yearifyDataFrame(dataFrame):
    newDF = []
    dfList = dataFrame.values.tolist()
    iterList = iter(dfList)
    yearDict = {}
    for j in iterList:
        newRowQrt = dataRow(j[0],j[1],j[2],j[3],"","","","","","","","","")
        if newRowQrt.seriesID not in yearDict:
            yearDict[newRowQrt.seriesID] = {}
        if newRowQrt.year not in yearDict[newRowQrt.seriesID]:
            yearDict[newRowQrt.seriesID][newRowQrt.year] = []
        yearDict[newRowQrt.seriesID][newRowQrt.year].append(float(newRowQrt.value))
    for x in yearDict:
        for k in yearDict[x]:
            newDF.append([x,k,"",arrayAvg(yearDict[x][k])])
    return pd.DataFrame(newDF, columns=["series_id","year","year","yearly average"])

def createCustomFormattedDataFrame(dataFrame):
    print("For each of these options type 0 for yes or 1 for no:")
    avgOverQrt = int(input("Would you like the values averaged over quarters?: "))
    if avgOverQrt == 0:
        avgOverYear = int(input("Would you like the values averaged over the years?: "))
        if avgOverYear == 1:
            dataFrame = yearifyDataFrame(dataFrame)
        else:
            timeFormat = int(input("Would you like the dates converted to yyyy-mm-01 format?: "))
            m13Drop = int(input("Would you like to drop all M13 periods?: "))
    if avgOverQrt == 1:
        dataFrame = quarteriseDataFrame(dataFrame)
    percentageChg = int(input("Would you like to add the percentage change between periods?: "))
    labelAdd = int(input("Would you like to add labels for each level?: "))
    codeSplit = int(input("Would you like to split all the id codes?: "))
    seasonColumn = int(input("Would you like to add a column for seasonal codes?: "))
    seasonal = []
    groupCode = []
    itemCode = []
    if percentageChg == 1:
        dataFrame = periodOverPeriodCalculation(dataFrame)
    if labelAdd == 1:
        BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpGrp","groupLabels")
        BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpLRef","labels")
        newPath = path + '\\RawData\\' + BLS_Request.getLatestVersionFileName("wpLRef",BLS_Request.getAllFilesInDirectory("wpLRef"))
        newGroupPath = path + '\\RawData\\' + BLS_Request.getLatestVersionFileName("wpGrp",BLS_Request.getAllFilesInDirectory("wpGrp"))
        newGroupFrame = changeRowHeaders(readParquet(newGroupPath)).drop([0])
        newDataFrame = changeRowHeaders(readParquet(newPath)).drop([0])
        mergeLeft = pd.merge(left=newGroupFrame,right=newDataFrame,how='left',left_on='group_code',right_on='group_code')
        for row in dataFrame.index:
            columnRow = dataFrame["series_id"][row]
            seasonal.append(columnRow[2:3])
            if columnRow[3:5] in newGroupFrame["group_code"].tolist():
                groupCode.append(columnRow[3:5])
                itemCode.append(columnRow[5:])
            elif columnRow[3:6] in newGroupFrame["group_code"].tolist():
                groupCode.append(columnRow[3:6])
                itemCode.append(columnRow[6:])
        if seasonColumn == 1:
            dataFrame.insert((dataFrame.columns.get_loc("value")+percentageChg+1),"seasonal", seasonal, True)
        dataFrame.insert((dataFrame.columns.get_loc("value")+percentageChg+seasonColumn+1),"group_code",groupCode,True)
        dataFrame.insert((dataFrame.columns.get_loc("value")+percentageChg+seasonColumn+2),"item_code",itemCode,True)
        dataFrame = pd.merge(left=dataFrame,right=mergeLeft,how='left',left_on=['group_code','item_code'],right_on=['group_code','item_code'])
        listOfHeaders = list(dataFrame.columns)
        print(listOfHeaders)
        listOfHeaders[listOfHeaders.index("group_name")] = "item_code"
        listOfHeaders[listOfHeaders.index("item_code")] = "group_name"
        print(listOfHeaders)
        dataFrame = dataFrame.reindex(columns=listOfHeaders)
        print(dataFrame)
    elif codeSplit == 1:
        for row in dataFrame.index:
            columnRow = dataFrame["series_id"][row]
            seasonal.append(columnRow[2:3])
            if columnRow[3:5] in newGroupFrame["group_code"].tolist():
                groupCode.append(columnRow[3:5])
                itemCode.append(columnRow[5:])
            elif columnRow[3:6] in newGroupFrame["group_code"].tolist():
                groupCode.append(columnRow[3:6])
                itemCode.append(columnRow[6:])
        if seasonColumn == 1:
            dataFrame.insert(1,"seasonal", seasonal, True)
        dataFrame.insert(2,"group_code",groupCode,True)
        dataFrame.insert(3,"item_code",itemCode,True)

    if avgOverQrt == 0 and avgOverYear == 0:
        if m13Drop == 1:
            dataFrame = dataFrame[dataFrame.period != "M13"]
        if timeFormat == 1:
            formattedTime = []
            for row in dataFrame.index:
                formattedTime.append(formatTimePeriod(dataFrame["year"][row],dataFrame["period"][row]))
            dataFrame.insert(1,"formatted_time",formattedTime,True)
            dataFrame = dataFrame.drop(['year','period'],axis=1)
    return dataFrame

def formatString(stringToChange):
    while ",," in stringToChange:
        stringToChange = stringToChange.replace(",,",",")
    if stringToChange[len(stringToChange)-1] == ",":
        stringToChange = stringToChange[:-1]
    return stringToChange

def changeRowHeaders(dataFrame):
    dfList = dataFrame.values.tolist()
    for i in range(0,len(dfList[0])):
        dataFrame = dataFrame.rename(columns = {i:dfList[0][i]})
    return dataFrame

def formatPeriod(per):
    return per[1:]

def experimentalQuarterisation():
    qrtDF = pd.DataFrame(columns=['series_id','year','quarter_title','quarterly_avg'])
    temp = dataFrame.groupby(["series_id","year"])
    print(temp.size())
    quarterLabel = ["Q1","Q2","Q3","Q4"]
    t0 = time.time()
    for key, item in temp:
        quarter = [[],[],[],[]]
        dfList = item.values.tolist()
        print(dfList)
        for row in dfList:
            if row[2] in quartersArr[0]:
                quarter[0].append(float(row[3]))
            elif row[2] in quartersArr[1]:
                quarter[1].append(float(row[3]))
            elif row[2] in quartersArr[2]:
                quarter[2].append(float(row[3]))
            elif row[2] in quartersArr[3]:
                quarter[3].append(float(row[3]))
        for q in range(0,len(quarter)):
            qrtDF = qrtDF.append({'series_id':dfList[0][0], 'year':dfList[0][1], 'quarter_title':quarterLabel[q],'quarterly_avg':arrayAvg(quarter[q])},ignore_index=True) 
    t1 = time.time()
    print(qrtDF.size)
    print(t1-t0)

def wpProcessing():
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpCur","Current")
    newPath = path + '\\RawData\\' + BLS_Request.getLatestVersionFileName("wpCur",BLS_Request.getAllFilesInDirectory("wpCur"))
    writeToCSV(newPath,createCustomFormattedDataFrame(changeRowHeaders(readParquet(newPath)).drop([0])))

wpProcessing()
