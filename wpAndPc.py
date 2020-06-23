import BLS_Request
import os
import pyarrow.parquet as pq
import csv
import pandas as pd
import pyarrow as pa
path = str(os.path.dirname(os.path.realpath(__file__)))

class dataRow:
    def __init__(self,seriesID, year, period, surveyAbbr, industry_code, product_code, seasonal, timePeriod):
        self.seriesID = seriesID
        self.year = year
        self.period = period
        self.surveyAbbr = surveyAbbr
        self.industry_code = industry_code
        self.product_code = product_code
        self.seasonal = seasonal
        self.timePeriod = timePeriod
"Time Period (YYYY-MM-01)"


def checkForLatestVersion():
    print("Choose an option:")
    print("1: pc        (Industry)")
    print("2: wp        (Commodity)")
    wpORpc = str(input("Type either pc or wp: "))
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion(wpORpc)

# This should probably be moved to BLS_Request 
def getIndustryAndProductReferenceData(wpORpc):
    commodityURL = "https://download.bls.gov/pub/time.series/wp/wp.item"
    industryURL = "https://download.bls.gov/pub/time.series/pc/pc.product"
    rawData = []
    tempName = ''
    if wpORpc == "pc":
        rawData = BLS_Request.getBLSData(industryURL,wpORpc)
        tempName = path + '\\RawData\\Industry\\wp_item.parquet'
    else:
        rawData =  BLS_Request.getBLSData(commodityURL,wpORpc)
        tempName = path + '\\RawData\\Commodity\\pc_product.parquet'
    df = pd.DataFrame(rawData)
    table = pa.Table.from_pandas(df)
    pq.write_table(table,tempName)

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

def dropM13():
    print("drop M13")

def Addlabels(dataFrame):
    dataFrame = dataFrame[0].tolist()
    labelDictionary = {}
    for labels in dataFrame:
        if labels not in labelDictionary:
            labelArr = []
            labelArr.append(labels[:2])
            labelArr.append(labels[2:3])
            labelArr.append(labels[3:9])
            labelArr.append(labels[10:])
            labelDictionary[labels] = labelArr
    return labelDictionary

def createCustomFormattedDataFrame(dataFrame):
    labelDict = Addlabels(dataFrame)
    columnTitlesSet = False
    newDataFrame = []
    print("For each of these options type 0 for yes or 1 for no:")
    timeFormat = int(input("Would you like the dates converted to yyyy-mm-01 format?: "))
    #m13Drop = int(input("Would you like to drop all M13 periods?: "))
    #labelAdd = int(input("Would you like to add labels for each level?: "))
    codeSplit = int(input("Would you like to split all the id codes?: "))
    seasonColumn = int(input("Would you like to add a column for seasonal codes?: "))
    dfList = dataFrame.values.tolist()
    #Figure out how to get from pandas data frame to 2d list
    #______________Iterating through the list_____________________
    for i in dfList:
        newRow = []
        newRow.append(i[0])
    #______________Splitting the ID code__________________________
        if codeSplit == 0:
            for k in range(0,len(labelDict[i[0]])):
                if k != 1:
                    newRow.append(labelDict[i[0]][k])
            if columnTitlesSet == False:
                newRow[newRow.index("se")] = "survey abbr."
                newRow[newRow.index("ies_id")] = "industry_code"
                newRow[newRow.index("industry_code")+1] = "product_code"
    #______________________Season Column__________________________
        if seasonColumn == 0:
            if len(newRow) > 1:
                newRow.insert(1,labelDict[i[0]][1])
            if columnTitlesSet == False:
                    newRow[newRow.index("r")] = "seasonal"
    #_____________________Time Formatting_________________________
        if timeFormat == 0:
            newRow.append(formatTimePeriod(i[1],i[2]))
            if columnTitlesSet == False:
                    newRow[newRow.index("year-eriod-01")] = "Time Period (YYYY-MM-01)"
        else:
            newRow.append(i[1])
            newRow.append(i[2]) 
    #______________________Season Column__________________________
        #if labelAdd == 0:
            #print("LABEL")
        newDataFrame.append(newRow)
        if columnTitlesSet == False:
            columnTitlesSet = True
    return newDataFrame
             
#checkForLatestVersion()
print("Choose an option:")
print("1: pc        (Industry)")
print("2: wp        (Commodity)")
wpORpc = str(input("Type either pc or wp: "))
newPath = path + '\\RawData\\' + BLS_Request.getLatestVersionFileName(wpORpc,BLS_Request.getAllFilesInDirectory(wpORpc))
#print(BLS_Request.checkForIndustryOrCommodity(wpORpc, newPath))
print("NEW PATH: " + str(newPath))
dataFrame = readParquet(newPath)
data = createCustomFormattedDataFrame(dataFrame)
writeToCSV(newPath,data)