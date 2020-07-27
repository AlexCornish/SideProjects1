import BLS_Request
import os
import pyarrow.parquet as pq
import pandas as pd
import csv
import numpy as np
#path: Dynamic path which is the current directory where the wp.py program is located.
path = str(os.path.dirname(os.path.realpath(__file__)))
#QuartersArr: Contains the quarters used in the quartising function. 
quartersArr = ["M01M02M03","M04M05M06","M07M08M09","M10M11M12"]

#Quarter class: Used in the calculation of the quarterly values.
class quarters:
    def __init__(self, q1, q2, q3, q4):
        self.q1 = q1
        self.q2 = q2
        self.q3 = q3
        self.q4 = q4

#Gets the latest version of the wp.data.0.Current using the BLS_Request library located in BLS_Request.py
def checkForLatestVersion():
    # Compares the latest version online with the latest version downloaded, if the online version is newer, the online one is downloaded.
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpCur","Current")

#Reads a parquet file and turns it into pyarrow table, then .to_pandas() converts the table to a dataframe.
def readParquet(fileName):
    # - fileName: (String) The name of the parquet file to be read and converted from parquet to pandas.
    return pq.read_table(fileName).to_pandas()

# Writes the pandas dataframe to a .csv file.
def writeToCSV(fileName,data):
    # - fileName: (String) The name of the original parquet file from which the name for the .csv file will be made.
    # - data: (Dataframe) The formatted data in a pandas dataframe.
    # removes the .parquet extension from the filename and adds the .csv extension.
    tempName = fileName[:-8] + ".csv"
    data = data.round(1)
    # converts the dataframe to a .csv file and removes the indexes from the dataframe.
    data.to_csv(tempName,index=False)
    print("Formatted dataframe written to .CSV")

# Formats the date from a separate year / month (period) to a string that is in the yyyy-mm-01 format.
def formatTimePeriod(year,monthPeriod):
    # monthPeriod[1:]: (string) Removes the M from the MXX period string to leave the numbers.
    return year + "-" + monthPeriod + "-01"

# Rounds the resulting difference from currentNum - previousNum
def specialRounding(currentNum, previousNum):
    # - currentNum: (Float) The latest number of the 2
    # - previousNum: (Float) The other number
    return round((currentNum-previousNum),1)

# Performs the year over year calculations to determine the changes between the same months of consecutive years (Ex. March 2019 to March 2020)
def yearOverYearCalculation(dataFrame,dropM13):
    # - dataFrame: (Dataframe) The dataframe containing all the information
    # - dropM13: (Integer) Indicates whether or not the rows containing the M13 have been dropped. 
    if dropM13 == 1:
        dataFrame['value'] = dataFrame['value'].astype(float)
        dataFrame['year_over_year'] = dataFrame.groupby("series_id")['value'].diff(12)
        return dataFrame 
    else:
        # Initialises the yearOverYear array
        yearOverYear = []
        # Sorts the dataframe by period then year
        dataFrame = dataFrame.sort_values(by=["period","year"])
        # Populates the blank yearOverYear array with empty strings.
        for i in range(0,len(dataFrame)):
            yearOverYear.append("")
        # Attaches the new yearOverYear column to the current dataFrame.
        dataFrame.insert(3,"yearOverYear",yearOverYear,True)
        # Groups the content of the dataframe by the series_id
        grouped = dataFrame.groupby("series_id")
        # Initialises the new dataframe
        newDF = []
        # Iterates through the grouped dataframe.
        for group in grouped:
            tempGroup = group[1]
            # For each grouped dataframe...
            # Converts the dataframe to a 2d array.
            tempGroup = tempGroup.values.tolist()
            tempGroup[0][5] = "X"
            # Iterates through the individual group.
            for i in range(1,len(tempGroup)):
                # Checks if the year is greater than the year in the row above.
                if int(tempGroup[i][1]) > int(tempGroup[i-1][1]):
                    # Rounds the difference between the year and the previous year with the same month.
                    tempGroup[i][5] = specialRounding(float(tempGroup[i][4]),float(tempGroup[i-1][4]))
                else:
                    tempGroup[i][5] = "X"
            # Iterates through the modified tempGroup
            for i in tempGroup:
                # Adds the modified tempGroup row to the newDF
                newDF.append(i)
        # Creates a new dataframe from the newDF 2d array.
        newFrame = pd.DataFrame(newDF,columns=["series_id","year","period","footnote_code","value","yearOverYear"])
        # Sorts the new dataframe.
        newFrame = newFrame.sort_values(by=["series_id","year"])
        return newFrame

# QuarteriseDataFrame: Converts the dataframe from monthly periods to quarters. 
def quarteriseDataFrame(dataFrame):
    # Converts the dataframe to a 2D array.
    dfList = dataFrame.values.tolist()
    # Initialises the newDF array.
    newDF = []
    # Initialises the quarterDict
    quarterDict = {}
    # Iterates through the dfList
    for row in range(0,len(dfList)):
        # Checks if the series_id is in the quarterdict
        if dfList[row][0] not in quarterDict:
            # Creates a nested dictionary with the key as the series_id
            quarterDict[dfList[row][0]] = {}
        # Checks if the series_id is in the quarterDict and if it is, adds an entry in the nested dictionary with the keys being the series_id and the year.
        if dfList[row][1] not in quarterDict[dfList[row][0]]:
            quarterDict[dfList[row][0]][dfList[row][1]] = quarters([],[],[],[])
        # Iterates through the quartersArr
        for m in range(0,len(quartersArr)):
            # Checks if the current row's period is in current quarter in the quartersArr.
            if dfList[row][2] in quartersArr[m]:
                if m == 0:
                    quarterDict[dfList[row][0]][dfList[row][1]].q1.append(float(dfList[row][3]))
                elif m == 1:
                    quarterDict[dfList[row][0]][dfList[row][1]].q2.append(float(dfList[row][3]))
                elif m == 2:
                    quarterDict[dfList[row][0]][dfList[row][1]].q3.append(float(dfList[row][3]))
                elif m == 3:
                    quarterDict[dfList[row][0]][dfList[row][1]].q4.append(float(dfList[row][3]))
    # Iterates through the outer dictionary.
    for x in quarterDict:
        # Iterates through the nested dictionar 
        for k in quarterDict[x]:
            # Adds each of the quarter arrays to the newDF.
            newDF.append([x,k,"Q1",arrayAvg(quarterDict[x][k].q1)])
            newDF.append([x,k,"Q2",arrayAvg(quarterDict[x][k].q2)])
            newDF.append([x,k,"Q3",arrayAvg(quarterDict[x][k].q3)])
            newDF.append([x,k,"Q4",arrayAvg(quarterDict[x][k].q4)])
    # Creates the new dataframe from the 2d array.
    newDataFrame = pd.DataFrame(newDF, columns=["series_id","year","quarter","value"])
    # Removes the rows from the dataframe where the value == X
    newDataFrame = newDataFrame[newDataFrame["value"]!="X"]
    return newDataFrame

# Gets the average of the values in the array.
def arrayAvg(arr):
    # An array of numbers
    if len(arr) == 0:
        return "X"
    return round(sum(arr)/len(arr),1)

# periodOverPeriodCalculation: Calculates the difference between consecutive time periods
def periodOverPeriodCalculation(dataFrame):
    dataFrame['value'] = dataFrame['value'].astype(float)
    dataFrame['percent_change'] = np.nan
    dataFrame = dataFrame.groupby("series_id").apply(periodCalc)
    return dataFrame

# periodCalc: Calculates the percent difference between every row in the grouped dataframe
def periodCalc(dataFrame):
    dataFrame['percent_change'] = (100*(dataFrame["value"].div(dataFrame["value"].shift(periods=1))-1))
    return dataFrame

# Makes the dataframe from monthly (period based) into year based ones.
def yearifyDataFrame(dataFrame):
    # Initialises the blank newDF
    newDF = []
    # converts the dataframe to a 2d Array
    dfList = dataFrame.values.tolist()
    # Initialises the yearDict
    yearDict = {}
    # Iterates through dfList
    for row in range(0,len(dfList)):
        # Checks if the series_id is in the labeldict
        if dfList[row][0] not in yearDict:
            # Initialises an empty dictionary with the key being the series_id
            yearDict[dfList[row][0]] = {}
        # Checks if the year is in the dictionary that has the key series_id
        if dfList[row][1] not in yearDict[dfList[row][0]]:
            # Creates a blank array in the nested dictionary.
            yearDict[dfList[row][0]][dfList[row][1]] = []
        # Appends the value to the array in the nested dictionary.
        yearDict[dfList[row][0]][dfList[row][1]].append(float(dfList[row][3]))
    # Iterates through the outer dictionary
    for x in yearDict:
        # Iterates through the inner dictionary.
        for k in yearDict[x]:
            # Adds the new row which has the series_id, year, and averaged year value.
            newDF.append([x,k,arrayAvg(yearDict[x][k])])
    return pd.DataFrame(newDF, columns=["series_id","year","value"])

# Takes the original dataframe information and converts in into a custom formatted dataframe.
def createCustomFormattedDataFrame(dataFrame):
    # Initialises the values to avoid problems with use before initialisation. 
    avgOverYear = 0
    timeFormat = 0
    yearOverYearBool = 0
    # ________________ Control flow _____________________ (This needs to be improved.)
    print("For each of these options type 1 for yes or 0 for no:")
    avgOverQrt = int(input("Would you like the values averaged over quarters?: "))
    while avgOverQrt != 0 and avgOverQrt != 1:   
        avgOverQrt = int(input("Would you like the values averaged over quarters?: ")) 
    if avgOverQrt == 0:
        avgOverYear = int(input("Would you like the values averaged over the years?: "))
        while avgOverYear != 0 and avgOverYear != 1:    
            avgOverYear = int(input("Would you like the values averaged over the years?: "))
        if avgOverYear == 1:
            # dataframe gets replaced with a dataframe in the "yearified" format.
            dataFrame = yearifyDataFrame(dataFrame)
        else:
            m13Drop = int(input("Would you like to drop all M13 periods?: "))
            while m13Drop != 0 and m13Drop != 1:
                m13Drop = int(input("Would you like to drop all M13 periods?: "))

            yearOverYearBool = int(input("Would you like the year-over-year percentage changes calculated?"))
            while yearOverYearBool != 0 and yearOverYearBool != 1:
                yearOverYearBool = int(input("Would you like the year-over-year percentage changes calculated?"))

            if yearOverYearBool == 1:
                # Returns the dataframe with the year over year calculations added.
                dataFrame = yearOverYearCalculation(dataFrame,m13Drop)

            timeFormat = int(input("Would you like the dates converted to yyyy-mm-01 format?: "))
            while timeFormat != 0 and timeFormat != 1:
                timeFormat = int(input("Would you like the dates converted to yyyy-mm-01 format?: "))

    if avgOverQrt == 1:
        # Converts the dataframe from periods into quarters
        dataFrame = quarteriseDataFrame(dataFrame)
    #______Control flow with error checking____________
    percentageChg = int(input("Would you like to add the percentage change between periods?: "))
    while percentageChg != 0 and percentageChg != 1:
        percentageChg = int(input("Would you like to add the percentage change between periods?: "))

    labelAdd =int(input("Would you like to add labels for each level?: "))
    while labelAdd != 0 and labelAdd != 1:
        labelAdd =int(input("Would you like to add labels for each level?: "))

    codeSplit = int(input("Would you like to split all the id codes?: "))
    while codeSplit != 0 and codeSplit != 1:
        codeSplit = int(input("Would you like to split all the id codes?: "))

    seasonColumn = int(input("Would you like to add a column for seasonal codes?: "))
    while seasonColumn != 0 and seasonColumn != 1:
        seasonColumn = int(input("Would you like to add a column for seasonal codes?: "))

    if percentageChg == 1:
        # Returns the dataframe with period over period calculations performed.
        dataFrame = periodOverPeriodCalculation(dataFrame)
    if labelAdd == 1:
        dataFrame = labelToAdd(dataFrame,seasonColumn,percentageChg)
    elif codeSplit == 1:
        dataFrame = labelToAdd(dataFrame,seasonColumn,percentageChg)
    # Checks if the quartised or yearified functions haven't been used and the dataframe is in standard period format.
    if avgOverQrt == 0 and avgOverYear == 0:
        # Check if m13 is to be dropped
        if m13Drop == 1:
            # Drops all entries that have a period equal to M13 from the dataframe
            dataFrame = dropM13(dataFrame)
        # Checks if the user wants the time formatted in yyyy-mm-01 format.
        if timeFormat == 1:
            dataFrame = formatTimeFunc(dataFrame)
    # Asks the user if they want the dataframe converted into narrow format.
    dataFrameMelting = int(input("Would you like the data in Narrow Format(1 for yes, 0 for no)?: "))
    while dataFrameMelting != 0 and dataFrameMelting != 1:
        dataFrameMelting = int(input("Would you like the data in Narrow Format(1 for yes, 0 for no)?: "))
    if dataFrameMelting == 1:
        # Returns the melted dataframe.
        return wideFormat(dataFrame,avgOverQrt,avgOverYear,timeFormat,percentageChg,yearOverYearBool)
    else:
        return dataFrame

def dropM13(dataFrame):
    return dataFrame[dataFrame.period != "M13"]

def formatTimeFunc(dataFrame):
    # Iterates through the dataframe by index.
    dataFrame['period'] = dataFrame["period"].str.replace('M',"")
    dataFrame.year = formatTimePeriod(dataFrame.year,dataFrame.period)
    # Renames the year column to formatted_time
    dataFrame = dataFrame.rename(columns={"year": "formatted_time"})
    return dataFrame.drop(['period'],axis=1)    

def labelToAdd(dataFrame,seasonColumn,percentageChg):
    # Gets the group labels using the BLS_Request library.
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpGrp","groupLabels")
    # Gets the item labels using the BLS_Request library.
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpLRef","labels")
    # Creates the paths for the for the item labels and the group labels
    newPath = os.path.join(path,'RawData',BLS_Request.getLatestVersionFileName("wpLRef",BLS_Request.getAllFilesInDirectory("wpLRef")))
    newGroupPath = os.path.join(path,'RawData',BLS_Request.getLatestVersionFileName("wpGrp",BLS_Request.getAllFilesInDirectory("wpGrp")))
    # Modifies the row headers for the two data frames.
    newGroupFrame = changeRowHeaders(readParquet(newGroupPath)).drop([0])
    newDataFrame = changeRowHeaders(readParquet(newPath)).drop([0])
    # Merges the two dataframes using a left join.
    mergeLeft = pd.merge(left=newGroupFrame,right=newDataFrame,how='left',left_on='group_code',right_on='group_code')
    mergeLeft["combinedCodes"] = mergeLeft["group_code"] + mergeLeft["item_code"]
    dataFrame["combinedCodes"] = dataFrame["series_id"].str[3:]
    # Performs a left join on the dataframe and the mergeLeft dataframe to add the labels. 
    dataFrame = pd.merge(left=dataFrame,right=mergeLeft,how='left',left_on="combinedCodes",right_on="combinedCodes")
    return dataFrame.drop(['combinedCodes'],axis=1)    
  
# Converts the standard dataframe into the wide format.
def wideFormat(dataframe,avgQrt,avgYear,timeForm,percentageChg,yearToDrop):
    # Initialises the columnTitle
    columnTitle = []
    # Iterates through the dataframe columns. 
    for col in dataframe.columns:
        # Appends the col to the columnTitle array
        columnTitle.append(col)
    # Checks if the user has had the data formatted by quarters.
    if avgQrt == 1:
        # Columns to drop from the original dataframe
        toDropFromDataframe = ["year","quarter","value"]
        # Values that will be included in the wide formatting
        valuesForDF = ["value"]
        # Checks if percentage change is selected
        if percentageChg == 1:
            # Adds the percent_change column to the dropped column list and the value list
            toDropFromDataframe.append("percent_change")
            valuesForDF.append("percent_change")
        # Pivots the dataframe based on the values list.
        df = dataframe.pivot_table(index="series_id",columns=["year","quarter"],values=valuesForDF,aggfunc='first')
        # Drops the columns that are in the toDrop list
        dataframe = dataframe.drop(columns=toDropFromDataframe)
        # Eliminates the duplicate rows from the dataframe.
        dataframe = dataframe.drop_duplicates()
        # Merges the pivoted dataframe and the original one.
        result = pd.merge(left=dataframe,right=df,how='inner',right_index=True,left_on='series_id')
        return result
    # Checks if the user has had the data formatted by years.
    elif avgYear == 1:
        # Columns to drop from the original dataframe
        toDropFromDataframe = ["year","value"]
        # Values that will be included in the wide formatting
        valuesForDF = ["value"]
        # Checks if percentage change is selected
        if percentageChg == 1:
            # Adds the percent_change column to the dropped column list and the value list
            toDropFromDataframe.append("percent_change")
            valuesForDF.append("percent_change")
        # Pivots the dataframe based on the values list.
        df = dataframe.pivot(index="series_id",columns="year",values=valuesForDF)
        # Drops the columns that are in the toDrop list
        dataframe = dataframe.drop(columns=toDropFromDataframe)
        # Eliminates the duplicate rows from the dataframe.
        dataframe = dataframe.drop_duplicates()
        result = pd.merge(left=dataframe,right=df,how='inner',right_index=True,left_on='series_id')
        # Merges the pivoted dataframe and the original one.
        return result
    # Checks if the user has had the time formatted..
    elif timeForm == 1:
        # Columns to drop from the original dataframe
        toDropFromDataframe = ["formatted_time","value"]
        # Values that will be included in the wide formatting
        valuesForDF = ["value"]
        # Checks if percentage change is selected
        if 'footnote_code' in dataframe:
            toDropFromDataframe.append("footnote_code")
            valuesForDF.append("footnote_code")
        if percentageChg == 1:
            # Adds the percent_change column to the dropped column list and the value list
            toDropFromDataframe.append("percent_change")
            valuesForDF.append("percent_change")
        if yearToDrop == 1:
            # Adds the year over year column to the dropped column list and the value list
            toDropFromDataframe.append("yearOverYear")
            valuesForDF.append("yearOverYear")
        # Pivots the dataframe based on the values list.
        df = dataframe.pivot(index="series_id",columns="formatted_time",values=valuesForDF)
        # Drops the columns that are in the toDrop list
        dataframe = dataframe.drop(columns=toDropFromDataframe)
        # Eliminates the duplicate rows from the dataframe.
        dataframe = dataframe.drop_duplicates()
        # Merges the pivoted dataframe and the original one.
        result = pd.merge(left=dataframe,right=df,how='inner',right_index=True,left_on='series_id')
        return result
    else:
        # Columns to drop from the original dataframe
        toDropFromDataframe = ["year","period","value"]
        # Values that will be included in the wide formatting
        valuesForDF = ["value"]
        if 'footnote_code' in dataframe:
            toDropFromDataframe.append("footnote_code")
            valuesForDF.append("footnote_code")
        # Checks if percentage change is selected
        if percentageChg == 1:
            # Adds the percent_change column to the dropped column list and the value list
            toDropFromDataframe.append("percent_change")
            valuesForDF.append("percent_change")
        if yearToDrop == 1:
            # Adds the year over year column to the dropped column list and the value list
            toDropFromDataframe.append("yearOverYear")
            valuesForDF.append("yearOverYear")
        # Pivots the dataframe based on the values list.
        df = dataframe.pivot_table(index="series_id",columns=["year","period"],values=valuesForDF,aggfunc='first')
        # Drops the columns that are in the toDrop list
        dataframe = dataframe.drop(columns=toDropFromDataframe)
        # Eliminates the duplicate rows from the dataframe.
        dataframe = dataframe.drop_duplicates()
        # Merges the pivoted dataframe and the original one.
        result = pd.merge(left=dataframe,right=df,how='inner',right_index=True,left_on='series_id')
        return result

# Modifies the row headers. Takes the column titles from the first row in the dataframe and renames the column index titles with the content. 
def changeRowHeaders(dataFrame):
    dfList = dataFrame.values.tolist()
    for i in range(0,len(dfList[0])):
        dataFrame = dataFrame.rename(columns = {i:dfList[0][i]})
    return dataFrame

# A function that encapsulates all the code that is needed to be run to produce formatted data.
def wpProcessing():
    BLS_Request.compareLatestOnlineVersionWithLatestDownloadedVersion("wpCur","Current")
    newPath = os.path.join(path,'RawData',BLS_Request.getLatestVersionFileName("wpCur",BLS_Request.getAllFilesInDirectory("wpCur")))
    writeToCSV(newPath,createCustomFormattedDataFrame(changeRowHeaders(readParquet(newPath)).drop([0])))
