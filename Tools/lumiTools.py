import sys,os,re
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools')
import Tools.coreTools as coreTools
def calcLumi(jsonFile):
    csvFile=jsonFile.strip()+"_lumiCSV.csv"
    lC = coreTools.executeCommandSameEnv("lumiCalc2.py overview -i "+jsonFile+" -o "+csvFile)
    coreTools.checkCommandAbortIfFail(lC)
    intLumi = 0
    for l in open(csvFile):
      match = re.match('^[0-9]+:.*,\ *([0-9]+\.[0-9])\ *.*$',l)
      if match:
        intLumi += float(match.group(1))
    print "intLumi ",intLumi
    return intLumi

