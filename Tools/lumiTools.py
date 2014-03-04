import sys,os,re
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools')
import Tools.coreTools as coreTools
from FWCore.PythonUtilities.LumiList   import LumiList
def calcLumi(jsonFile,addLumiOpt='',debug=False):
    csvFile=jsonFile.strip()+"_lumiCSV.csv"
    cmd="lumiCalc2.py "+addLumiOpt+" overview -i "+jsonFile+" -o "+csvFile
    if debug:
      print cmd
    lC = coreTools.executeCommandSameEnv(cmd)
    coreTools.checkCommandAbortIfFail(lC)
    intLumi = 0
    for l in open(csvFile):
      match = re.match('^[0-9]+:.*,\ *([0-9]+\.[0-9])\ *.*$',l)
      if match:
        intLumi += float(match.group(1))
    print "intLumi ",intLumi
    return intLumi

