import sys,os,re
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools')
import Tools.coreTools as coreTools
from FWCore.PythonUtilities.LumiList   import LumiList
def calcLumi(jsonFile,addLumiOpt='',debug=False):
    csvFile=jsonFile.strip()+"_lumiCSV_tmp.csv"
    cmd="pixelLumiCalc.py "+addLumiOpt+" overview -i "+jsonFile+" -o "+csvFile
    if debug:
      print cmd
    stdout,exitC = coreTools.executeCommandSameEnvBkpReturnCode(cmd)
    if debug:
      if exitC != 0:
        print "command failed: ",cmd
      print "".join(stdout)
      print "exitC ",exitC
    if debug:
      print stdout
    intLumi = 0
    for l in open(csvFile):
      match = re.match('^[0-9]+:.*,\ *([0-9]+\.[0-9])\ *.*$',l)
      if match:
        intLumi += float(match.group(1))
    os.remove(csvFile)
    if debug:
      print "intLumi ",intLumi
    return intLumi

