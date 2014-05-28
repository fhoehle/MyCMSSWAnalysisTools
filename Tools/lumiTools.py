import sys,os,re
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools')
import Tools.coreTools as coreTools
from FWCore.PythonUtilities.LumiList   import LumiList
def getIntLumiByCmd(jsonFile,addLumiOpt,debug,tool="pixelLumiCalc.py"):
    csvFile=jsonFile.strip()+"_lumiCSV_tmp.csv"
    cmd=tool+" "+addLumiOpt+" overview -i "+jsonFile+" -o "+csvFile
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
    return csvFile
def calcLumi(jsonFile,addLumiOpt='',debug=False):
    tmpFileName=os.getenv('PWD')+'tmpCalcLumifile.json'
    if isinstance(jsonFile,LumiList):
      jsonFile.writeJSON(tmpFileName)
      jsonFile=tmpFileName
    csvFile = getIntLumiByCmd(jsonFile=jsonFile,addLumiOpt=addLumiOpt,debug=debug)
    if not os.path.isfile(csvFile):
      csvFile = getIntLumiByCmd(tool="lumiCalc2.py",jsonFile=jsonFile,addLumiOpt=addLumiOpt,debug=debug)
    intLumi = 0
    for l in open(csvFile):
      match = re.match('^[0-9]+:.*,\ *([0-9]+\.[0-9])\ *.*$',l)
      if match:
        intLumi += float(match.group(1))
    os.remove(csvFile)
    if tmpFileName == jsonFile:
       os.remove(tmpFileName)
    if debug:
      print "intLumi ",intLumi
    return intLumi

