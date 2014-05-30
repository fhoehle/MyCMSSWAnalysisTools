import sys,os,re
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools')
import Tools.coreTools as coreTools
from FWCore.PythonUtilities.LumiList   import LumiList
def getIntLumiByCmd(jsonFile,addLumiOpt,debug,tool="pixelLumiCalc.py"):
    csvFile=jsonFile.strip()+"_"+coreTools.idGenerator(size=3)+"_lumiCSV_tmp.csv"
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
    tmpFileName=os.getenv('PWD')+'tmpCalcLumifile_'+coreTools.idGenerator(size=3)+'.json'
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
#################

class lumiContentHLTTriggerByLS(object):
  def __init__(self,jsonFile,trigger,debug=False,minRun=-1,maxRun=-1,jsonOutput=None):
    self.jsonFile = jsonFile
    self.trigger = trigger
    self.debug = debug
    self.minRun = minRun
    self.maxRun = maxRun
    self.jsonOutput = jsonOutput
  def callLumiContent(self):
    hltByLScmd="hltbyls --name "+self.trigger
    self.csvOutput = os.path.basename(self.jsonFile)+"_lumiContentOutput_"+coreTools.idGenerator(size=3)
    lC = lumiContent(self.jsonFile,self.csvOutput,hltByLScmd,debug=self.debug)
  def analysisOutput(self):
    if not hasattr(self,'csvOutput'):
      self.callLumiContent()
    import csv
    reader = csv.reader(open(self.csvOutput),delimiter=',',quotechar='"')
    header = reader.next()
    if self.debug:
      print header
    hltInfoByLs = []
    for row in reader:
      hltInfo = row[2][1:-1].split(',')
      hltInfoByLs.append(row[:2]+[(hltInfo)])
    ##sorting and print prescaled lumis
    hltInfoByLs.sort(key=lambda hlt : hlt[0]+hlt[1])
    return hltInfoByLs
  def getGoodRuns(self):
    hltInfoByLs =self.analysisOutput()
    goodRunsAndLumis = LumiList(lumis = [[int(hltInf[0]),int(hltInf[1])] for hltInf in hltInfoByLs if int(hltInf[2][1]) == 1] )
    if self.jsonOutput:
      goodRunsAndLumis.writeJSON(jsonOutput+"_good")
      return jsonOutput+"_good"
    else:
      return goodRunsAndLumis
  def getStrangeRuns(self):
    hltInfoByLs =self.analysisOutput()
    strangeRunsLumis = LumiList( lumis = [[int(hltInf[0]),int(hltInf[1])] for hltInf in hltInfoByLs if not int(hltInf[2][1]) >= 1] )
    if self.jsonOutput:
      strangeRunsLumis.writeJSON(jsonOutput+"_strange")
    else:
      return strangeRunsLumis
  def getPrescaledRuns(self):
    hltInfoByLs =self.analysisOutput()
    prescaledRunsAndLumis = LumiList( lumis = [[int(hltInf[0]),int(hltInf[1])] for hltInf in hltInfoByLs if int(hltInf[2][1]) >= 1] )
    if self.jsonOutput:
      prescaledRunsAndLumis.writeJSON(jsonOutput+"_prescaled")
      return jsonOutput+"_prescaled"
    else:
      return prescaledRunsAndLumis
  def printAll(self):
    for hltInf in self.analysisOutput():
      if self.minRun > int(hltInf[0]):
        continue
      if self.maxRun > 0  and self.maxRun < int(hltInf[0]):
        break
      if hltInf[2][1] != '1':
        print hltInf
def lumiContent(jsonFile,csvOutput,arguments,debug):
  # calling lumiContent
  cmd = os.getenv('CMSSW_BASE')+os.path.sep+'src/RecoLuminosity/LumiDB/scripts/lumiContext.py '+arguments+' -i '+jsonFile+" -o "+csvOutput
  if debug:
     print cmd
  stdout,exitC = coreTools.executeCommandSameEnvBkpReturnCode(cmd,debug=debug)
  if debug:
    if exitC != 0:
      print "command failed: ",cmd
    print "".join(stdout)
    print "exitC ",exitC
  if debug:
    print stdout
  return csvOutput 
