# tools for run cmssw analysis
import FWCore.ParameterSet.Config as cms
import sys,imp,subprocess,re,os
#########################
class sample():
  def __init__(self,name,postfix = "",maxEvents=-1):
    self.name = name
    self.postfix = postfix
    self.maxEvents = maxEvents
    if not isinstance(name,list):
     self.name = [name]
  def getInputfiles(self):
    inputFiles = cms.untracked.vstring([f for f in self.name if f.startswith('file:/') or  f.startswith('/store/')])
    if len(self.name) == len(inputFiles):
      return inputFiles
    else:
      sys.exit('inputFiles not okay')
  def xSec(self,xSec):
    self.xSec = float(xSec)
  def numEvts(self,numEvts):
    self.numEvts=float(numEvts)
  def intLumi(self):
    if self.numEvts and self.xSec:
      return  self.numEvts/self.xSec
########################
class processSample():
  def __init__(self,cfgFileName):
    self.cfgFileName = cfgFileName
    self.newCfgName = None
    self.tmpCfg = None
  def createTmpCfg(self):
    if self.tmpCfg != None:
      print "its already there"
      return 
    cfgFile = open(self.cfgFileName,'r') #copy.deepcopy(f);f.close()
    cfgFileLoaded = imp.load_source('cfgTMP',self.cfgFileName,cfgFile);cfgFile.close()
    reTmpCfgFileName =  re.match('(.*[^\.])(\.[^ \.]*)',self.cfgFileName)
    tmpCfg = open(reTmpCfgFileName.group(1)+'_TMP'+reTmpCfgFileName.group(2) , 'w')
    tmpCfg.write(cfgFileLoaded.process.dumpPython())
    tmpCfg.close()
    self.tmpCfg = tmpCfg.name
  def createNewCfg (self,samp,putNewCfgHere):
    self.createTmpCfg()
    import copy
    #f = open(self.cfgFileName,'r')
    print "laoding ",self.tmpCfg
    tmpCfgFile = open(self.tmpCfg,'r') #copy.deepcopy(f);f.close()
    tmpCfgFileLoaded = imp.load_source('cfg'+samp.postfix,self.tmpCfg,tmpCfgFile);tmpCfgFile.close()
#    cfgFileLoaded.process.MessageLogger
    #adapt input
    tmpCfgFileLoaded.process.source.fileNames = samp.getInputfiles();tmpCfgFileLoaded.process.maxEvents.input.setValue(samp.maxEvents)
    #adapt output
    for outItem in tmpCfgFileLoaded.process.outputModules.values():
        reOutName = re.match('(.*)(\.[^ \.]*)',outItem.fileName.value())
        outItem.fileName.setValue(reOutName.group(1)+ (("_"+samp.postfix) if samp.postfix != "" else "") + reOutName.group(2) )
    # TFileService
    if hasattr(tmpCfgFileLoaded.process,"TFileService"):
      reTFileServiceName = reOutName = re.match('(.*)(\.[^ \.]*)', tmpCfgFileLoaded.process.TFileService.fileName.value())
      tmpCfgFileLoaded.process.TFileService.fileName.setValue (reTFileServiceName.group(1)+ (("_"+samp.postfix) if samp.postfix != "" else "") +reTFileServiceName.group(2))
    # MessageLogger
    if hasattr(tmpCfgFileLoaded.process,"MessageLogger"):
      for dest in tmpCfgFileLoaded.process.MessageLogger.destinations:
        if hasattr(tmpCfgFileLoaded.process.MessageLogger,dest):
          if hasattr(getattr(tmpCfgFileLoaded.process.MessageLogger,dest),'filename'): 
            reMessageLoggerFilename = re.match('(.*)(\.[^ \.]*)',getattr(getattr(tmpCfgFileLoaded.process.MessageLogger,dest),'filename').value())
            getattr(getattr(tmpCfgFileLoaded.process.MessageLogger,dest),'filename').setValue(reMessageLoggerFilename.group(1)+(("_"+samp.postfix) if samp.postfix != "" else "") + reMessageLoggerFilename.group(2))     
    # create new cfg
    reCfgFileName =  re.match('(.*[^\.])(\.[^ \.]*)',self.cfgFileName)
    newCfgFileName=reCfgFileName.group(1) + (("_"+samp.postfix) if samp.postfix != "" else "") +"_createdBy_runOverSample"+reCfgFileName.group(2)
    newCfgFileName = re.match('.*[^\/]\/([^\/][^\/]*)',newCfgFileName).group(1) if putNewCfgHere else newCfgFileName
    newCfg = open(newCfgFileName , 'w')
    newCfg.write(tmpCfgFileLoaded.process.dumpPython())
    print newCfg.name
    newCfg.close()
    del tmpCfgFileLoaded
    del tmpCfgFile
    self.newCfgName = newCfg.name
  def runSample(self,samp,putNewCfgHere):
    self.createNewCfg (samp,putNewCfgHere)
    command="cmsRun "+ self.newCfgName +' >& '+self.newCfgName+"_output.log"
    print command
    subPrOutput = subprocess.Popen([command],shell=True,stdout=subprocess.PIPE,env=os.environ)
    subPrOutput.wait()
    errorcode = subPrOutput.returncode
    print "ERRORCODE ",errorcode 
  def end(self):
    print "deleting tmp cfg ",self.tmpCfg
    os.remove(self.tmpCfg) 
