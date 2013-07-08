# tools for run cmssw analysis
import FWCore.ParameterSet.Config as cms
import sys,imp,subprocess,re,os
def addPostFixToFilename(name,postfix):
  reName = re.match('(.*[^\.])(\.[^ \.]*)',name)
  return reName.group(1) + (("_"+postfix) if postfix != "" else "") +reName.group(2)
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
      return 
    cfgFile = open(self.cfgFileName,'r') #copy.deepcopy(f);f.close()
    cfgFileLoaded = imp.load_source('cfgTMP',self.cfgFileName,cfgFile);cfgFile.close()
    tmpCfg = open(addPostFixToFilename(self.cfgFileName,'TMP') , 'w')
    tmpCfg.write(cfgFileLoaded.process.dumpPython())
    tmpCfg.close()
    self.tmpCfg = tmpCfg.name
  # create new config, i.e. apply sample specific changes: inputFiles output TFileService MessageLogger
  def createNewCfg (self,samp,putNewCfgHere,additionalOutputFolder=''):
    self.createTmpCfg()
    import copy
    tmpCfgFile = open(self.tmpCfg,'r') 
    tmpCfgFileLoaded = imp.load_source('cfg'+samp.postfix,self.tmpCfg,tmpCfgFile);tmpCfgFile.close()
    #adapt input
    tmpCfgFileLoaded.process.source.fileNames = samp.getInputfiles();tmpCfgFileLoaded.process.maxEvents.input.setValue(samp.maxEvents)
    additionalOutputFolder = 'file:'+additionalOutputFolder
    #adapt output
    for outItem in tmpCfgFileLoaded.process.outputModules.values():
        outItem.fileName.setValue(additionalOutputFolder+addPostFixToFilename(outItem.fileName.value(),samp.postfix))
    # TFileService
    if hasattr(tmpCfgFileLoaded.process,"TFileService"):
      tmpCfgFileLoaded.process.TFileService.fileName.setValue(additionalOutputFolder+addPostFixToFilename(tmpCfgFileLoaded.process.TFileService.fileName.value(),samp.postfix))
    # MessageLogger
    if hasattr(tmpCfgFileLoaded.process,"MessageLogger"):
      for dest in tmpCfgFileLoaded.process.MessageLogger.destinations:
        if hasattr(tmpCfgFileLoaded.process.MessageLogger,dest):
          if hasattr(getattr(tmpCfgFileLoaded.process.MessageLogger,dest),'filename'): 
            getattr(getattr(tmpCfgFileLoaded.process.MessageLogger,dest),'filename').setValue(additionalOutputFolder+addPostFixToFilename(getattr(getattr(tmpCfgFileLoaded.process.MessageLogger,dest),'filename').value(),samp.postfix))     
    # create new cfg
    newCfgFileName= addPostFixToFilename(self.cfgFileName , samp.postfix) 
    newCfgFileName = re.match('.*[^\/]\/([^\/][^\/]*)',newCfgFileName).group(1) if putNewCfgHere else newCfgFileName
    newCfg = open(newCfgFileName , 'w')
    newCfg.write(tmpCfgFileLoaded.process.dumpPython())
    print newCfg.name
    newCfg.close()
    self.newCfgName = newCfg.name
  # process sample
  def runSample(self,samp,putNewCfgHere,additionalOutputFolder=''):
    print additionalOutputFolder
    if not additionalOutputFolder == '' and not additionalOutputFolder == None:
      if additionalOutputFolder.endswith('/'):
         additionalOutputFolder +='/'  
      if os.path.exists(additionalOutputFolder):
        print "Warning folder exists"
      else :
        os.makedirs(additionalOutputFolder) 
    self.createNewCfg (samp,putNewCfgHere,additionalOutputFolder)
    command="cmsRun "+ self.newCfgName +' >& '+self.newCfgName+"_output.log"
    print command,"  outputFolder ",additionalOutputFolder
    subPrOutput = subprocess.Popen([command],shell=True,stdout=subprocess.PIPE,env=os.environ)
    subPrOutput.wait()
    errorcode = subPrOutput.returncode
    print "ERRORCODE ",errorcode 
  def end(self):
    print "deleting tmp cfg ",self.tmpCfg
    os.remove(self.tmpCfg) 
