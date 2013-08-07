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
    from os import getenv
    self.tmpLocation = os.getenv('PWD')
  def createTmpCfg(self):
    if self.tmpCfg != None:
      return 
    cfgFile = open(self.cfgFileName,'r') #copy.deepcopy(f);f.close()
    cfgFileLoaded = imp.load_source('cfgTMP',self.cfgFileName,cfgFile);cfgFile.close()
    from os import path
    tmpCfgName = path.split(addPostFixToFilename(self.cfgFileName,'TMP'))[1]
    tmpCfg = open(tmpCfgName , 'w')
    tmpCfg.write(cfgFileLoaded.process.dumpPython())
    tmpCfg.close()
    self.tmpCfg = tmpCfg.name
  def loadCfg(self,samp):
    self.createTmpCfg()
    import copy
    from os import path
    tmpCfgFile = open(self.tmpCfg,'r')
    self.tmpCfgFileLoaded = imp.load_source('cfg'+samp.postfix,self.tmpCfg,tmpCfgFile);tmpCfgFile.close()
  # create new config, i.e. apply sample specific changes: inputFiles output TFileService MessageLogger
  def applyChanges(self,samp,putNewCfgHere,additionalOutputFolder=''):
    from os import path
    self.loadCfg(samp)
    #adapt input
    self.tmpCfgFileLoaded.process.source.fileNames = samp.getInputfiles();self.tmpCfgFileLoaded.process.maxEvents.input.setValue(samp.maxEvents)
    additionalOutputFolderFWK = 'file:'+path.realpath(additionalOutputFolder)
    #adapt output
    for outItem in self.tmpCfgFileLoaded.process.outputModules.values():
        outItem.fileName.setValue(additionalOutputFolderFWK+path.sep+path.split(addPostFixToFilename(outItem.fileName.value(),samp.postfix))[1])
    # TFileService
    if hasattr(self.tmpCfgFileLoaded.process,"TFileService"):
      self.tmpCfgFileLoaded.process.TFileService.fileName.setValue(additionalOutputFolderFWK+path.sep+path.split(addPostFixToFilename(self.tmpCfgFileLoaded.process.TFileService.fileName.value(),samp.postfix))[1])
    # MessageLogger
    if hasattr(self.tmpCfgFileLoaded.process,"MessageLogger"):
      for dest in self.tmpCfgFileLoaded.process.MessageLogger.destinations:
        if hasattr(self.tmpCfgFileLoaded.process.MessageLogger,dest):
          if hasattr(getattr(self.tmpCfgFileLoaded.process.MessageLogger,dest),'filename'): 
            getattr(getattr(self.tmpCfgFileLoaded.process.MessageLogger,dest),'filename').setValue(additionalOutputFolderFWK+path.sep+path.split(addPostFixToFilename(getattr(getattr(self.tmpCfgFileLoaded.process.MessageLogger,dest),'filename').value(),samp.postfix))[1])     
  # create new file on disk 
  def createNewCfg (self,samp,putNewCfgHere,additionalOutputFolder=''):
    from os import path
    if not hasattr(self,'tmpCfgFileLoaded') or getattr(self,'tmpCfgFileLoaded') == None:
      self.applyChanges(samp,putNewCfgHere,additionalOutputFolder)
    # create new cfg
    newCfgFileName= addPostFixToFilename(self.cfgFileName , samp.postfix) 
    newCfgFileName = path.realpath(additionalOutputFolder) + path.sep + path.split(newCfgFileName)[1]
    newCfg = open(newCfgFileName , 'w')
    newCfg.write(self.tmpCfgFileLoaded.process.dumpPython())
    print newCfg.name
    newCfg.close()
    self.newCfgName = newCfg.name
  # process sample
  def runSample(self,samp,putNewCfgHere,additionalOutputFolder=''):
    if not additionalOutputFolder == '' and not additionalOutputFolder == None:
      additionalOutputFolder = os.path.realpath(additionalOutputFolder) + os.path.sep
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
    toBeRemoved = [self.tmpCfg,self.tmpCfg+'c']
    print "deleting tmp cfg ",self.tmpCfg, " and ",self.tmpCfg+'c'
    for tbR in toBeRemoved:
      try:
        os.remove(tbR)
      except OSError:
        pass
##############
def getFileMetaInformation(inputFiles):
 import subprocess,os,json
 return  json.loads(subprocess.Popen(["edmFileUtil -j "+" ".join(inputFiles)],shell=True,stdout=subprocess.PIPE,env=os.environ).communicate()[0])
## bookKeeping
class bookKeeping():
  def __init__(self):
    self.data = {}
  def numInputEvts(self,loadedCfg,postfix):
    inputFilesInfo = getFileMetaInformation(loadedCfg.process.source.fileNames.value())
    maxInputEvts = sum([f["events"] for f in inputFilesInfo])
    self.data[postfix] = {"totalEvents":maxInputEvts}
    maxEvtsProcess = loadedCfg.process.maxEvents.input.value()
    if maxEvtsProcess > 0 and maxEvtsProcess < maxInputEvts:
      self.data[postfix]["totalEvents"] = maxEvtsProcess
  def save(self,outputPath,timeStamp):
    import json
    with open(outputPath+'bookKeeping_'+timeStamp+'.json','wb') as bookKeepingFile:
      json.dump(self.data,bookKeepingFile)

