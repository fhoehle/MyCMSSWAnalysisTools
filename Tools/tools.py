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
    self.dataset = None
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
  def getSampleName(self):
    import sys,os,re
    sys.path.append(os.getenv('CMSSW_BASE')+os.path.sep+'/MyCMSSWAnalysisTools/MyDASTools')
    import dasTools
    myDasClient = dasTools.myDasClient()
    fileIdentifier = re.match('.*([0-9A-Z]{8}-[0-9A-Z]{4}-[0-9A-Z]{4}-[0-9A-Z]{4}-[0-9A-Z]{12}\.root)',self.name[0]).group(1)
    print "searching dataset for ",fileIdentifier
    datasets = myDasClient.getDataSetNameForFile("*"+fileIdentifier)
    print "found ",datasets
    self.dataset = datasets[0]
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
  def  setOutputFilesGrid(self):
    from os import path
    for outItem in self.tmpCfgFileLoaded.process.outputModules.values():
      outItem.fileName.setValue(path.basename(outItem.fileName.value()))
    if hasattr(self.tmpCfgFileLoaded.process,"TFileService"):
      self.tmpCfgFileLoaded.process.TFileService.fileName.setValue(path.basename(self.tmpCfgFileLoaded.process.TFileService.fileName.value()))
    print "testing TFileServie set ",self.tmpCfgFileLoaded.process.TFileService.fileName.value()
  ###
  def getListOfOutputFiles(self):
    from os import path
    outputList = []
    for outItem in self.tmpCfgFileLoaded.process.outputModules.values():
      outputList.append(path.basename(outItem.fileName.value()))
    if hasattr(self.tmpCfgFileLoaded.process,"TFileService"):
      outputList.append(path.basename(self.tmpCfgFileLoaded.process.TFileService.fileName.value()))
    return outputList
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
    print "test self.newCfgName ",self.newCfgName
    print "testing TFileServie createNewCfg ",self.tmpCfgFileLoaded.process.TFileService.fileName.value()
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
#####
def createWorkDirCpCfg(wDir,cfg,timeSt):
  import os,shutil
  cpCfg = wDir+ os.path.splitext(os.path.basename(cfg))[0]+"_"+timeSt + os.path.splitext(os.path.basename(cfg))[1]
  os.makedirs(os.path.dirname(cpCfg))
  shutil.copyfile(cfg,cpCfg)
  return cpCfg
####
def removeAddOptions(toRemove,options):
  for key in toRemove:
   options = re.sub(key+'=[^\ ]*','',options)
  return options
###
def compileCfg(cfg,options,addOptions):
  import os
  remainingOptions = removeAddOptions(options.keys(),addOptions)
  if remainingOptions != '' and not remainingOptions.isspace():
    cfgDumpPython = os.path.splitext(cfg)[0]+"_addedDumpLine"+os.path.splitext(cfg)[1]
    with open(cfg,"a") as cfgAddLine: 
      cfgAddLine.write('myTmpFile = open ("'+cfgDumpPython+'","w"); myTmpFile.write(process.dumpPython()); myTmpFile.close() # added by script in order to dump/compile cfg');
    import subprocess
    buildFile = subprocess.Popen(["python "+cfg+" "+remainingOptions],shell=True,stdout=subprocess.PIPE,env=os.environ)
    buildFile.wait()
    errorcode = buildFile.returncode
    if errorcode != 0:
      sys.exit("failed building config with "+str(errorcode))
      return 
    else:
      print "python cfg creation done"
    return cfgDumpPython
  else:
    return cfg
###
def getTimeStamp():
  import datetime,time
  return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')
