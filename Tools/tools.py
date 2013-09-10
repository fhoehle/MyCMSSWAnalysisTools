# tools for run cmssw analysis
import FWCore.ParameterSet.Config as cms
import sys,imp,subprocess,re,os
def addPostFixToFilename(name,postfix):
  from os import path
  splittedName = path.splitext(name)
  return splittedName[0] + (("_"+postfix) if postfix != "" else "") + splittedName[1]
#########################
class sample(object):
  def __init__(self,filenames,label,xSec,postfix = "",maxEvents=-1,dataset = None):
    self.filenames = filenames
    self.postfix = postfix
    self.label = label
    self.xSec = float(xSec)
    self.maxEvents = maxEvents
    self.dataset = dataset
    if not isinstance(filenames,list):
     self.filenames = [filenames]
  def getInputfiles(self):
    inputFiles = cms.untracked.vstring([f for f in self.filenames if f.startswith('file:/') or  f.startswith('/store/')])
    if len(self.filenames) == len(inputFiles):
      return inputFiles
    else:
      sys.exit('inputFiles not okay')
  def setDataset(self):
    self.dataset = getDatasetName(self)
####################
def getDatasetName(sample):
  import sys,os,re
  sys.path.append(os.getenv('CMSSW_BASE')+os.path.sep+'/MyCMSSWAnalysisTools/MyDASTools')
  import dasTools
  myDasClient = dasTools.myDasClient()
  fileIdentifier = re.match('.*([0-9A-Z]{8}-[0-9A-Z]{4}-[0-9A-Z]{4}-[0-9A-Z]{4}-[0-9A-Z]{12}\.root)',sample.filenames[0]).group(1)
  print "searching dataset for ",fileIdentifier
  datasets = myDasClient.getDataSetNameForFile("*"+fileIdentifier)
  print "found ",datasets
  return datasets[0]
########################
def executeCommandSameEnv(command):
 import os,subprocess
 return subprocess.Popen([command],bufsize=1 , stdin=open(os.devnull),shell=True,stdout=subprocess.PIPE,env=os.environ)
class processSample(object):
  def __init__(self,cfgFileName,debug=False):
    self.cfgFileName = cfgFileName
    self.newCfgName = None
    self.tmpCfg = None
    self.debug = debug
    from os import getenv,path
#    self.workLoc = os.getenv('PWD') if workLoc == "" else workLoc
#    from os import path
    self.workLoc = path.dirname(self.cfgFileName) + path.sep
  def createTmpCfg(self):
    if self.tmpCfg != None:
      print "tmpCfg already exists ",self.tmpCfg 
      return 
    cfgFile = open(self.cfgFileName,'r') #copy.deepcopy(f);f.close()
    cfgFileLoaded = imp.load_source('cfgTMP',self.cfgFileName,cfgFile);cfgFile.close()
    from os import path
    tmpCfgName = addPostFixToFilename(self.cfgFileName,'_compiledInputCfg_TMP')
    tmpCfg = open(tmpCfgName , 'w')
    tmpCfg.write(cfgFileLoaded.process.dumpPython())
    tmpCfg.close()
    self.tmpCfg = tmpCfg.name
    if self.debug:
      print "created tmpCfg ",self.tmpCfg
  def loadTmpCfg(self):
    if not hasattr(self,'tmpCfg') or self.tmpCfg == None:
      self.createTmpCfg()
    import copy
    from os import path
    tmpCfgFile = open(self.tmpCfg,'r')
    self.tmpCfgFileLoaded = imp.load_source('tmpCfg_loaded',self.tmpCfg,tmpCfgFile);tmpCfgFile.close()
    if self.debug:
      print "loaded tmpCfg ",self.tmpCfg
  def setOutputFilesGrid(self):
    from os import path
    for outItem in self.tmpCfgFileLoaded.process.outputModules.values():
      outItem.fileName.setValue(path.basename(outItem.fileName.value()))
    if hasattr(self.tmpCfgFileLoaded.process,"TFileService"):
      self.tmpCfgFileLoaded.process.TFileService.fileName.setValue(path.basename(self.tmpCfgFileLoaded.process.TFileService.fileName.value()))
  ###
  def getListOfOutputFiles(self):
    from os import path
    outputList = []
    for outItem in self.tmpCfgFileLoaded.process.outputModules.values():
      outputList.append(path.basename(outItem.fileName.value()))
    if hasattr(self.tmpCfgFileLoaded.process,"TFileService"):
      outputList.append(path.basename(self.tmpCfgFileLoaded.process.TFileService.fileName.value()))
    return outputList
  def applyChanges(self,samp,cfgOutputFolder=''):
    # resetting
    self.newCfgName = None
    from os import path
    self.cfgOutputFolder = cfgOutputFolder if cfgOutputFolder != '' else self.workLoc
    if not path.exists(self.cfgOutputFolder): 
      os.makedirs(self.cfgOutputFolder)
    self.samp =  samp
    from os import path
    self.loadTmpCfg()
    #adapt input
    self.tmpCfgFileLoaded.process.source.fileNames = self.samp.getInputfiles();self.tmpCfgFileLoaded.process.maxEvents.input.setValue(self.samp.maxEvents)
    cfgOutputFolderFWK = 'file:'+path.realpath(self.cfgOutputFolder)
    #adapt output
    for outItem in self.tmpCfgFileLoaded.process.outputModules.values():
        outItem.fileName.setValue(cfgOutputFolderFWK+path.sep+path.split(addPostFixToFilename(outItem.fileName.value(),self.samp.postfix))[1])
    # TFileService
    if hasattr(self.tmpCfgFileLoaded.process,"TFileService"):
      self.tmpCfgFileLoaded.process.TFileService.fileName.setValue(cfgOutputFolderFWK+path.sep+path.split(addPostFixToFilename(self.tmpCfgFileLoaded.process.TFileService.fileName.value(),self.samp.postfix))[1])
    # MessageLogger
    if hasattr(self.tmpCfgFileLoaded.process,"MessageLogger"):
      for dest in self.tmpCfgFileLoaded.process.MessageLogger.destinations:
        if hasattr(self.tmpCfgFileLoaded.process.MessageLogger,dest):
          if hasattr(getattr(self.tmpCfgFileLoaded.process.MessageLogger,dest),'filename'): 
            getattr(getattr(self.tmpCfgFileLoaded.process.MessageLogger,dest),'filename').setValue(cfgOutputFolderFWK+path.sep+path.split(addPostFixToFilename(getattr(getattr(self.tmpCfgFileLoaded.process.MessageLogger,dest),'filename').value(),self.samp.postfix))[1])    
  def createNewCfgFileName(self,otherLoc = ''):
    from os import path
    loc = self.workLoc
    if not hasattr(self,'tmpCfgFileLoaded') or getattr(self,'tmpCfgFileLoaded') == None:
      print "load Cfg before"
      return
    if otherLoc != '':
      loc = path.realpath(otherLoc) + path.sep
      if not path.exists(loc):
        os.makedirs(loc)
    # create new cfg
    if not hasattr(self,'samp'):
      print "No changes were applied!!! "
      return
    newCfgFileName= addPostFixToFilename(self.cfgFileName , "_changesApplied")
    newCfgFileName = loc + path.split(newCfgFileName)[1] 
    self.newCfgName = newCfgFileName
    if self.debug:
      print "created newCfgName ",self.newCfgName
  # create new file on disk 
  def createNewCfg (self):
    if not hasattr(self,'newCfgName') or self.newCfgName == None:
      self.createNewCfgFileName()
    newCfg = open(self.newCfgName , 'w')
    newCfg.write(self.tmpCfgFileLoaded.process.dumpPython())
    newCfg.close()
    if self.debug:
      print "written newCfg on disk ",self.newCfgName
  def getLogFileName(self):
    if not hasattr(self,'newCfgName') or self.newCfgName == None:
      self.createNewCfgFileName()
    return self.newCfgName+"_output.log"
  # process sample
  def runSample(self,callCmsRun = True):
    from os import path
    if not hasattr(self,'newCfgName') or self.newCfgName == None or not path.isfile(self.newCfgName): 
      self.createNewCfg()
    command="cmsRun "+ self.newCfgName +' >& '+self.getLogFileName()
    print "run Analysis by calling:\n ",command  
    if callCmsRun:
      sys.stdout.flush()
      subPrOutput = subprocess.Popen([command],shell=True,stdout=subprocess.PIPE,env=os.environ)
      subPrOutput.wait()
      errorcode = subPrOutput.returncode
      print "ERRORCODE ",errorcode 
    else:
      return command
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
  def bookKeep(self,processSample):
    if not hasattr(processSample,"tmpCfgFileLoaded") or  not hasattr(processSample,"samp"):
      print "no bookKeeping, ",processSample
      return
    self.postfix = processSample.samp.postfix
    inputFilesInfo = getFileMetaInformation(processSample.tmpCfgFileLoaded.process.source.fileNames.value())
    maxInputEvts = sum([f["events"] for f in inputFilesInfo])
    self.data[self.postfix] = {"totalEvents":maxInputEvts}
    maxEvtsProcess = processSample.tmpCfgFileLoaded.process.maxEvents.input.value()
    if maxEvtsProcess > 0 and maxEvtsProcess < maxInputEvts:
      self.data[self.postfix]["totalEvents"] = maxEvtsProcess
    self.data[self.postfix]["cfg"] = processSample.newCfgName 
    self.data[self.postfix]["cfgLog"] = processSample.getLogFileName() 
    self.data[self.postfix]["outputFiles"] = processSample.getListOfOutputFiles()
    self.data[self.postfix]["sample"] = processSample.samp.__dict__
  def save(self,outputPath,timeStamp):
    import json
    with open(outputPath+'bookKeeping_'+timeStamp+'.json','wb') as bookKeepingFile:
      json.dump(self.data,bookKeepingFile)
  def addCrab(self,crabJson):
    self.data[self.postfix]["crabJob"]=crabJson
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
def removeOptFromArgv(opt,rem = False):
  import sys
  optFound = next( (o for o in sys.argv if re.match('^--'+opt+'={0,1}.*',o)),None)
  if optFound:
    idx = sys.argv.index(optFound)
    print idx, " ",optFound
    if len(sys.argv) > idx+1 and not sys.argv[idx+1].startswith('-'):
      sys.argv.pop(idx+1)
    sys.argv.remove(optFound)
###
def compileCfg(cfg,options,postfix=""):
  import os,shutil
  cpCfg = os.path.splitext(cfg)[0]+"_compileTMP"+os.path.splitext(cfg)[1]
  shutil.copyfile(cfg,cpCfg)
  cfgDumpPython = os.path.splitext(cfg)[0]+"_"+postfix+os.path.splitext(cfg)[1]
  with open(cpCfg,"a") as cfgAddLine: 
    cfgAddLine.write('myTmpFile = open ("'+cfgDumpPython+'","w"); myTmpFile.write(process.dumpPython()); myTmpFile.close() # added by script in order to dump/compile cfg');
  import subprocess
  buildFile = subprocess.Popen(["python "+cpCfg+" "+options],shell=True,stdout=subprocess.PIPE,env=os.environ)
  buildFile.wait()
  errorcode = buildFile.returncode
  if errorcode != 0:
    sys.exit("failed building config with "+str(errorcode))
    return 
  else:
    print "python cfg creation done"
    return cfgDumpPython
###
def getTimeStamp():
  import datetime,time
  return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')
def updateColorsBookKept(bookKept,inputDicts):
  if not hasattr(bookKept,"data"):
    print "wrong bookKept"
    return False
  for key in bookKept.data.keys():
    if not inputDicts.has_key(key):
      print "won't update",key," because not given in input"
      return False
    else:
      if not inputDicts[key].has_key("color"):
        print "no color provided for ",key
        return False
      else:
        bookKept.data[key]["sample"]["color"] =  inputDicts[key]["color"]
        return True
