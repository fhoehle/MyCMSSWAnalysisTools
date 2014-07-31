# tools for run cmssw analysis
import FWCore.ParameterSet.Config as cms
import sys,imp,subprocess,re,os,ROOT
ROOT.TH1.AddDirectory(False)
sys.path.extend([os.getenv('HOME')+'/PyRoot_Helpers/PyRoot_Functions',os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools/Tools'])
import coreTools
import MyHistFunctions_cfi as MyHistFunctions
#########################
class analysisSample (object):
  def __init__(self,lumiForPlots):
    self.lumiForPlots = lumiForPlots
  def loadFromDataset(self,dS,mergeLabel = None):
    if not isinstance(dS,list):
      dS = [dS]
    self.datasets=dS
    if len(dS) > 1 and not mergeLabel:
      print "provide a merge label"
      return
    if len(dS) > 1:
      self.label = mergeLabel
      self.mergedSample = True
    else:
      self.mergedSample = False
      self.__dict__.update(dS[0])
      self.processedLumi = float(self.processedEvents)/float(self.xSec)
  def merge(self,color = None ):
    self.color = color if color else self.datasets[0]["color"]
    self.xSec = sum([float(ds["xSec"]) for ds in self.datasets])
    if not self.lumiForPlots:
      print "no lumi for merge given"
      return
    self.processedLumi = self.lumiForPlots 
  def scalePlot(self,ds,plot):
     if not ds["processedEvents"] or not ds["xSec"] or not self.lumiForPlots:
       print "lumi for scaling not given "
       return
     plot.Sumw2()
     plot.Scale(self.lumiForPlots/(float(ds["processedEvents"])/float(ds["xSec"])))
     return plot
  def get(self,plotname):
    if self.mergedSample:
      plot = None;plot = ROOT.TFile(self.datasets[0]["file"]).Get(plotname);plot = plot.Clone(plot.GetName()+"_mergedTo_"+self.label)
      plot = self.scalePlot(self.datasets[0],plot)
      for ds in self.datasets[1:]:
        tmpP = ROOT.TFile(ds["file"]).Get(plotname);tmpP = tmpP.Clone(tmpP.GetName()+"_isMergedTo_"+self.label)
        MyHistFunctions.addOverFlowToLastBin(tmpP)
        tmpP = self.scalePlot(ds,tmpP)
        plot.Add(tmpP)
      plot.SetLineColor(self.color)
      return plot
    else:
      plot = ROOT.TFile(self.datasets[0]["file"]).Get(plotname);plot = plot.Clone(plot.GetName()+"_gotByAS")
      MyHistFunctions.addOverFlowToLastBin(plot)
      plot.SetLineColor(self.color)
      return self.scalePlot(self.datasets[0],plot)
############################
class sample(object):
  def __init__(self,filenames=[],label=None,xSec=None,postfix = "",maxEvents=-1,datasetName = None):
    self.filenames = filenames
    self.postfix = postfix
    self.label = label
    self.xSec = float(xSec) if xSec else None
    self.maxEvents = maxEvents
    self.datasetName = datasetName
    self.useXRootDAccess=False
    self.debug=False
    if not isinstance(filenames,list) and not filenames:
     self.filenames = [filenames]
     print "correcting",filenames
    else:
      print "is already list"
  def loadDict(self,dictionary):
    for k in self.__dict__.keys():
     if dictionary.has_key(k):
       self.__dict__[k]=dictionary[k]
  def useXRootDLocation(self):
    self.useXRootDAccess=True
  def getInputfiles(self):
    print "filenames, ",self.filenames,","
    inputFiles = None
    if self.filenames != [None]:
      print 'hae'
    if self.filenames != [None] and self.filenames != [] and not self.filenames:
      inputFiles = cms.untracked.vstring([f for f in self.filenames if f and (f.startswith('file:/') or  f.startswith('/store/')) or f.startswith('root://')])
    else:
      return None
    if self.useXRootDAccess:
      import alternativeLocation
      xRootDPathMaker = alternativeLocation.xRootDPathCreator(debug=self.debug)
      xRootDInputfiles=[]
      for inputF in inputFiles:
        if not inputF.startswith('file:/') and f.startswith('/store/'):
          xRootDInputfiles.append(str(xRootDPathMaker.getXrootDPath(inputF)))
      inputFiles = cms.untracked.vstring(xRootDInputfiles)
    if len(self.filenames) == len(inputFiles):
      return inputFiles
    elif not self.datasetName:
      sys.exit('inputFiles not okay')
    else:
      return None
  def setDataset(self,datasetName,debug=False):
    self.datasetName = getDatasetNameFromString(datasetName,debug=debug)
####################
def getDatasetNameFromString(DatasetNameInString,debug=False):
#  if not hasattr(sample,'filenames') or sample.filenames == None or len(sample.filenames) == 0:
#    print 'sample.filenames not given for sample ',sample.label
#    return None
#  import sys,os,re
#  sys.path.append(os.getenv('CMSSW_BASE')+os.path.sep+'/MyCMSSWAnalysisTools/MyDASTools')
#  import dasTools
#  myDasClient = dasTools.myDasClient(debug=debug)
  datasetName = '/'+re.sub('__','/',DatasetNameInString)
  if debug:
    print " datasetName ",datasetName
  return datasetName
def getStringFromDatasetName(datasetName):
  return re.sub('/','__',datasetName.lstrip('/'))
def getLabelFromDatasetName(datasetName):
  return re.sub('/','_',datasetName.strip('/').rstrip('AOD')).rstrip('_')
########################
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
  def loadInputCfg(self):
    return coreTools.loadFileAsModule(self.cfgFileName,modName='cfgTMP')
  def createTmpCfg(self):
    if self.tmpCfg != None:
      print "tmpCfg already exists ",self.tmpCfg 
      return 
    cfgFileLoaded = self.loadInputCfg()
    from os import path
    tmpCfgName = coreTools.addPostFixToFilename(self.cfgFileName,'_compiledInputCfg_TMP')
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
    import copy
    self.samp =  copy.deepcopy(samp)
    from os import path
    self.loadTmpCfg()
    #adapt input
    tmpInputFIles = self.samp.getInputfiles()
    print "input from Sample ",tmpInputFIles
    if tmpInputFIles and  len(tmpInputFIles) : 
      self.tmpCfgFileLoaded.process.source.fileNames =  tmpInputFIles;
    else:
      print "not inputFiles set. dataset given ",self.samp.datasetName
    self.tmpCfgFileLoaded.process.maxEvents.input.setValue(self.samp.maxEvents)
    cfgOutputFolderFWK = 'file:'+path.realpath(self.cfgOutputFolder)
    #adapt output
    for outItem in self.tmpCfgFileLoaded.process.outputModules.values():
        outItem.fileName.setValue(cfgOutputFolderFWK+path.sep+path.split(coreTools.addPostFixToFilename(outItem.fileName.value(),self.samp.postfix))[1])
    # TFileService
    if hasattr(self.tmpCfgFileLoaded.process,"TFileService"):
      self.tmpCfgFileLoaded.process.TFileService.fileName.setValue(cfgOutputFolderFWK+path.sep+path.split(coreTools.addPostFixToFilename(self.tmpCfgFileLoaded.process.TFileService.fileName.value(),self.samp.postfix))[1])
    # MessageLogger
    if hasattr(self.tmpCfgFileLoaded.process,"MessageLogger"):
      for dest in self.tmpCfgFileLoaded.process.MessageLogger.destinations:
        if hasattr(self.tmpCfgFileLoaded.process.MessageLogger,dest):
          if hasattr(getattr(self.tmpCfgFileLoaded.process.MessageLogger,dest),'filename'): 
            getattr(getattr(self.tmpCfgFileLoaded.process.MessageLogger,dest),'filename').setValue(cfgOutputFolderFWK+path.sep+path.split(coreTools.addPostFixToFilename(getattr(getattr(self.tmpCfgFileLoaded.process.MessageLogger,dest),'filename').value(),self.samp.postfix))[1])    
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
    newCfgFileName= coreTools.addPostFixToFilename(self.cfgFileName , "_changesApplied")
    newCfgFileName = loc + path.split(newCfgFileName)[1] 
    self.newCfgName = newCfgFileName
    if self.debug:
      print "created newCfgName ",self.newCfgName
  # create new file on disk
  def getTriggersUsedForAnalysis (self):
    from sys import stdout
    origSTDOUT = stdout; stdout = NullService()
    inputLoaded = self.loadInputCfg()
    stdout = origSTDOUT
    return inputLoaded.triggersUsedForAnalysis if hasattr(inputLoaded,'triggersUsedForAnalysis') else None
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
def getFileMetaInformation(inputFile,debug=False):
  import subprocess,os,json
  edmFileUtil_out = subprocess.Popen(["edmFileUtil -j "+inputFile],shell=True,stdout=subprocess.PIPE,env=os.environ).communicate()[0]
  if debug:
    print "edmFileUtil_out ",edmFileUtil_out
  try:
    return  json.loads(edmFileUtil_out)
  except ValueError, e:
    print "getFileMetaInformation "+inputFile+" not valid json: "
    print edmFileUtil_out
    return None
## bookKeeping
class bookKeeping():
  def __init__(self,debug=False):
    self.data = {}
    self.debug=debug
  def bookKeep(self,processSample,runGrid=False):
    if not hasattr(processSample,"tmpCfgFileLoaded") or  not hasattr(processSample,"samp"):
      print "no bookKeeping, ",processSample
      return
    self.postfix = processSample.samp.postfix
    maxEvtsProcess = processSample.tmpCfgFileLoaded.process.maxEvents.input.value()
    maxInputEvts = 0
    if not runGrid:
      for inputF in processSample.tmpCfgFileLoaded.process.source.fileNames.value():
        inputFileInfo = getFileMetaInformation(inputF,self.debug)
        maxInputEvts +=inputFileInfo[0]["events"]
        if maxEvtsProcess > 0 and maxEvtsProcess <= maxInputEvts:
          break
      self.data[self.postfix] = {"totalEvents":maxInputEvts}
      if maxEvtsProcess > 0 and maxEvtsProcess < maxInputEvts:
        self.data[self.postfix]["totalEvents"] = maxEvtsProcess
    else:
      self.data[self.postfix]={"runGrid":runGrid}
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
def removeDuplicateCmsRunOpts(options):
  cleanedOpts=""
  for opt in options.split():
    if not opt in cleanedOpts:
      cleanedOpts +=" "+opt
  return cleanedOpts 
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
def compileCfg(cfg,options,postfix="",debug=False):
  import os,shutil
  cpCfg = os.path.splitext(cfg)[0]+"_compileTMP"+os.path.splitext(cfg)[1]
  shutil.copyfile(cfg,cpCfg)
  cfgDumpPython = os.path.splitext(cfg)[0]+"_"+postfix+os.path.splitext(cfg)[1]
  with open(cpCfg,"a") as cfgAddLine: 
    cfgAddLine.write('myTmpFile = open ("'+cfgDumpPython+'","w"); myTmpFile.write(process.dumpPython()); myTmpFile.close() # added by script in order to dump/compile cfg');
  import subprocess
  print "compiling options ",options
  compileCmd = "python "+cpCfg+" "+options
  if debug:
    print "compileCfg compileCmd: ",compileCmd
  buildFile = subprocess.Popen([compileCmd],shell=True,stdout=subprocess.PIPE,env=os.environ)
  buildFile.wait()
  errorcode = buildFile.returncode
  if errorcode != 0:
    print buildFile.communicate()[0]
    sys.exit("failed building config with "+str(errorcode))
    return 
  else:
    print "python cfg creation done"
    return cfgDumpPython
###
def _pretty_lines(self, keys):
        size = max(len(k) for k in keys) + 2
        return "{\n" + ",\n".join(
                    ("%"+str(size)+"s: ")%("'"+k+"'")
                    + repr(getattr(self, k))
                    for k in keys
                ) + ",\n}"
class NullService():
   def write(self,s):
     pass
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
def datasetToName(dsName):
  return dsName.strip().strip('/').replace('/','__')
###############
class frameworkJobReportParser (object):
  def __init__(self,xmlFile):
    import xml.dom.minidom as minidom
    self.xmlFile = xmlFile
    dom = minidom.parse(xmlFile)
    self.FrameworkJobReport = coreTools.myGetSubNodeByName(dom,"FrameworkJobReport")
    if len(self.FrameworkJobReport) != 1:
      print "multiple FrameworkJobReports found"
      self.FrameworkJobReport = None
    self.FrameworkJobReport = self.FrameworkJobReport[0]
  def getFile(self):
    self.File = coreTools.myGetSubNodeByName(self.FrameworkJobReport,'File')
    if len(self.File) != 1:
      print "multiple Files found"
      self.File = None
    self.File=self.File[0]
  def getFileLFN(self):
    import re
    if not hasattr(self,'File'):
      self.getFile()
    return " ".join([str(re.sub(r'\s', '', n.nodeValue)) for n in coreTools.myGetSubNodeByName(self.File,'LFN')[0].childNodes])
  def getFileSize(self):
    import re
    if not hasattr(self,'File'):
      self.getFile()
    return " ".join([str(re.sub(r'\s', '', n.nodeValue)) for n in coreTools.myGetSubNodeByName(self.File,'Size')[0].childNodes])
  def getInputFiles(self):
    return [ coreTools.myGetSubNodeByName(iF,'LFN')[0].childNodes[0].nodeValue.strip() for iF in coreTools.myGetSubNodeByName(self.FrameworkJobReport,'InputFile') ]
