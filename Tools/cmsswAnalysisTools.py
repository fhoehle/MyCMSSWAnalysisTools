###
# run cmssw analysis
import FWCore.ParameterSet.Config as cms
import sys,imp,subprocess,os,getopt,re
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools/Tools')
import tools as myTools
####
class cmsswAnalysis(object):
  def __init__(self,samples,cfg):
     self.cfg=cfg
     self.samples=samples
  def readOpts(self):
    opts, args = getopt.getopt(sys.argv[1:], '',['addOptions=','help','runGrid','runParallel=','specificSamples=','dontExec'])
    print "given opts ",sys.argv
    self.numProcesses=3
    self.runParallel=False
    self.addOptions=''
    self.runGrid = False
    self.specificSamples = None
    self.dontExec = False
    for opt,arg in opts:
      if opt in ("--addOptions"):
        self.addOptions=arg
        myTools.removeOptFromArgv(opt,True)
      if opt in ("--runGrid"):
        self.runGrid = True
        myTools.removeOptFromArgv(opt)
      if opt in ("--dontExec"):
        self.dontExec = True; myTools.removeOptFromArgv(opt)
      if ("--runParallel") in opt:
        self.numProcesses =  int(arg) if arg != None  else self.numProcesses
        myTools.removeOptFromArgv(opt,arg != None)
        self.runParallel=True
      if ("--specificSamples") in opt:
        self.specificSamples = arg.split(",")
        myTools.removeOptFromArgv(opt,True)
      if opt in ("--help"):
        print 'python runAnalysis.py --specificSamples label1,label2 --runParallel 2 --runGrid --addOptions \"maxEvents=1 outputPath=/net/scratch_cms/institut_3b/hoehle/hoehle/tmp\"'
        sys.exit(0)
    options ={}
    options["maxEvents"]=1000
    self.timeStamp = myTools.getTimeStamp()
    options["outputPath"]=os.getenv("PWD")+os.path.sep+'TMP_'+self.timeStamp
    for opt in self.addOptions.split():
      reOpt = re.match('([^=]*)=([^=]*)',opt)
      if options.has_key(reOpt.group(1)):
        options[reOpt.group(1)]=reOpt.group(2)
      if options["outputPath"] != os.getenv("PWD"):
        options["outputPath"]= os.path.realpath(options["outputPath"])+"_"+self.timeStamp+os.path.sep 
      print options["outputPath"]
    self.options =options
## preparing cfg with additional options
#make tmp copy  
  def startAnalysis(self):
    tmpCfg = self.cfg
    tmpCfg = myTools.createWorkDirCpCfg(self.options["outputPath"],tmpCfg,self.timeStamp)
    ### json output
    self.bookKeeping = myTools.bookKeeping()
    ####
    commandList = []
    dontExecCrab = self.dontExec

    for postfix,sampDict in self.samples.iteritems()if self.specificSamples == None else [(p,s) for p,s in self.samples.iteritems() if p in self.specificSamples ]:
      remainingOpts = myTools.removeAddOptions(self.options.keys(),self.addOptions+(" "+sampDict["addOptions"]) if sampDict.has_key("addOptions") else "")
      print "remainingOpts ",remainingOpts
      cfgSamp = myTools.compileCfg(tmpCfg,remainingOpts,postfix ) 
      processSample =  myTools.processSample(cfgSamp)
      sample = myTools.sample(sampDict["localFile"],sampDict["label"],sampDict["xSec"],postfix,int(self.options["maxEvents"]))
      sample.__dict__["color"]=sampDict["color"]
      processSample.applyChanges(sample)
      print "processing ",postfix," ",sampDict["localFile"]
      sys.stdout.flush()
      if not self.runGrid:
        if not ( self.dontExec and not self.runParallel):
          commandList.append(processSample.runSample(not self.runParallel))
        self.bookKeeping.bookKeep(processSample)
      else:
        processSample.setOutputFilesGrid()
        processSample.createNewCfg()
        self.bookKeeping.bookKeep(processSample)
        sys.stdout.flush()
        sys.path.append(os.getenv('CMSSW_BASE')+os.path.sep+'MyCMSSWAnalysisTools')
        import CrabTools
        sample.setDataset()
        crabP = CrabTools.crabProcess(postfix,processSample.newCfgName,sample.dataset,self.options["outputPath"],self.timeStamp,addGridDir="test")
        crabP.setCrabDir(sample.postfix,self.timeStamp,self.options["outputPath"])
        crabP.createCrabCfg(sampDict.get("crabConfig"))
        crabCfgFilename = crabP.createCrabDir()
        crabP.writeCrabCfg()
        crabP.executeCrabCommand("-create",debug = True) 
        CrabTools.saveCrabProp(crabP,self.options["outputPath"]+"/"+postfix+"_"+self.timeStamp+"_CrabCfg.json")
        if not dontExecCrab:
          crabP.executeCrabCommand("-submit",debug = True)
          crabP.executeCrabCommand("-status")
    processSample.end()
    dontExecParallel = self.dontExec
    if self.runParallel and len(commandList) > 0:
      print "running ",self.numProcesses," cmsRun in parallel"
      sys.path.append(os.getenv('CMSSW_BASE')+'/ParallelizationTools/BashParallel')
      import doWhatEverParallel
      if not dontExecParallel:
        doWhatEverParallel.execute(commandList,self.numProcesses)

  ##
    self.bookKeeping.save(self.options["outputPath"]+'/',self.timeStamp)
   
