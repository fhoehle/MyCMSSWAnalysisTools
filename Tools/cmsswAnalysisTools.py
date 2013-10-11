###
# run cmssw analysis
import FWCore.ParameterSet.Config as cms
import sys,imp,subprocess,os,getopt,re,argparse
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools/Tools')
import tools as myTools
####
class cmsswAnalysis(object):
  def __init__(self,samples,cfg):
     self.cfg=cfg
     self.samples=samples
  def readOpts(self):
    #opts, args = getopt.getopt(sys.argv[1:], '',['addOptions=','help','runGrid','runParallel=','specificSamples=','dontExec'])
    parser = argparse.ArgumentParser()
    parser.add_argument('--runGrid',action='store_true',default=False,help=' if crab should be called, specific crab arguments can be provided in sample dictionary')
    parser.add_argument('--dontExec',action='store_true',default=False,help=' don\'t call crab or run cfgs just create them and crab.cfg if is used')
    parser.add_argument('--addOptions',type=str,default='',help="options used for cfg compilation options like runOnTTbar=True or outputPath")
    parser.add_argument('--runParallel',default=False,help="call multiple instances for cfgs each process runs on one cfg")
    parser.add_argument('--specificSamples',type=str,default=None,help="only process given samples given by labels")
    parser.add_argument('--debug',action='store_true',default=False,help=' activate debug modus ')
    parser.add_argument('--usage',action='store_true',default=False,help='help message')
    parser.add_argument('--showAvailableSamples',action='store_true',default=False,help='show samples which can be processed')
    parser.add_argument('--runOnData',action='store_true',default=False,help=' activate running on data, will be transmitted to addOptions of cfg')
    args = parser.parse_known_args()
    args,notKnownArgs = args
    self.debug = args.debug
    print args.usage
    if args.usage:
      parser.print_help()
      sys.exit(0)
    if args.showAvailableSamples:
      print 'available samples: ',self.samples.keys()
      sys.exit(0)
    print "args ",args
    self.notKnownArgs = notKnownArgs
    print "notKnown ",notKnownArgs
    self.numProcesses=3
    if args.runParallel != False:
      self.numProcesses = int(args.runParallel)
    
    self.runParallel= args.runParallel 
    self.runGrid = args.runGrid
    self.specificSamples = args.specificSamples
    self.dontExec = args.dontExec
    self.addOptions=args.addOptions+(' runOnData=True' if args.runOnData else '')
    for opt in args.__dict__.keys():
       myTools.removeOptFromArgv(opt)
#      if opt in ("--help"):
#        print 'python runAnalysis.py --specificSamples label1,label2 --runParallel 2 --runGrid --addOptions \"maxEvents=1 outputPath=/net/scratch_cms/institut_3b/hoehle/hoehle/tmp\"'
#        sys.exit(0)
    self.specificSamples = self.specificSamples if self.specificSamples != None else self.specificSamples
    options ={}
    options["maxEvents"]=1000
    self.timeStamp = myTools.getTimeStamp()
    
    options["outputPath"]=os.getenv("PWD")+os.path.sep+'TMP_'+self.timeStamp+os.path.sep
    for opt in self.addOptions.split():
      reOpt = re.match('([^=]*)=([^=]*)',opt)
      if options.has_key(reOpt.group(1)):
        options[reOpt.group(1)]=reOpt.group(2)
      if not options["outputPath"].endswith(self.timeStamp+os.path.sep):
        options["outputPath"]= os.path.realpath(options["outputPath"])+"_"+self.timeStamp+os.path.sep 
      print options["outputPath"]
    self.options =options
    self.args = args
    if self.debug:
      print self.__dict__
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
      print "notKnown ",self.notKnownArgs
      analysisTriggers = myTools.processSample(tmpCfg).getTriggersUsedForAnalysis()
      if self.args.runOnData:
        dataTriggers = analysisTriggers
        print "running On Data: available triggers for channels: ",dataTriggers.keys()
        for k,dct in dataTriggers.iteritems():
          print "ch ",k," ",dataTriggers[k]['data']
       
      cfgSamp = myTools.compileCfg(tmpCfg,remainingOpts,postfix ) 
      processSample =  myTools.processSample(cfgSamp)
      sample = myTools.sample(sampDict["localFile"],sampDict["label"],sampDict["xSec"],postfix,int(self.options["maxEvents"]))
      sample.loadDict(sampDict)
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
        crabP = CrabTools.crabProcess(postfix,processSample.newCfgName,sample.datasetName,self.options["outputPath"],self.timeStamp,addGridDir="test")
        crabP.setCrabDir(sample.postfix,self.timeStamp,self.options["outputPath"])
        crabP.createCrabCfg(sampDict.get("crabConfig"))
        crabCfgFilename = crabP.createCrabDir()
        crabP.writeCrabCfg()
        crabP.create()#executeCrabCommand("-create",debug = True) 
        crabJsonFile = self.options["outputPath"]+"/"+postfix+"_"+self.timeStamp+"_CrabCfg.json"
        CrabTools.saveCrabProp(crabP,crabJsonFile)
        if not dontExecCrab:
          crabP.submit()
          crabP.executeCrabCommand("-status")
        self.bookKeeping.addCrab(crabJsonFile)
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
