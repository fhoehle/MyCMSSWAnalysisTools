###
# run cmssw analysis
import FWCore.ParameterSet.Config as cms
import sys,imp,subprocess,os,getopt,re,argparse
sys.path.extend([os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools/Tools',os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools'])
import MyDASTools.dasTools as dasTools
import tools as myTools
import coreTools
import jsonTools 
import runRangesManagment
####
class cmsswAnalysis(object):
  def __init__(self,samples,cfg):
     self.cfg=cfg
     self.samples=samples
     self.timeStamp = coreTools.getTimeStamp()
  def readOpts(self):
    #opts, args = getopt.getopt(sys.argv[1:], '',['addOptions=','help','runGrid','runParallel=','specificSamples=','dontExec'])
    parser = argparse.ArgumentParser()
    parser.add_argument('--runGrid',action='store_true',default=False,help=' if crab should be called, specific crab arguments can be provided in sample dictionary')
    parser.add_argument('--gridOutputDir',default='test',help=' outputDirectory used for gridJob Output')
    parser.add_argument('--maxGridEvents',default=-99,help=' max Events for GridJob')
    parser.add_argument('--dontExec',action='store_true',default=False,help=' don\'t call crab or run cfgs just create them and crab.cfg if is used')
    parser.add_argument('--addOptions',type=str,default='',help="options used for cfg compilation options like runOnTTbar=True or outputPath")
    parser.add_argument('--runParallel',default=False,help="call multiple instances for cfgs each process runs on one cfg")
    parser.add_argument('--specificSamples',type=str,default=None,help="only process given samples given by labels")
    parser.add_argument('--debug',action='store_true',default=False,help=' activate debug modus ')
    parser.add_argument('--debugAnalysis',action='store_true',default=False,help=' activates analysis debug modus ')
    parser.add_argument('--usage',action='store_true',default=False,help='help message')
    parser.add_argument('--showAvailableSamples',action='store_true',default=False,help='show samples which can be processed')
    parser.add_argument('--runOnData',action='store_true',default=False,help=' activate running on data, will be transmitted to addOptions of cfg')
    parser.add_argument('--outputDirectory',default=os.getenv("PWD")+os.path.sep+'TMP_',help='dircetory where output is stored with additional timeStamp')
    parser.add_argument('--useXRootDAccess',action='store_true',default=False,help=' if dcap door down use xrootd alternative access')
    args = parser.parse_known_args()
    args,notKnownArgs = args
    self.debug = args.debug
    self.outputDirectory = args.outputDirectory+self.timeStamp+os.path.sep
    os.makedirs(os.path.dirname(self.outputDirectory))
    if not self.debug:
      self.newstdoutFile = self.outputDirectory+'log_'+self.timeStamp+'.txt'
      print "capturing stdout in ",self.newstdoutFile
      self.newstdoutFile = open(self.newstdoutFile, 'w')
      self.stdoutBck= sys.stdout
      sys.stdout = self.newstdoutFile
      print "was called by command "," ".join(sys.argv)
    self.useXRootDAccess = args.useXRootDAccess
    if args.usage:
      parser.print_help()
      print "========================"
      print "additional Options for specific cfg:"
      print "========================"
      helpCfgCmd = "python "+self.cfg+" --help"
      if self.debug:
        print helpCfgCmd
      helpCfgPr = myTools.coreTools.executeCommandSameEnv(helpCfgCmd)
      helpCfgPr.wait()
      print helpCfgPr.communicate()[0]
      sys.exit(0)
    if args.showAvailableSamples:
      print 'available samples: ',self.samples.keys()
      sys.exit(0)
    self.notKnownArgs = notKnownArgs
    if self.debug:
      print "notKnown ",notKnownArgs
    self.numProcesses=3
    if args.runParallel != False:
      self.numProcesses = int(args.runParallel)
    
    self.runParallel= args.runParallel 
    self.runGrid = args.runGrid
    self.specificSamples = args.specificSamples
    self.dontExec = args.dontExec
    self.addOptions=""
    print "addIOpts before ",self.addOptions
    self.addOptions=args.addOptions+( (' runOnData=True' if not 'runOnData=True' in args.addOptions and not 'runOnData=True' in self.addOptions else "") if args.runOnData else '')+(' debug=True' if args.debugAnalysis else "")
    print "addIOpts after ",self.addOptions
    for opt in args.__dict__.keys():
       myTools.removeOptFromArgv(opt)
    self.specificSamples = self.specificSamples if self.specificSamples != None else self.specificSamples
    options ={}
    options["maxEvents"]=1000
    self.outputDirectory = args.outputDirectory+self.timeStamp+os.path.sep
    options["outputPath"]=self.outputDirectory

    for opt in self.addOptions.split():
      reOpt = re.match('([^=]*)=([^=]*)',opt)
      if reOpt:
        options[reOpt.group(1)]=reOpt.group(2)
    print "all options ",options

    self.options =options
    self.args = args
    if self.debug:
      print self.__dict__
## preparing cfg with additional options
#make tmp copy  
  def startAnalysis(self):

    ### json output
    self.bookKeeping = myTools.bookKeeping(debug=self.debug)
    ####
    commandList = []
    dontExecCrab = self.dontExec
    print "starting analysis"
    for postfix,sampDict in self.samples.iteritems()if self.specificSamples == None else [(p,s) for p,s in self.samples.iteritems() if p in self.specificSamples ]:
      if self.debug:
        print "processing ",postfix," ",sampDict["localFile"]
        print "options before ",self.options.keys()," self.addOptions ",self.addOptions," samp ",(sampDict["addOptions"]) if sampDict.has_key("addOptions") else ""
      remainingOpts = myTools.removeAddOptions(['outputPath'],self.addOptions+(" "+sampDict["addOptions"] if sampDict.has_key("addOptions") else ""))
      if self.debug:
        print "remainingOptsTest ",remainingOpts
        print "notKnown ",self.notKnownArgs
      tmpCfg = self.cfg
      outputLocation=self.outputDirectory+sampDict["label"]+(os.path.sep if not sampDict["label"].endswith('/') else "")
      tmpCfg = myTools.createWorkDirCpCfg(outputLocation,tmpCfg,self.timeStamp)
      ##############
      analysisTriggers = myTools.processSample(tmpCfg).getTriggersUsedForAnalysis()
      if self.args.runOnData:
        dataTriggers = analysisTriggers
        print "running On Data: available triggers for channels: ",dataTriggers.keys()
        for k,dct in dataTriggers.iteritems():
          print "ch ",k," ",dataTriggers[k]['data']
      print "remainingOpts ",remainingOpts
      runRange=""
      if not self.runGrid:
        if self.args.runOnData:  # set runRange 
          import lumiListFromFile
          fileRuns = lumiListFromFile.getLumiListFromFile('dcap://grid-dcap.physik.rwth-aachen.de/pnfs/physik.rwth-aachen.de/cms'+sampDict["localFile"] if sampDict["localFile"].startswith('/store/') else sampDict["localFile"][5:]).getRuns()
          runRange=" runRange="+fileRuns[0]+"-"+fileRuns[-1]
        cfgSamp = myTools.compileCfg(tmpCfg,myTools.removeDuplicateCmsRunOpts(remainingOpts) + runRange,postfix ) 
        processSample =  myTools.processSample(cfgSamp)
        sample = myTools.sample(sampDict["localFile"],sampDict["label"],sampDict["xSec"],postfix,int(self.options["maxEvents"]))
        sample.loadDict(sampDict)
        sample.__dict__["color"]=sampDict["color"]
        if self.useXRootDAccess:
          sample.useXRootDLocation()
        processSample.applyChanges(sample)
        sys.stdout.flush()
        if not ( self.dontExec and not self.runParallel):
          commandList.append(processSample.runSample(not self.runParallel))
        self.bookKeeping.bookKeep(processSample)
      else:
        crabPs = []    
        sample = myTools.sample(sampDict["localFile"],sampDict["label"],sampDict["xSec"],postfix,int(self.options["maxEvents"]))
        sample.loadDict(sampDict)
        sample.__dict__["color"]=sampDict["color"]
        if self.useXRootDAccess:
          sample.useXRootDLocation()
        import CrabTools
        if not hasattr (sample,"datasetName"):
          sample.setDataset(postfix,debug=True)
        if self.args.runOnData:
          dataTriggers = analysisTriggers
          runRanges = []
          print "running On Data: available triggers for channels: ",dataTriggers.keys()
          for k,dct in dataTriggers.iteritems():
            print "ch ",k," ",dataTriggers[k]['data']
            runRanges.extend(dataTriggers[k]['data'].values())
          triggerRunRanges = runRangesManagment.runRangeManager(runRanges)
          triggerRunRanges.calcTriggerRunRanges()
          print "runRanges ",runRanges
          print "constTriggerRanges ",triggerRunRanges.ranges
          print "processing sample ",sample.datasetName
          myDASClient = dasTools.myDasClient(self.debug)
          myDASClient.limit=0
 	  DatasetLumilist = myDASClient.getJsonOfDataset(sample.datasetName)
	  onlyRunsDataset = [ int(r) for r in DatasetLumilist.getRuns()]
          print "onlyRuns ",onlyRunsDataset
          JSONfilename = sampDict.get("crabConfig")["CMSSW"]["lumi_mask"]
          JSONlumilist = jsonTools.LumiList (filename = JSONfilename)
          onlyRunsJSONfiles = [ int(r) for r in JSONlumilist.getRuns()]
          print "JSONfileRunsOnly ",onlyRunsJSONfiles
          datasetAndJSON = DatasetLumilist & JSONlumilist
          runDatasetAndJSON = [ int(r) for r in datasetAndJSON.getRuns()]
	  print "runs in both ",runDatasetAndJSON
	  print "runs from dataset removed ",[int(r) for r in (DatasetLumilist - datasetAndJSON).getRuns()]
          shortendJSONs = []
	  for runR in triggerRunRanges.ranges:
            if self.debug:
              print "runR[0] ",runR[0],"  runR[1] ",runR[1] 
            shortJSON = jsonTools.shortenJson(datasetAndJSON,runR[0],runR[1])
            if self.debug:
              print "len(shortJSON) ",len(shortJSON)
            if len(shortJSON):             
              shortJSONfilename = myTools.addPostFixToFilename (JSONfilename,'_'+postfix+'_part_'+str(len(shortendJSONs)))
              if self.debug:
                print "shortJSONfilename ",shortJSONfilename
	      setattr(shortJSON,'JSONfileName',shortJSONfilename);setattr(shortJSON,'label','_part_'+str(len(shortendJSONs)))
              shortendJSONs.append(shortJSON)
          if self.debug:
            print "numberOfCrabs ",len(shortendJSONs)
          for shJ in shortendJSONs:
            if self.debug:
              print "writing ",shJ.JSONfileName
            shJ.writeJSON(shJ.JSONfileName) 
            cfgSamp = myTools.compileCfg(tmpCfg,myTools.removeDuplicateCmsRunOpts(remainingOpts)+" runRange="+shJ.getRuns()[0]+"-"+shJ.getRuns()[-1],postfix+"_"+shJ.label ) 
            processSample =  myTools.processSample(cfgSamp)
            processSample.applyChanges(sample)
            print "processing ",postfix," ",sampDict["localFile"]
            sys.stdout.flush()
            processSample.setOutputFilesGrid()
            processSample.createNewCfg()
            self.bookKeeping.bookKeep(processSample)
            sys.stdout.flush()
            sys.path.append(os.getenv('CMSSW_BASE')+os.path.sep+'MyCMSSWAnalysisTools')
            default_lumis_per_job = 5
            sampDict["crabConfig"]["CMSSW"]["lumi_mask"]=shJ.JSONfileName
            if not sampDict["crabConfig"]["CMSSW"].has_key("lumis_per_job"):
              print "lumis_per_job given therefore used default ",default_lumis_per_job
              sampDict["crabConfig"]["CMSSW"]["lumis_per_job"]=default_lumis_per_job
            crabP = CrabTools.crabProcess(postfix+shJ.label,processSample.newCfgName,sample.datasetName,outputLocation,self.timeStamp,addGridDir=self.args.gridOutputDir+os.path.sep+os.path.basename(os.path.normpath(self.outputDirectory)))
            crabP.setCrabDir(sample.postfix+shJ.label,self.timeStamp,outputLocation)
	    keysToDelete = ['total_number_of_events',"number_of_jobs"]
            for kD in keysToDelete:
                if CrabTools.crabCfg["CMSSW"].has_key(kD):
                        del(CrabTools.crabCfg["CMSSW"][kD])
            crabP.createCrabCfg(sampDict.get("crabConfig"))
            print "lumi_mask",crabP.crabCfg["CMSSW"]["lumi_mask"]
            crabP.createCrabDir()
            crabP.writeCrabCfg()
            crabPs.append(crabP)
 

           #print "this many lumis ",len(shJ.getLumis())
        else:
          cfgSamp = myTools.compileCfg(tmpCfg,myTools.removeDuplicateCmsRunOpts(remainingOpts) + runRange,postfix )
          processSample =  myTools.processSample(cfgSamp)
          processSample.applyChanges(sample)
          print "processing ",postfix," ",sampDict["localFile"]
          sys.stdout.flush()
          processSample.setOutputFilesGrid()
          processSample.createNewCfg()
          self.bookKeeping.bookKeep(processSample)
          sys.stdout.flush()
          crabP = CrabTools.crabProcess(postfix,processSample.newCfgName,sample.datasetName,outputLocation,self.timeStamp,addGridDir=self.args.gridOutputDir+os.path.sep+os.path.basename(os.path.normpath(self.outputDirectory)))
          crabP.setCrabDir(sample.postfix,self.timeStamp,outputLocation)
          if self.args.maxGridEvents != -99:
            if sampDict.has_key("crabConfig"):
              if  sampDict["crabConfig"].has_key("CMSSW"):
                sampDict["crabConfig"]["CMSSW"]["total_number_of_events"]= self.args.maxGridEvents
              else:
                sampDict["crabConfig"]["CMSSW"] = {"total_number_of_events":self.args.maxGridEvents}
            else:     
              sampDict["crabConfig"] = {"CMSSW":{"total_number_of_events":self.args.maxGridEvents}}
          crabP.createCrabCfg(sampDict.get("crabConfig"))
          crabP.createCrabDir()
          crabP.writeCrabCfg()
          crabPs.append(crabP)
        print "number of crabs ",len(crabPs)
        for crabP in crabPs:
          crabP.create()#executeCrabCommand("-create",debug = True) 
          crabJsonFile = outputLocation+"/"+crabP.postfix+"_"+self.timeStamp+"_CrabCfg.json"
          CrabTools.saveCrabProp(crabP,crabJsonFile)
          if not dontExecCrab:
              crabP.submit()
              crabP.executeCrabCommand("-status")
          self.bookKeeping.addCrab(crabJsonFile)
    #processSample.end()
    dontExecParallel = self.dontExec
    if self.runParallel and len(commandList) > 0:
      print "running ",self.numProcesses," cmsRun in parallel"
      sys.path.append(os.getenv('CMSSW_BASE')+'/ParallelizationTools/BashParallel')
      import doWhatEverParallel
      if not dontExecParallel:
        doWhatEverParallel.execute(commandList,self.numProcesses)

  ##
    self.bookKeeping.save(self.outputDirectory+'/',self.timeStamp)
    if not self.debug:
      sys.stdout=self.stdoutBck
