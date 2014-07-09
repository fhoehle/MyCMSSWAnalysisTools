### default dict
import sys,os
sys.path.extend([ os.getenv('CMSSW_BASE')+os.path.sep+p for p in ['MyCrabTools','MyCMSSWAnalysisTools','ParallelizationTools']])
import crabDeamonTools
import Tools.tools as tools
import Tools.alternativeLocation as alternativeLocation
crabCfg = {
  "CRAB" :{
    "jobtype":"cmssw"
    ,"scheduler" : "remoteGlidein" #sge
    #,"server_name" : "legnaro"
    #,"submit_host": "cern_vocms20" #"cern_vocms83"
    ,"use_server" : 0
    }
  #,"SGE":{
  #  "resource" :" -V -l h_vmem=2G  -l site=hh -M hoehle@physik.rwth-aachen.de -m aesb"
  #  ,"se_white_list": " dcache-se-cms.desy.de "
  #  }
  ,"CMSSW":{
    "dbs_url":"http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet" #"https://cmsdbsprod.cern.ch:8443/cms_dbs_ph_analysis_02_writer/servlet/DBSServlet"
    ,"datasetpath":"/test/USER"
    ,"pset":"test.py"
    #,"number_of_jobs" : 500
    ,"total_number_of_events" : -1
    #,"total_number_of_lumis" : -1
    #,"lumis_per_job" :  1000
    ,"events_per_job":10000
    ,"output_file" : "test.root"
    #,"lumi_mask": "test_Cert_136033-147120_7TeV_Nov4ReReco_Collisions10_JSON.txt"
    #,"generator":"lhe"
    }
  ,"USER":{
    "return_data" : 0
    ,"copy_data" :  1
    #,"storage_path" : "/srm/managerv2?SFN=/pnfs/physik.rwth-aachen.de/cms/store/user/fhohle/"
    ,"storage_element" : "T2_DE_RWTH"
    ,"user_remote_dir" : "/MyTestOutputRemoteDir/"
    ,"check_user_remote_dir": 0
    #,"publish_data" : 1
    #,"publish_data_name" : "MyTestPublish"
    #,"dbs_url_for_publication" : "https://cmsdbsprod.cern.ch:8443/cms_dbs_ph_analysis_02_writer/servlet/DBSServlet"
    #,"additional_input_files":"/test/tta01.lhef"
    #,"script_exe" : "test.sh"
    ### The additional arguments for script_exe (comma separated list)
    #,"script_arguments" = "a,b,c"
    }
  ,"GRID":{
    "group" : "dcms"
    #,"role" : "priorityuser"
    ,"dont_check_proxy":1
    #,"ce_white_list" : "T2_DE_RWTH"
    #,"se_white_list" : "dcache-se-cms.desy.de"
    #,"ce_black_list" : "T2_DE_RWTH"
    #,"se_black_list" : "T2_TW_Taiwan"
    }
}
###
def checkGridCert():
  import subprocess,os
  subPrOutput = subprocess.Popen(["voms-proxy-info -exists"],shell=True,stdout=subprocess.PIPE,env=os.environ)
  subPrOutput.wait()
  return subPrOutput.returncode == 0
###
class crabProcess(crabDeamonTools.crabDeamon):
  def __init__(self,postfix,cfg,samp,workdir,timeSt,addGridDir=""):
    self.crabJobDir = None
    self.postfix = postfix
    self.cfg = cfg
    self.samp = samp
    self.workdir = workdir
    self.timeSt = timeSt 
    self.addGridDir = addGridDir
    self.user_remote_dir = self.addGridDir +( "/" if self.addGridDir != "" and self.addGridDir != None else "") #+ self.postfix+"_"+self.timeSt
    self.__type__="crabProcess"
    self.crabJobDir = None
    self.checkRequirements()
  def applyChanges(self,willBeChanged,changesGivenHere):
    for k,i in changesGivenHere.iteritems():
      if not willBeChanged.has_key(k):
        willBeChanged[k]=i
      else:
        willBeChanged[k].update(i)
  def checkRequirements(self):
    import sys,os
    if not checkGridCert():
      sys.exit("grid cert not okay, test voms-proxy-init failed")
    if not os.environ.has_key("CRABDIR"):
      sys.exit("crab not found")
  def writeCrabCfg(self):
    import os
    cCfg = open(self.crabDir.rstrip('/')+os.path.sep+"crab.cfg" ,'w')
    for sec,vals in self.crabCfg.iteritems():
      cCfg.write("["+sec+"]");cCfg.write("\n")
      for k,val in vals.iteritems():
        cCfg.write(k+"="+str(val));cCfg.write("\n")
    cCfg.close()
    self.crabCfgFilename = cCfg.name
    if self.crabCfg.has_key('CMSSW') and self.crabCfg['CMSSW'].has_key('lumi_mask') and self.crabCfg['CMSSW']['lumi_mask']:
      from shutil import copy2
      print "copying lumi_mask: ", self.crabCfg['CMSSW']['lumi_mask']," to ",os.path.dirname(cCfg.name) + os.path.sep
      copy2(self.crabCfg['CMSSW']['lumi_mask'],os.path.dirname(cCfg.name) + os.path.sep)
    return cCfg.name 
  ###
  def setCrabDir(self,addCrabDir ="",timeSt = "",workdir = ""):
    if not workdir == "":
       self.workdir = workdir
    import os
    crabDir = os.path.realpath(self.workdir)+((os.path.sep+addCrabDir) if addCrabDir != "" else "")+(("_"+timeSt) if timeSt != "" else "")
    self.crabJsonFile = self.workdir+"/"+self.postfix+"_"+self.timeSt+"_CrabCfg.json"
    #if self.debug:
    print "crabDir ",crabDir
    self.crabDir = crabDir + os.path.sep if not crabDir.endswith('/') else "" 
    return crabDir
  def createCrabDir(self):
    import os
    if not os.path.isdir(self.crabDir):
      os.makedirs(self.crabDir)
  ##
  def createCrabCfg(self,changes = None):
    import copy
    tmpCrabCfg = crabCfg
    tmpCrabCfg["CMSSW"]["pset"]=self.cfg
    tmpCrabCfg["USER"]["user_remote_dir"] = self.user_remote_dir
    tmpCrabCfg["CMSSW"]["get_edm_output"] = 1
    tmpCrabCfg["CMSSW"].pop("output_file",None)
    tmpCrabCfg["CMSSW"]["datasetpath"]=self.samp
    if changes != None:
      self.applyChanges(tmpCrabCfg,changes)  
    self.crabCfg =copy.deepcopy(tmpCrabCfg)
    return self.crabCfg
  def create(self):
    self.executeCrabCommand("-create",debug = True)
    self.findCrabJobDir(self.crabDir)
  def reportLumi(self):
    import re
    import Tools.lumiTools as lumiTools
    self.executeCrabCommand("-report",debug = True) 
    self.intLuminosity=lumiTools.calcLumi(self.crabJobDir+"/res/lumiSummary.json")
    return self.intLuminosity
  def changeCrabJobDir(self,newDir):
    self.crabJobDir = newDir
  def executeCrabCommand(self,command,debug = False,returnOutput = False):
    if not hasattr(self,'crabDir'):
      #self.createCrabDir()
      sys.exit('crab dir is missing')
    #if not hasattr(self,'crabJobDir') or not self.crabJobDir or self.crabJobDir == '': 
    return  self.executeCommand(command,debug ,returnOutput,where=self.crabDir)
    #else:
    #  return  self.executeCommand(command,debug ,returnOutput)
  def submit(self,debug=False):
    output = self.executeCrabCommand("-status",False,True)
    import re
    listJobs = [ l.split()[0] for l in output if re.match('^[0-9]+[ \t]+[YN][ \t]+Created[ \t]+Created',l)]
    if debug:
      print listJobs
    self.multiCommand('-submit',listJobs,True)
  def getAcGridDir(self):
    import os,subprocess
    if self.crabCfg["USER"].has_key("user_remote_dir"):
      return '/pnfs/physik.rwth-aachen.de/cms/store/user/fhohle/'+self.crabCfg["USER"]["user_remote_dir"]
    else:
      return None
  def getJobFJR(self,no,debug=False):
    import os
    fjrPath = None
    if not self.crabJobDir:
      print "no self.crabJobDir found"
    elif not os.path.exists(resPath):
      print "no directory res found in ",self.crabJobDir
    else:
      fjrPath = self.crabJobDir+os.path.sep+'res/'+ 'crab_fjr_'+str(no)+'.xml'
      if not os.path.isfile(fjrPath):
        print "warning fjrPath ",fjrPath," doesnot exist "
    return fjrPath
  def gridFJRgoodJobs(self,debug=False):
    return [ getJobFJR(no) for no in self.jobRetrievedGood() ] 
  def gridOutputfileList(self,debug=False):
    outputFileList = []
    crab_fjr_list = self.gridFJRgoodJobs(debug=debug)
    if debug:
      print crab_fjr_list
    for fjr in crab_fjr_list:
      jobRep = tools.frameworkJobReportParser(fjr) 
      outputFileList.append(jobRep.getFileLFN())
    return outputFileList
  def writeOutputFileList(self,prefix=""):
    if not self.crabDir:
      print "no crabDir set"
      return None
    outFileList = open(self.crabDir+'/'+self.postfix+'_gridOutputFiles.txt','w')
    for f in self.gridOutputfileList():
      outFileList.write(prefix+f+'\n')
    outFileList.close()
    return outFileList.name
  def createMergeCfg(self,where=os.getenv('PWD'),debug=False,cmsswOpts="",useXRootD=False):
    #if not hasattr(self,"mergeGirdJobDict"):
    #  self.mergeGirdJobDict = {}
    if hasattr(self,'isMerged') and self.isMerged == True:
      return 0
    if debug:
      print "before getting good jobs"
    fjrs = self.gridFJRgoodJobs(debug=debug);outputSize=0
    if debug:
      print "getting good done"
    if not hasattr(self,'mergeSizeNeglect') or not self.mergeSizeNeglect:
      for fjr in fjrs:
        fjr = tools.frameworkJobReportParser(fjr)
        outputSize += int(fjr.getFileSize())
      print "estimated outputSize ",outputSize
      if outputSize > 10000000000:
        print "too big"
        sys.exit(1)
    inputFileList = None
    if useXRootD:
      xrdHelper = alternativeLocation.xRootDPathCreator()
      xrdHelper.findXRootDPrefix()
      inputFileList = self.writeOutputFileList(prefix=xrdHelper.xRootDPrefix)
    else:
      inputFileList = self.writeOutputFileList()
    import re
    if not hasattr(self,"mergeId"):
      self.mergeId = tools.coreTools.idGenerator()
    baseOutputDir=where+'/'+self.postfix+'_'+self.timeSt+'_'+self.mergeId+'/'
    if not os.path.exists(baseOutputDir):
      os.makedirs(baseOutputDir)
    #self.mergeGirdJobDict["mergeOutputDir"]=baseOutputDir
    #self.mergeGirdJobDict["postfix"]=self.postfix
    outputFilename=baseOutputDir+re.match('.*\/([^\/]*_)[0-9][0-9]*_[0-9][0-9]*_[a-zA-Z0-9][a-zA-Z0-9][a-zA-Z0-9]\.root',tools.frameworkJobReportParser(fjrs[0]).getFileLFN()).group(1)+'merged'
    # create merge cfg
    mergeTempCmd = os.getenv('CMSSW_BASE')+'/src/PhysicsTools/Utilities/configuration/copyPickMerge_cfg.py inputFiles_load='+inputFileList+' outputFile='+outputFilename+" "+cmsswOpts
    if debug:
      print "mergeTempCmd ",mergeTempCmd
    with open(baseOutputDir+'settings.txt','w') as settingFile:
      settingFile.write(mergeTempCmd)
    self.mergeCfg = baseOutputDir+'/copyPickMerge_cfg.py'
    if debug:
      print "before compile"
    createCfgCmd = 'ipython -c "exec(\\\"import IPython.ipapi\\nip = IPython.ipapi.get()\\nip.magic(\'run '+mergeTempCmd+'\')\\nf=open(\''+self.mergeCfg+'\',\'w\')\\nf.write(process.dumpPython())\\nf.close()\\\")"'
    if debug:
      print "createCfgCmd ",createCfgCmd
    createCfgJob = tools.coreTools.executeCommandSameEnv(createCfgCmd)
    createCfgJob.wait()
    if debug:
      print "after compile"

    if createCfgJob.returncode == 0:
      self.mergeCfgCreated = True
      self.outputFilename = outputFilename
      print "mergeCfg ",self.mergeCfg
    else:
      print "mergeCfg creation failed"
    return createCfgJob.returncode
   
  def doMerging(self,parallel=False,debug=False,where=os.getenv('PWD'),cmsswOpts="",dontExec=False,useXRootD=False):
    if hasattr(self,'isMerged') and self.isMerged:
      print self.postfix
      print "no merging needed, self.isMerged=True "
      return 0
    else:
      self.isMerged = False
    #self.reportLumi() 
    if not hasattr(self,'mergeGirdJobDict'):
      self.mergeGirdJobDict={}
      self.mergeGirdJobDict["postfix"]=self.postfix
    if debug:
      print "creating cfg"
    createMergeCfg = self.createMergeCfg(where=where,debug=debug,cmsswOpts=cmsswOpts,useXRootD=useXRootD)
    self.mergeGirdJobDict["mergeCfg"]=self.mergeCfg
    self.mergeGirdJobDict["postfix"]=self.postfix
    self.mergeGirdJobDict["mergeOutputDir"]=os.path.dirname(self.mergeGirdJobDict["mergeCfg"])
    if debug: 
      print "creating cfg done"
    if not createMergeCfg == 0:
      return None
    import json
    tmpReturnVal = None
    self.mergeCrabLogJson = os.path.dirname(self.mergeCfg)+'/mergeJson_'+self.postfix+'_'+self.timeSt+'_JSON.txt'
    tmpDict = dict(self.mergeGirdJobDict.items() + {'crabJobJSON': (self.loadedFromCrabJson if hasattr(self,'loadedFromCrabJson') else (self.crabJsonFile if hasattr(self,'crabJsonFile') else 'not existing') ) }.items() )
    if hasattr(self,'loadedFromCrabJson') or hasattr(self,'crabJsonFile'):
      from shutil import copy2       
      if hasattr(self,'loadedFromCrabJson') and hasattr(self,'crabJsonFile'):
        print "both are given: loadedFromCrabJson ",self.loadedFromCrabJson," and crabJsonFile ",self.crabJsonFile
        copy2( self.crabJsonFile,os.path.dirname(self.mergeCfg))
      copy2( self.loadedFromCrabJson if hasattr(self,'loadedFromCrabJson') else self.crabJsonFile , os.path.dirname(self.mergeCfg) ) 
    print "logging in ",self.mergeCrabLogJson
    if not parallel:
      mergeCmd='cmsRun '+self.mergeCfg+">& "+self.mergeCfg.strip()+"_log.txt "
      if debug:
        print "mergeCmd ",mergeCmd
      if dontExec:
        print mergeCmd
        tmpReturnVal = mergeCmd
      mergeJob = tools.coreTools.executeCommandSameEnv(mergeCmd)
      mergeJob.wait()
      if mergeJob.returncode == 0:
        self.isMerged=True
        outputFilename = self.outputFilename+".root"
        self.mergedFilename = outputFilename
        tmpReturnVal =  outputFilename
      else:
        print "merging Failed"
        print mergeCmd
        tmpReturnVal =  1
    else:
      noJobs=self.mergeNoJobs if hasattr(self,'mergeNoJobs') else 11
      noParallel=self.mergeNoParallel if hasattr(self,'mergeNoParallel') else 3
      import CMSSWParallel.cmsswParallel as cmsParallel
      if dontExec:
        cmd = "cd "+os.path.dirname(self.mergeCfg)+" && "+'$CMSSW_BASE'+'/ParallelizationTools/CMSSWParallel/cmsswParallel.py --numProcesses '+str(noParallel)+" --numJobs "+str(noJobs)+" --cfgFileName "+self.mergeCfg
        print cmd
        tmpReturnVal= cmd
      else:
        pR = cmsParallel.parallelRunner(self.mergeCfg,noParallel,noJobs,'',debug)
        self.mergeCmds = pR.createCfgs()
        t= pR.runParallel()
        if t.strip() == "0":
          self.isMerged=True
          tmpDict['intLumi']=self.reportLumi()
        tmpReturnVal =  t
        tmpDict['parallelMerge']=pR.jsonLogFileName
    with open (self.mergeCrabLogJson,'w') as jsonMergeLog:
      json.dump(tmpDict,jsonMergeLog,indent=2)
    return tmpReturnVal
  
def getCrabJobDatasetname(cJ,debug=False):
  gridFileList = cJ.gridOutputfileList()
  if not isinstance(gridFileList,list) or len(gridFileList) == 0:
    print "gridFileList not valid ",gridFileList
    return None
  if debug:
    print "gridFileList ",gridFileList
  import MyDASTools.dasTools as dasTools
  dasC = dasTools.myDasClient()
  return dasC.getDataSetNameForFile(gridFileList[0],'instance=prod/phys03')
#############################################################
def commandAcGridFolder(command,gridFolder):
    import subprocess,os,sys
    command = 'uberftp grid-ftp " '+command+" "+gridFolder+'" ; echo "DONE"'
    subPrOutput = subprocess.Popen([command],bufsize=1 , stdin=open(os.devnull),shell=True,stdout=subprocess.PIPE,env=os.environ)
    for line in iter(subPrOutput.stdout.readline,"DONE\n"):
      print line
    subPrOutput.stdout.close()
def removeGridFolderCrab(cJ):
  commandAcGridFolder("rm ",cJ.getAcGridDir().rstrip("/")+"/*")
  commandAcGridFolder("rmdir ",cJ.getAcGridDir().rstrip("/"))
def saveCrabProp(crabP,alternativeJSONname=None):
    jsonFilename = crabP.crabJsonFile if not alternativeJSONname else alternativeJSONname
    print "saving crab configuration: ",jsonFilename
    import json
    #self.jsonFilename = jsonFilename
    with open (jsonFilename,'wb') as f:
      json.dump(crabP.__dict__,f,indent=2)
def loadCrabJob(jsonFilename):
    import json
    def objD(obj):
      if '__type__' in obj and obj['__type__'] == 'crabProcess':
        return crabProcess(obj['postfix'],obj['cfg'],obj['samp'],obj['workdir'],obj['timeSt'],obj['addGridDir'])
      return obj
    cP = None
    with open(jsonFilename , 'rb') as jsonFile:
      cP = json.load(jsonFile,object_hook=objD)
    with open(jsonFilename , 'rb') as jsonFile:
      cP.__dict__ = json.load(jsonFile)
    cP.loadedFromCrabJson = jsonFilename
    return cP
def getSubmitServer(dbFile,debug=False):
  print "dbFile ",dbFile
  import sqlite3
  conn = sqlite3.connect(dbFile)
  conn.row_factory = sqlite3.Row
  cur = conn.cursor()
  #if debug:
  #  tabs = cur.execute("SELECT * FROM sqlite_master WHERE type='table';").fetchall()
  #  for tab in tabs:
  #    print tab["name"]
  #    print tab
  rows = cur.execute("SELECT server_name FROM bl_task")
  #if rows.arraysize > 1:
  #  print "error more jobs than expected ",len(rows)
  #if debug:
  #  print "rows ",rows
  srv = rows.next()
  print "old server ",srv["server_name"]
  return srv["server_name"]


def updateSubmitServer(newServer,dbFile,debug=False):
  import sqlite3 
  conn = sqlite3.connect(dbFile)
  conn.row_factory = sqlite3.Row
  cur = conn.cursor()
  print "debug ",debug
  if debug:
    tabs = cur.execute("SELECT * FROM sqlite_master WHERE type='table';").fetchall()
    print tabs
    for tab in tabs:
      print tab["name"]
  rows = cur.execute("SELECT server_name FROM bl_task") 
  if rows.arraysiye > 1:
    print "error more jobs than expected ",len(rows)
  print "old server ",rows.next()["server_name"]
  cur.execute("UPDATE bl_task SET server_name='"+newServer+"'")
  conn.commit()
  print "updated to ",newServer
########
def getListOfCrabCfgsInDir (where='.'):
  import glob
  return glob.glob(where+'/*_CrabCfg.json')
def getListOfCrabCfgs(where='.'):
  import fnmatch
  import os
  matches = []
  for root, dirnames, filenames in os.walk(os.getenv('PWD') if where == '.' else where):
    for filename in fnmatch.filter(filenames, '*CrabCfg.json'):
      matches.append(os.path.join(root, filename))
  return matches
def listCrabJobs(detailedInfo=False, timePoint = None,sorted=False):
  import os,re
  matches = getListOfCrabCfgs()
  if detailedInfo:
    print matches
    print "-----------"
  import datetime,time
  if timePoint:
    if not isinstance(timePoint,datetime.datetime):
      timePoint =datetime.datetime.now()-datetime.timedelta(days=timePoint)
  jobs= [m for m in matches if timePoint and int(loadCrabJob(m).timeSt.replace("_","").replace("-","")) > int(timePoint.strftime('%Y%m%d%H%M%S')) ]
  if sorted:
    jobs.sort(key=lambda c : re.match('.*([0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}-[0-9]{2}-[0-9]{2}).*',os.path.basename(c)).group(1) )
  if detailedInfo:
    print jobs
    print "jobs: ",len(jobs)
  return jobs 
def commandAllJobs(cmd,timePoint = None,debug=False,listJ=None):
  matches = listJ if listJ else listCrabJobs(False,timePoint)
  for m in matches:
    cJ = loadCrabJob(m)
    cJ.executeCrabCommand(cmd,debug=debug)
def automaticResubmitAll(timePoint = None,debug=False,listJ=None):
  matches = listJ if listJ else listCrabJobs(False,timePoint)
  for m in matches:
    cJ = loadCrabJob(m)
    cJ.automaticResubmit(debug=debug)
def overview(detailedInfo=False, timePoint = None):
  matches = listCrabJobs(detailedInfo,timePoint)
  notFinished=[]; finished=[]
  for m in matches:
    cJ = loadCrabJob(m)
    status_cJ = cJ.getStatusList()
    noJobs = len(status_cJ) 
    noGoodJob = cJ.jobRetrievedGood()
    if detailedInfo:
      print "".join(cJ.getStatusList()  )
    print m
    print "good ",len(noGoodJob), " of ",noJobs
    if len(noGoodJob) == noJobs:
      finished.append(m)
    else:
      notFinished.append(m)
  return {'notFinished':notFinished,"finished":finished}
#####################
class crabNanny(object):
  def __init__(self,cJs,mergeOutput):
    self.cJs = cJs
    self.mergeOutput=mergeOutput
    self.crabJobTMPstdout=None
    self.mergeNoParallel = 4;self.mergeSizeNeglect = True
    self.useXRootD = False
    self.debug=False
    if  self.mergeOutput and not os.path.exists(self.mergeOutput):
      os.makedirs( self.mergeOutput) 
  def startNursing(self,waitingTime=600):
    self.retrievedJobs = []
    self.allMerged = []
    dontStop = True
    jobList = self.cJs
    
    while dontStop:
      for i,c in enumerate(jobList):
        if not hasattr(c,'stdoutTMPfile'):
          if not self.crabJobTMPstdout:
            c.setTMPstdoutFile(c.crabJsonFile+'_TMDstdout')
          else:
            c.setTMPstdoutFile(self.crabJobTMPstdout+('/' if not self.crabJobTMPstdout.endswith('/') else '' )+os.path.basename(t.crabJsonFile))
        if not hasattr(c,'allRetrieved') or not c.allRetrieved: 
          print "autoResubmitting ",c.postfix
          c.automaticResubmit(onlySummary=True) 
        else:        
          if hasattr(c,'isMerged') and c.isMerged:
            if self.debug:
              print c.postfix," is merged "
            if hasattr(c,'nursingDone') and c.nursingDone:
              continue
            self.allMerged.append(c)
            jsonCrab = c.crabJsonFile
            mergeDonePostfix = "mergeDONE"
            import glob
            mergedMatches = glob.glob(os.path.splitext(jsonCrab)[0]+"_"+mergeDonePostfix+"*")
            if len(mergedMatches ) > 0 :
              print "older Versions already merged ",mergedMatches
            doneJsonCrab = tools.coreTools.addPostFixToFilename(jsonCrab, mergeDonePostfix+"_"+c.mergeId)
            saveCrabProp(c,alternativeJSONname=doneJsonCrab)
            c.nursingDone = True 
          else:
            if not os.path.exists(self.mergeOutput):
              print "merging not possible, target location does not exist"
            else:
              print "merging ",c.postfix
              c.mergeNoParallel = self.mergeNoParallel; c.mergeSizeNeglect = self.mergeSizeNeglect; 
              c.doMerging(debug=True,where= self.mergeOutput,parallel=True,useXRootD=self.useXRootD)
      if len(self.cJs) == len (self.allMerged):
        dontStop = False
      else:
        print "waiting for ",waitingTime," seconds"
        import time
        time.sleep(waitingTime)
    print " allDone "
