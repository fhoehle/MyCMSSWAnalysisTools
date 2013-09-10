### default dict
crabCfg = {
  "CRAB" :{
    "jobtype":"cmssw"
    ,"scheduler" : "remoteGlidein" #sge
    #,"server_name" : "legnaro"
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
    ,"number_of_jobs" : 500
    ,"total_number_of_events" : -1
    #,"total_number_of_lumis" : -1
    #,"lumis_per_job" :  1000
    #,"events_per_job":100
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
    ,"se_black_list" : "T2_TW_Taiwan"
    }
}
###
def checkGridCert():
  import subprocess,os
  subPrOutput = subprocess.Popen(["voms-proxy-info -exists"],shell=True,stdout=subprocess.PIPE,env=os.environ)
  subPrOutput.wait()
  return subPrOutput.returncode == 0
###
class crabProcess(object):
  def __init__(self,postfix,cfg,samp,workdir,timeSt,addGridDir=""):
    self.postfix = postfix
    self.cfg = cfg
    self.samp = samp
    self.workdir = workdir
    self.timeSt = timeSt 
    self.addGridDir = addGridDir
    print "self.addGridDir ",self.addGridDir," self.postfix ",self.postfix," self.timeSt ",self.timeSt 
    self.user_remote_dir = self.addGridDir +( "/" if self.addGridDir != "" and self.addGridDir != None else "") + self.postfix+"_"+self.timeSt
    self.__type__="crabProcess"
    self.checkRequirements()
  def applyChanges(self,willBeChanged,changesGivenHere):
    for k,i in changesGivenHere.iteritems():
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
    return cCfg.name 
  ###
  def setCrabDir(self,addCrabDir ="",timeSt = "",workdir = ""):
    if workdir == "":
      workdir =  self.workdir
    print workdir
    import os
    print "workdir ",workdir," addCrabDir ",addCrabDir," timeSt ",timeSt
    crabDir = os.path.realpath(workdir)+((os.path.sep+addCrabDir) if addCrabDir != "" else "")+(("_"+timeSt) if timeSt != "" else "")
    print "crabDir ",crabDir
    self.crabDir = crabDir + os.path.sep if not crabDir.endswith('/') else "" 
    return crabDir
  def createCrabDir(self):
    import os
    if not os.path.isdir(self.crabDir):
      os.makedirs(self.crabDir)
  ##
  def createCrabCfg(self,changes = None):
    tmpCrabCfg = crabCfg
    tmpCrabCfg["CMSSW"]["pset"]=self.cfg
    tmpCrabCfg["USER"]["user_remote_dir"] = self.user_remote_dir
    tmpCrabCfg["CMSSW"]["get_edm_output"] = 1
    tmpCrabCfg["CMSSW"].pop("output_file",None)
    tmpCrabCfg["CMSSW"]["datasetpath"]=self.samp
#    tmpCrabCfg["USER"]["ui_working_dir"]
    if changes != None:
      self.applyChanges(tmpCrabCfg,changes)  
    self.crabCfg =tmpCrabCfg
    return self.crabCfg
  def create(self):
    self.executeCrabCommand("-create",debug = True) 

  def executeCrabCommand(self,command,debug = False,returnOutput = False):
    if not hasattr(self,'crabDir'):
      self.createCrabDir()
    import subprocess,os,sys
    command = "cd "+self.crabDir+" && crab "+command+' ; echo "stopKeyDONE"'
    if debug:
      print "executing command ",command
    subPrOutput = subprocess.Popen([command],bufsize=1 , stdin=open(os.devnull),shell=True,stdout=subprocess.PIPE,env=os.environ)
    subPStdOut = []
    for line in iter(subPrOutput.stdout.readline,"stopKeyDONE\n"):
      if debug:
        print line
      subPStdOut.append(line)
    subPrOutput.stdout.close()
    if not returnOutput:
      print "ERRORCODE ",subPrOutput.returncode
    else:
      return subPStdOut
  def status(self):
    self.executeCrabCommand("-status",debug = True)
  def getoutput(self):
    self.executeCrabCommand("-getoutput",debug = True)
  def submit(self):
    output = self.executeCrabCommand("-status",False,True)
    import re
    numJobs = len([ l for l in output if re.match('^[0-9]+[ \t]+[YN][ \t]+[a-zA-Z]+[ \t]+',l)])
    if numJobs > 500:
      print "submitting ",numJobs," jobs"
      import time
      for i in range(numJobs/500+1):
        print "submitting block ",i
        self.executeCrabCommand("-submit "+str(i*500+1)+"-"+str((i+1)*500),True)
    else:
      self.executeCrabCommand("-submit ",True)
  def jobRetrievedGood(self,jobStatusS = None):
    import re
    jobs = []
    for  j in jobStatusS if jobStatusS else [ l for l in self.executeCrabCommand("-status",False,True) if re.match('^[0-9]+[ \t]+[YN][ \t]+[a-zA-Z]+[ \t]+',l)]:
      jSplit = j.split()
      if  len(jSplit) > 5 and jSplit[2] == "Retrieved" and jSplit[5] == "0":
        jobs.append(jSplit[0])
    return jobs
  def automaticResubmit(self,onlySummary = False):
    import re
    jobOutput = [ l for l in self.executeCrabCommand("-status",False,True) if re.match('^[0-9]+[ \t]+[YN][ \t]+[a-zA-Z]+[ \t]+',l)]
    doneJobsGood = self.jobRetrievedGood(jobOutput); doneJobsBad = []; abortedJobs = []; downloadableJobs = []; downloadedJobsBad = [];downloadableNoCodeJobs=[]
    for j in jobOutput:
      jSplit = j.split()
      if  len(jSplit) > 5:
        if jSplit[2] == "Retrieved" and jSplit[5] != "0":
          downloadedJobsBad.append(jSplit[0])
        if jSplit[2] == "Done" and jSplit[5] != "0":
          doneJobsBad.append(jSplit[0]) 
      if jSplit[2] == "Aborted":
        abortedJobs.append(jSplit[0])
      if len(jSplit) > 5 and jSplit[2]  == "Done" and jSplit[3]  == "Terminated" and jSplit[5] == "0": 
        downloadableJobs.append(jSplit[0])
      if  len(jSplit) < 5 and len(jSplit) > 2 and jSplit[2]  == "Done" and jSplit[3]  == "Terminated" :
        downloadableNoCodeJobs.append(jSplit[0])
    if not onlySummary:
      print " downloadableJobs ",downloadableJobs
      print "doneJobsGood ",doneJobsGood
      print "doneJobsBad ", doneJobsBad 
      print "abortedJobs ",abortedJobs
      print "downloadedJobsBad ",downloadedJobsBad
      print "downloadableNoCodeJobs", downloadableNoCodeJobs
    if len(doneJobsBad+downloadableJobs+downloadableNoCodeJobs):
      self.executeCrabCommand("-get "+",".join(doneJobsBad+downloadableJobs+downloadableNoCodeJobs),debug = True and not onlySummary )
    if len(doneJobsBad+downloadedJobsBad+abortedJobs) > 0:
      self.executeCrabCommand("-resubmit "+",".join(doneJobsBad+downloadedJobsBad+abortedJobs),debug = True and not onlySummary)
    if onlySummary:
      print "CrabJob ",self.postfix
      print "jobs done/retrieved good ",",".join(doneJobsGood+downloadableJobs)
      print len(doneJobsGood+downloadableJobs),"/",len(jobOutput)
      print "jobs resubmitted ",",".join(doneJobsBad+downloadedJobsBad+abortedJobs)
      print "just downloaded ",",".join(downloadableNoCodeJobs)
  def getAcGridDir(self):
    import os,subprocess
    if self.crabCfg["USER"].has_key("user_remote_dir"):
      return '/pnfs/physik.rwth-aachen.de/cms/store/user/fhohle/'+self.crabCfg["USER"]["user_remote_dir"]
    else:
      return None
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
def saveCrabProp(crabP,jsonFilename):
    print "saving crab configuration: ",jsonFilename
    import json
    #self.jsonFilename = jsonFilename
    with open (jsonFilename,'wb') as f:
      json.dump(crabP.__dict__,f)
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
    return cP
def myGetSubNodeByName(node,name): 
 if not node: 
   return None 
 if not hasattr(node,'childNodes'): 
   return None 
 for i,tmp_node in enumerate(node.childNodes): 
  if tmp_node.nodeName == name: 
   return tmp_node 
 return None

