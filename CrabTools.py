### default dict
import sys,os
sys.path.extend([ os.getenv('CMSSW_BASE')+os.path.sep+p for p in ['MyCrabTools','MyCMSSWAnalysisTools']])
import crabDeamonTools
crabCfg = {
  "CRAB" :{
    "jobtype":"cmssw"
    ,"scheduler" : "remoteGlidein" #sge
    #,"server_name" : "legnaro"
    #,"submit_host": "cern_vocms20"
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
class crabProcess(crabDeamonTools.crabDeamon):
  def __init__(self,postfix,cfg,samp,workdir,timeSt,addGridDir=""):
    self.crabJobDir = None
    self.postfix = postfix
    self.cfg = cfg
    self.samp = samp
    self.workdir = workdir
    self.timeSt = timeSt 
    self.addGridDir = addGridDir
    #print "self.addGridDir ",self.addGridDir," self.postfix ",self.postfix," self.timeSt ",self.timeSt 
    self.user_remote_dir = self.addGridDir +( "/" if self.addGridDir != "" and self.addGridDir != None else "") + self.postfix+"_"+self.timeSt
    self.__type__="crabProcess"
    self.crabJobDir = None
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
    if self.crabCfg.has_key('CMSSW') and self.crabCfg['CMSSW'].has_key('lumi_mask') and self.crabCfg['CMSSW']['lumi_mask']:
      from shutil import copy2
      print "copying lumi_mask: ", self.crabCfg['CMSSW']['lumi_mask']," to ",os.path.dirname(cCfg.name) + os.path.sep
      copy2(self.crabCfg['CMSSW']['lumi_mask'],os.path.dirname(cCfg.name) + os.path.sep)
    return cCfg.name 
  ###
  def setCrabDir(self,addCrabDir ="",timeSt = "",workdir = ""):
    if workdir == "":
      workdir =  self.workdir
    import os
    crabDir = os.path.realpath(workdir)+((os.path.sep+addCrabDir) if addCrabDir != "" else "")+(("_"+timeSt) if timeSt != "" else "")
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
    tmpCrabCfg = crabCfg
    tmpCrabCfg["CMSSW"]["pset"]=self.cfg
    tmpCrabCfg["USER"]["user_remote_dir"] = self.user_remote_dir
    tmpCrabCfg["CMSSW"]["get_edm_output"] = 1
    tmpCrabCfg["CMSSW"].pop("output_file",None)
    tmpCrabCfg["CMSSW"]["datasetpath"]=self.samp
    if changes != None:
      self.applyChanges(tmpCrabCfg,changes)  
    self.crabCfg =tmpCrabCfg
    return self.crabCfg
  def create(self):
    self.executeCrabCommand("-create",debug = True)
    self.findCrabJobDir(self.crabDir)
  def reportLumi(self):
    import re
    import Tools.lumiTools as lumiTools
    self.executeCrabCommand("-report",debug = True) 
    lumiTools.calcLumi(self.crabJobDir+"/res/lumiSummary.json")

  def changeCrabJobDir(self,newDir):
    self.crabJobDir = newDir
  def executeCrabCommand(self,command,debug = False,returnOutput = False):
    if not hasattr(self,'crabDir'):
      self.createCrabDir()
    if not hasattr(self,'crabJobDir') or not self.crabJobDir or self.crabJobDir == '': 
      return  self.executeCommand(command,debug ,returnOutput,where=self.crabDir)
    else:
      return  self.executeCommand(command,debug ,returnOutput)
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
def updateSubmitServer(newServer,dbFile,debug=False):
  import sqlite3 
  conn = sqlite3.connect(dbFile)
  conn.row_factory = sqlite3.Row
  cur = conn.cursor()
  if debug:
    tabs = cur.execute("SELECT * FROM sqlite_master WHERE type='table';").fetchall()
    for tab in tabs:
      print tab["name"]
  rows = cur.execute("SELECT server_name FROM bl_task").fetchall()
  if len(rows) > 1:
    print "error more jobs than expected ",len(rows)
  print "old server ",rows[0]["server_name"]
  cur.execute("UPDATE bl_task SET server_name='"+newServer+"'")
  conn.commit()
  print "updated to ",newServer
########
def listCrabJobs(detailedInfo=False, timePoint = None,sorted=False):
  import fnmatch
  import os,re
  matches = []
  for root, dirnames, filenames in os.walk(os.getenv('PWD')):
    for filename in fnmatch.filter(filenames, '*CrabCfg.json'):
      matches.append(os.path.join(root, filename)) 
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
