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
    #,"se_black_list" : "dcache-se-cms.desy.de"
    }
}
###
def checkGridCert():
  import subprocess,os
  subPrOutput = subprocess.Popen(["voms-proxy-info -exists"],shell=True,stdout=subprocess.PIPE,env=os.environ)
  subPrOutput.wait()
  return subPrOutput.returncode == 0
###
class crabProcess():
  def __init__(self,postfix,cfg,samp,workdir,timeSt,addGridDir=""):
    self.postfix = postfix
    self.cfg = cfg
    self.samp = samp
    self.workdir = workdir
    self.timeSt = timeSt 
    self.addGridDir = addGridDir
    if not checkGridCert():
      import sys
      sys.exit("grid cert not okay, test coms-proxy-init failed")
  def writeCrabCfg(self):
    import os
    cCfg = open(self.crabDir+os.path.sep+"crab.cfg" ,'w')
    for sec,vals in self.crabCfg.iteritems():
      cCfg.write("["+sec+"]");cCfg.write("\n")
      for k,val in vals.iteritems():
        cCfg.write(k+"="+str(val));cCfg.write("\n")
    cCfg.close()
    self.crabCfgFilename = cCfg.name
    return cCfg.name 
  ###
  def createCrabDir(self):
    import os
    crabDir = os.path.realpath(self.workdir)+os.path.sep+self.postfix+"_"+self.timeSt
    os.makedirs(crabDir)
    self.crabDir = crabDir
    return crabDir  
  ##
  def createCrabCfg(self):
    tmpCrabCfg = crabCfg
    tmpCrabCfg["CMSSW"]["pset"]=self.cfg.newCfgName
    tmpCrabCfg["USER"]["user_remote_dir"] = self.addGridDir +( "/" if self.addGridDir != "" and self.addGridDir != None else "") + self.postfix+"_"+self.timeSt
    self.cfg.setOutputFilesGrid()
    tmpCrabCfg["CMSSW"]["output_file"] = ",".join(self.cfg.getListOfOutputFiles())
    tmpCrabCfg["CMSSW"]["datasetpath"]=self.samp.dataset
    self.crabCfg =tmpCrabCfg
    return tmpCrabCfg
  def executeCrabCommand(self,command):
    import subprocess,os
    subPrOutput = subprocess.Popen(["cd "+self.crabDir+" && crab "+command],shell=True,stdout=subprocess.PIPE,env=os.environ)
    subPrOutput.wait()
    errorcode = subPrOutput.returncode
    print "ERRORCODE ",errorcode
