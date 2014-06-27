#!/usr/bin/env python
import os,sys,re,argparse
import xml.dom.minidom as minidom
# usage for file in $(ls crab_0_121012_230338/res/crab_fjr_*.xml); do ../FileManagment/trunk/printGridUrlfromCrabReport.py --xmlFile $file --useAnalysisFile | grep srm; done >& Ntuple_files.txt
sys.path.append(os.getenv('CMSSW_BASE')+os.path.sep+'MyCMSSWAnalysisTools')
from  CrabTools import  myGetSubNodeByName
def getJobNode (args,jobNo):
  for cld in args.childNodes:
    if cld.nodeName =="Job":
      if cld.getAttribute("JobID") == jobNo:
        return cld
def getMaxEvents(xmlF,jobNos,debug = False):
  maxEvents = {}
  dom = minidom.parse(xmlF)
  arguments = myGetSubNodeByName(dom,"arguments")
  if len(arguments) != 1:
    print "mulitple arguments's found"
    return None
  arguments=arguments[0]
  for jobNo in jobNos:
    job=getJobNode(arguments,jobNo)
    if job.hasAttribute("MaxEvents"):
      if debug:
        job.getAttribute("MaxEvents")
      if not job.getAttribute("MaxEvents") =="-1":
        maxEvents[job.getAttribute("JobID")] = job.getAttribute("MaxEvents")
      else:
        inputFiles=job.getAttribute('InputFiles').split(',')
        import subprocess
        dbsCommand='dbsql "find file.numevents where file='+' or file='.join(inputFiles)+'"'
        if debug:
          print dbsCommand
        evtsFiles = [ int(no) for no in subprocess.Popen([dbsCommand],bufsize=1 , stdin=open(os.devnull),shell=True,stdout=subprocess.PIPE,env=os.environ).communicate()[0].split('\n') if no[:1].isdigit()]
        if debug:
          print evtsFiles, ' ' , job.getAttribute("JobID")
        maxEvents[job.getAttribute("JobID")] = sum(evtsFiles) - int(job.getAttribute("SkipEvents"))
      if debug:
        print "maxEvents ",maxEvents[job.getAttribute("JobID")]
  return maxEvents
def main(): 
  parser = argparse.ArgumentParser()
  parser.add_argument('--xmlFile',default='',help=' input xmlFile')     
  parser.add_argument('--jobNum',nargs='+',default=[],help=' jobNumbers')
  args=parser.parse_args()
  if args.xmlFile == None or args.xmlFile =="" or (args.jobNum == []) :
    print "input wrong"
    sys.exit(1)
  maxEvents = getMaxEvents(args.xmlFile,args.jobNum) 
  print " ".join(maxEvents.values())
#####################
if __name__ == "__main__":
  main()
