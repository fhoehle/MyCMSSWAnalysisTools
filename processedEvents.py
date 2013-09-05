#!/usr/bin/env python
import getopt,sys,re,argparse
from xml.dom.minidom import *
# usage for file in $(ls crab_0_121012_230338/res/crab_fjr_*.xml); do ../FileManagment/trunk/printGridUrlfromCrabReport.py --xmlFile $file --useAnalysisFile | grep srm; done >& Ntuple_files.txt
def myGetSubNodeByName(node,name):
 if not node:
   return None
 if not hasattr(node,'childNodes'):
   return None
 for i,tmp_node in enumerate(node.childNodes):
  if tmp_node.nodeName == name:
   return tmp_node
 return None
def getJobNode (args,jobNo):
  for cld in args.childNodes:
    if cld.nodeName =="Job":
      if cld.getAttribute("JobID") == jobNo:
        return cld

parser = argparse.ArgumentParser()
parser.add_argument('--xmlFile',default='',help=' input xmlFile')     
parser.add_argument('--jobNum',nargs='+',default=[],help=' jobNumbers')
args=parser.parse_args()
#opts, args = getopt.getopt(sys.argv[1:], 'h',['xmlFile=','jobNum='])
#xmlFile=None
#jobNum = []
#for opt,arg in opts:
# #print opt , " :   " , arg
# if opt in  ("--xmlFile"):
#  xmlFile=arg
# if opt in ("--jobNum"):
#  jobNum = arg.split(',')
# if opt in ("-h"):
#  print "python processedEvents.py --xmlFile arguments.xml --jobNum 1,2,3 "
#  sys.exit(0)
dom = parse(args.xmlFile)
if args.xmlFile == None or args.xmlFile =="" or (args.jobNum == []) :
 print "input wrong"
 sys.exit(1)
arguments = myGetSubNodeByName(dom,"arguments")
jobs = []
for jobNo in args.jobNum:
  job=getJobNode(arguments,jobNo)
  if job.hasAttribute("MaxEvents"):
    print job.getAttribute("MaxEvents")
  jobs.append(job)
# ExitCode
#exitCode = myGetSubNodeByName(fwkRep,"ExitCode")
#fwkExitCode =  myGetSubNodeByName(fwkRep,"FrameworkError")
#fwkExitCode = fwkExitCode.getAttribute("ExitStatus")
#if str(fwkExitCode) != "0":
#  sys.exit("not all finished/done "+xmlFile+" "+str(fwkExitCode))
#print "ExitCode ",exitCode.getAttribute("Value")
## output files
#if usePoolOutputFile:
# poolOutputFile = myGetSubNodeByName(fwkRep,"File")
# poolOutputFileGridUrl = myGetSubNodeByName(poolOutputFile,"SurlForGrid")
# if poolOutputFileGridUrl:
#   mystring=str(poolOutputFileGridUrl.firstChild.nodeValue).strip()
#   if mystring:
#     print mystring
#   else:
#     print "error ",xmlFile
# else:
#   print "error ",xmlFile
#if useAnalysisFile:
# analysisFile = myGetSubNodeByName(fwkRep,"AnalysisFile")
# analysisFileGridUrl = myGetSubNodeByName(analysisFile,"SurlForGrid")
# if analysisFileGridUrl:
#   fileLoc = analysisFileGridUrl.getAttribute("Value") 
#   test= re.sub('srm://grid-srm.physik.rwth-aachen.de:8443/srm/managerv2\?SFN=','dcap://grid-dcap.physik.rwth-aachen.de',fileLoc) #dcap:\/\/grid-dcap\.physik\.rwth-aachen\.de',fileLoc)
#   if test:
#     print  test
#   else:
#     print "error ",xmlFile
# else:
#  print "error ",xmlFile
