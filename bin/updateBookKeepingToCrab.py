#!/usr/bin/env python
import sys,os,json,argparse
sys.path.extend([os.getenv('CMSSW_BASE')+fld for fld in ['/MyCMSSWAnalysisTools/','/MyCMSSWAnalysisTools/bin/','/MyCMSSWAnalysisTools/Tools/','/src/GridTools/GridStuff_FileManagement/']])
import CrabTools
import myHadd 
import processedEvents
import printGridUrlfromCrabReport
import tools
import coreTools
parser = argparse.ArgumentParser()
parser.add_argument('--bookKeeping',help='bookKeeping file which should be updated to craboutput',required=True)
parser.add_argument('--debug',action="store_true",default=False,help='debug mode')
parser.add_argument('--targetPostfix',default='',help=' targetname_POSTFIX.end')
args = parser.parse_args()
dataFile = open (args.bookKeeping)
data = json.load(dataFile)
key = data.keys()[0]
dataset = data[key]
cJ = CrabTools.loadCrabJob(dataset['crabJob'])
crabDirs=os.walk(cJ.crabDir).next()[1]
crabDirs.sort()
currentCrabJobDir=os.path.realpath(cJ.crabDir)+os.path.sep+crabDirs[-1]
print 'get status of crab Job',
jobs = cJ.jobRetrievedGood()
print ' done. ',len(jobs),' will be used'
argumentsFile = currentCrabJobDir+os.path.sep+'share/arguments.xml'
maxEvents = processedEvents.getMaxEvents(argumentsFile,jobs)
dataset['crab']=True
dataset['totalEvents']=sum([ int(e) for e in maxEvents.values()])
print "create list of Files",
analysisFiles = printGridUrlfromCrabReport.getFileNames(True,False,False,[currentCrabJobDir+os.path.sep+'res/crab_fjr_'+j+'.xml' for j in jobs],args.debug,True)
print "done"
if args.debug:
  print analysisFiles
if len(analysisFiles.values()) > 0:
  target = myHadd.removeCrabJobPostfix(analysisFiles.values()[0],"_bookKeeping"+(args.targetPostfix if not "label" else dataset['sample']["label"]))
  if os.path.isfile(target):
    print "target ",target," already exists "
  else:
    print "hadding ",target
    sys.stdout.flush()
    myHadd.mergedHadd(target,analysisFiles.values(), debug = args.debug)
    print "done" 
  dataset["outputFilesCrab"]=[target]
bookKeepingFileName = coreTools.addPostFixToFilename(args.bookKeeping,'_bookKeepingUpdated_'+coreTools.getTimeStamp())
updatedbookKeeping = open(bookKeepingFileName ,'wb')
json.dump(data,updatedbookKeeping);updatedbookKeeping.close()
print "new bookKeeping created ",bookKeepingFileName

