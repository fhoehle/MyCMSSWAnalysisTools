#!/usr/bin/env python
import os,sys,re,argparse
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools/')
import CrabTools
files = [arg for arg in sys.argv[1:] if re.match('^[^-].*\.json$',arg) and os.path.isfile(arg)]
for file in files:
  sys.argv.remove(file)
opts =  sys.argv[1:] 
print "loading ",files
if len(opts) > 2:
  sys.exit("too many options, only one supported")
for file in files:
  testCrab =  CrabTools.loadCrabJob(file)
  if "-automaticResubmit" in opts:
    testCrab.automaticResubmit(False if not "-onlySummary" in opts else True)
  else:
    testCrab.executeCrabCommand(" ".join(opts),debug=True)

