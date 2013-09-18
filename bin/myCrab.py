#!/usr/bin/env python
import os,sys,re,argparse
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools/')
import CrabTools
parser = argparse.ArgumentParser()
parser.add_argument('jsonFiles',nargs='+',default=[],help=' input jsonFiles')
parser.add_argument('--onlySummary',action='store_true',default=False,help='only summary')
parser.add_argument('-automaticResubmit',action='store_true',default=False,help=' test Job and resubmit failed and get successful jobs')
args , remaining= parser.parse_known_args()
#files = [arg for arg in sys.argv[1:] if re.match('^[^-].*\.json$',arg) and os.path.isfile(arg)]
for file in args.jsonFiles:
  if not os.path.isfile(file):
    sys.exit('not valid file '+file)
for file in args.jsonFiles:
  testCrab =  CrabTools.loadCrabJob(file)
  if args.automaticResubmit:
    testCrab.automaticResubmit(args.onlySummary)
  else:
    testCrab.executeCrabCommand(" ".join(remaining),debug=True)

