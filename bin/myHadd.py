#!/usr/bin/env python
import sys,copy,subprocess,os,argparse
###############
def executeCommandSameEnv(command):
  import os,subprocess
  return subprocess.Popen([command],bufsize=1 , stdin=open(os.devnull),shell=True,stdout=subprocess.PIPE,env=os.environ)
def removeCrabJobPostfix(name,postfix = ""):
  import re
  reName = re.match('.*\/([^\/]+)_[0-9]+_[0-9]+_[a-zA-Z0-9]{3}(\.[0-9a-zA-Z]+)',name)
  return reName.group(1)+("_"+postfix if not postfix == "" else "" )+reName.group(2) if len(reName.groups()) == 2 else None
##############
def mergedHadd(target,inputFiles,chunkSize=10,debug = False):
  noFiles=len(inputFiles)
  mergeSteps=noFiles/chunkSize+1
  if target == "":
    target = removeCrabJobPostfix(inputFiles[0])
    if debug:
      print "generated target from inputFiles ",target
  if target == None or target == "":
    sys.exit("invalid target")
  if debug:
    print "mergeSteps ",mergeSteps
  targetTmpOld="";targetTmp="";targetTmpPattern=target+"_tmp"
  for i in range( mergeSteps):
    if debug:
      print "merging ",i
    targetTmpOld=targetTmp
    targetTmp=targetTmpPattern+str(i)
    haddArgs=""
    inputFileList = inputFiles[ chunkSize*i : (chunkSize*(i+1) if len(inputFiles) > chunkSize*(i+1) else len(inputFiles) )]
    if debug:
      print "targetTmpOld ", targetTmpOld , "  targetTmp ",targetTmp 
    if i == mergeSteps-1 :
      haddArgs=target+" "+" ".join(inputFileList)+" "+targetTmpOld
    else:
      haddArgs=targetTmp+" "+" ".join(inputFileList)+" "+targetTmpOld
    if debug:
     print haddArgs
    sP = executeCommandSameEnv("hadd "+haddArgs)
    sP.wait()
    if sP.returncode != 0:
      print sP.communicate()[0]
      sys.exit(sP.returncode)
    sys.stdout.flush() 
  ## tidy up
  if mergeSteps > 1:
    sP = executeCommandSameEnv("rm "+targetTmpPattern+"*")
    sP.wait()
    if debug:
      print sP.communicate()[0]
    return sP.returncode
  return 0
################
def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--debug',action='store_true',default=False,help='debug mode')
  parser.add_argument('--target',type=str,help='target in which inputFiles are merged',default="")
  parser.add_argument('inputFiles',nargs='+',help='inputpufile1.root inputfile2.root OR fileList.txt')
  args = parser.parse_args()
  inputFiles=copy.deepcopy(args.inputFiles)
  if len(inputFiles) == 1 and not inputFiles[0].endswith('.root'):
    print "using fileList"
    inputFiles = [ l.strip() for l in open(inputFiles[0]) if not l.strip().startswith('#') ]
  if len(inputFiles) == 0:
    sys.exit('inputFile list has length 0 ') 
  if args.debug:
    print "target ",args.target
    print "inputFiles ",len(inputFiles)," : ",inputFiles
  mergedHadd(args.target,inputFiles,debug=args.debug)
  return 0
########
if __name__ == "__main__":
  sys.exit(main())
