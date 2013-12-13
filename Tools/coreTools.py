def executeCommandSameEnv(command):
  import os,subprocess
  return subprocess.Popen([command],bufsize=1 , stdin=open(os.devnull),shell=True,stdout=subprocess.PIPE,env=os.environ)
def checkCommandAbortIfFail(p):
  import sys
  p.wait()
  if p.returncode != 0:
    print p.communicate()[0]
    sys.exit(p.returncode)
################
def getTimeStamp():
  import datetime,time
  return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S')
