from FWCore.PythonUtilities.LumiList import LumiList

def shortenJson(jsonFile,minRun=0,maxRun=-1,output=None):
  runList = LumiList (filename = jsonFile)  # Read in first  JSON file
  allRuns = runList.getRuns()
  runsToRemove=[]
  for run in allRuns:
      if  int(run) < minRun:
          runsToRemove.append (run)
      if maxRun > 0 and int(run) > maxRun:
          runsToRemove.append (run)

  runList.removeRuns (runsToRemove)
  if output:
    runList.writeJSON (output)
  else:
    print  runList
