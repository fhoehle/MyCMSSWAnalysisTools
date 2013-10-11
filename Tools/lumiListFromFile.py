from DataFormats.FWLite import Lumis
from FWCore.PythonUtilities.LumiList   import LumiList
def getLumiListFromFile(filename):
  runsLumisDict = {}
  lumis = Lumis (filename)
  for lum in lumis:
    runList = runsLumisDict.setdefault (lum.aux().run(), [])
    runList.append( lum.aux().id().luminosityBlock() )
  jsonList = LumiList (runsAndLumis = runsLumisDict)
  return jsonList
