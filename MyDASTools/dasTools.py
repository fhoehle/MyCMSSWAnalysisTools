import os,sys
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools/MyDASTools/DASclient/python')
import das_client
##
def myDasClient(debug=False): 
  dC = theDasClient(debug)
  dC.limit=0
  return dC
class theDasClient:
  def __init__(self,debug=False):
    self.optmgr = das_client.DASOptionParser()
    self.opts, _ = self.optmgr.get_opt()
    self.host = self.opts.host
    self.debug = self.opts.verbose
    self.query = self.opts.query
    self.idx = self.opts.idx
    self.limit = self.opts.limit
    self.thr = self.opts.threshold
    self.ckey = self.opts.ckey
    self.cert = self.opts.cert
    self.das_h = self.opts.das_headers
    self.base = self.opts.base
    self.debug=debug
  def getDataSetNameForFile(self,filename):
    if self.debug:
      print "searching dataset for file ",filename
    jsondict = self.myQuery("dataset file = "+filename)
    if self.debug:
      print jsondict
    return [ str(ele.get('dataset')[0].get('name')) for ele in jsondict.get('data')]
  def getRunsFromDatasetname(self,datasetName):
    runsDAS = self.myQuery("run dataset = "+datasetName)
    runs = []
    for el in runsDAS['data']:
      runList = el['run']
      if len(runList) != 1:
        print('fail',runList)
      else:
        runs.append(runList[0]['run_number'])
    return runs
  def myQuery(self,query):
    return das_client.get_data(self.host, query , self.idx, self.limit , self.debug, self.thr, self.ckey, self.cert)
  def getJsonOfDataset(self,datasetName,debug=False):
    runsAndLumisJson = self.myQuery('run lumi dataset = '+datasetName)['data']
    print "processing ",len(runsAndLumisJson) ," runs of ",datasetName
    compactList = {}
    if self.debug:
      print runsAndLumisJson
    for rJson in runsAndLumisJson:
      if self.debug:
        print rJson
      if len(rJson["run"]) != 1 or len(rJson['lumi']) != 1:
        print 'run query result wrong ',rJson
        continue
      compactList[rJson["run"][0]["run_number"]]=rJson["lumi"][0]["number"]
    from FWCore.PythonUtilities.LumiList import LumiList
    return LumiList(compactList=compactList)

