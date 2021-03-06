import os,sys,copy
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools/MyDASTools/DASclient/python')
import das_client
##
def myDasClient(debug=False): 
  dC = theDasClient(debug)
  dC.limit=0
  return dC
class theDasClient:
  def __init__(self,debug=False):
    oldArgv = None
    if len(sys.argv) > 1:
      oldArgv = copy.deepcopy(sys.argv[1:])
      del sys.argv[1:]
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
    if oldArgv:
      sys.argv.extend(oldArgv)
  def getDataSetNameForFile(self,filename, addQuery = 'instance=prod/global'):
    if self.debug:
      print "searching dataset for file ",filename
    jsondict = self.myQuery("dataset file = "+filename+" "+addQuery)
    if self.debug:
      print jsondict
    return [ str(ele.get('dataset')[0].get('name')) for ele in jsondict.get('data')]
  def getFilesForDataset(self,dataset,addQuery = 'instance=prod/global'):
    jsondict = self.myQuery("file dataset="+dataset+" "+addQuery)
    return [ str(ele.get('file')[0].get('name')) for ele in jsondict.get('data')]
  def getSitesForDataset(self,dataset, addQuery = 'instance=prod/global'):
    jsondict = self.myQuery("site dataset="+dataset+" "+addQuery)
    return [ str(ele.get('site')[0].get('name')) for ele in jsondict.get('data')]
  def getSizeForDataset(self,dataset, addQuery = 'instance=prod/global'):
    jsondict = self.myQuery("dataset dataset="+dataset+" "+addQuery+" | grep dataset.size")
    return " ".join([str(i["size"]) for i in jsondict['data'][0]['dataset'] if i.has_key('size')])
  def getNEventsForDataset(self,dataset, addQuery = 'instance=prod/global'):
    jsondict = self.myQuery("dataset dataset="+dataset+" "+addQuery+" | grep dataset.nevents")
    return " ".join([str(i["nevents"]) for i in jsondict['data'][0]['dataset'] if i.has_key('nevents')]) 
  def getRunsFromDatasetname(self,datasetName, addQuery = 'instance=prod/global'):
    runsDAS = self.myQuery("run dataset = "+datasetName+" "+addQuery)
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

