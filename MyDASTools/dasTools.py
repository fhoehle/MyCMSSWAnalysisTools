import os,sys
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools/MyDASTools/DASclient/python')
import das_client
##
class myDasClient:
  def __init__(self):
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
  def getDataSetNameForFile(self,filename):
    jsondict = self.myQuery("dataset file = "+filename)
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
    datasetBlocks = self.myQuery('block dataset = '+datasetName)['data']
    print "processing ",len(datasetBlocks) ,"block of ",datasetName
    lumiDict = {}
    for block in [ b['block'] for b in datasetBlocks]:
      if len(block) < 1: # or block[0]['name'] != block[1]['name']:
        print 'block name result wrong ',block
        continue
      blockName = block[0]['name']
      print 'processing block ',blockName 
      lumisOfBlock = self.myQuery('lumi block = '+blockName)['data']
      for lumiRuns in [l['lumi'] for l in lumisOfBlock]:
        if len(lumiRuns) < 1: # or lumi[0]['number'] != lumi[1]['number']:
          print 'lumiRuns result wrong ',lumiRuns
          continue
        #print len(lumiRuns)," n lumi ",lumiRuns[0]['number']
        for lumi in lumiRuns:
          lumiNum=lumi['number'];lumiRunNumber=str(lumi['run_number'])
          #if len(lumiRuns) > 1:
          #  print 'lumiNum ',lumiNum," lumiRunNumber ",lumiRunNumber," blockName ",blockName," ",lumi['file']
          #if lumiNum==603 and lumiRunNumber == '163255':
          #  print 'err lumiNum ',lumiNum," lumiRunNumber ",lumiRunNumber," blockName ",blockName," ",lumi['file']
          #print lumisOfBlock
          if not lumiDict.has_key(lumiRunNumber):
            lumiDict[lumiRunNumber]=[]
          if lumiNum in lumiDict[lumiRunNumber]:
            print 'error lumi already there ', lumiRunNumber," ",lumiNum," ",blockName," continuing"
            #sys.exit('stop')
          lumiDict[lumiRunNumber].append(lumiNum)
    for k in lumiDict.keys():
      lumiDict[k].sort(key=lambda l : float(l))
    from FWCore.PythonUtilities.LumiList import LumiList
    return LumiList(runsAndLumis=lumiDict)
        

