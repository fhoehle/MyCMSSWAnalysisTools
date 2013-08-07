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
    jsondict = das_client.get_data(self.host, "dataset file = "+filename, self.idx, self.limit, self.debug, self.thr, self.ckey, self.cert)
    return [ str(ele.get('dataset')[0].get('name')) for ele in jsondict.get('data')] 
