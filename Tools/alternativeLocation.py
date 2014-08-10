import os,sys,re
import xml.dom.minidom as minidom
sys.path.append(os.getenv('CMSSW_BASE')+'/MyCMSSWAnalysisTools')
import Tools
import Tools.coreTools as coreTools
class xRootDPathCreator(object):
  def __init__(self,confFile=os.getenv('CMS_PATH')+'/SITECONF/local/PhEDEx/storage.xml',debug=False):
    self.confF = confFile
    self.debug=debug
  def findXRootDPrefix(self):
    dom = minidom.parse(self.confF)
    storageMapping = coreTools.myGetSubNodeByName(dom,'storage-mapping')[0]
    xRootDPrefix=None
    for n in storageMapping.childNodes:
      if n.nodeName == 'lfn-to-pfn' and n.hasAttribute('protocol'):
        if 'remote-xrootd' == n.getAttribute('protocol'):
          xRootDPrefix=(n.getAttribute('result'))
    if not xRootDPrefix:
      print "warning no remote-xrootd protocol found"
      return None
    self.xRootDPrefix=re.sub('/store/\$1',"",xRootDPrefix)
  def getXrootDPath(self,filename):
    if not hasattr(self,'xRootDPrefix'):
      self.findXRootDPrefix()
    xRootDPath = self.xRootDPrefix + filename #re.sub('/store/\$1',filename,self.xRootDPrefix)
    if self.debug:
      print "xRootDPath ",xRootDPath
    return xRootDPath
###########################
def listAllNodes(node):
  for sN in node.childNodes:
      print (sN.nodeName," ",sN.attributes.items())

