import FWCore as myFWCore
#####
class findEDFilters (object):
  def __init__(self):
    self.listFilters = []
  def enter(self,item):
    #print type(item)
    if isinstance(item,myFWCore.ParameterSet.Modules.EDFilter):
      if item.hasLabel_():
        self.listFilters.append(item.label())
      else:
        print "WARNING has no Label ",item
  def leave(self,item):
    pass
###############
def areContained (list1 ,list2 ):
 return [ el for el in list1 if el in list2]
###########
def areNotContained (list1 ,list2 ):
 return [ el for el in list1 if not el in list2]
########################
import FWCore.ParameterSet.Config as cms
########################
def debugCollection(coll,path,hists,process,prefix=""):
 newmod = myFWCore.ParameterSet.Config.EDAnalyzer('CandViewHistoAnalyzer', src = myFWCore.ParameterSet.Config.InputTag(coll), histograms = hists); setattr(process,prefix+coll+"TestAnalyzer",newmod); path += getattr(process,prefix+coll+"TestAnalyzer")
class AddFilterAndCreatePath(object):
 def __init__(self,additionalPaths = True):
  self.additionalPaths = additionalPaths
 def AddFilter(self,filter,path,process):
  path += filter
  if self.additionalPaths == True:
   newPath = cms.Path(path._seq)
   setattr(process,path.label()+"_"+filter.label()+"Path",newPath)
#####################
AddFilters = AddFilterAndCreatePath(True)

