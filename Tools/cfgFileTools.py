#####
import FWCore.ParameterSet.Config as cms
class EDFilterGatherer(object):
  def __init__(self):
    self.list=[] 
    self.noMods=-1
  def enter(self,obj):
    self.noMods+=1
    if isinstance(obj,cms.EDFilter):
      self.list.append(obj)
  def leave(self,obj):
    pass
###############
def createPathInclusiveMod(p,path,mod,label=""):
  expPath = path.expandAndClone()
  modNewPath = expPath._seq._collection[0:expPath._seq._collection.index(mod)+1]
  tmpPath = cms.Path()
  for m in modNewPath:
    tmpPath += m
  tmpPathLabel = "cutFlowPath"+label+path.label()+mod.label()
  setattr(p,tmpPathLabel,tmpPath)
  return tmpPathLabel
###############
def getViewCountFilters(path):
  edFiltersGath = EDFilterGatherer()
  path.visit(edFiltersGath)
  return [obj for obj in edFiltersGath.list if hasattr(obj,'type_') and 'ViewCountFilter' in obj.type_()]
#########################
def areContained (list1 ,list2 ):
 return [ el for el in list1 if el in list2]
###########
def areNotContained (list1 ,list2 ):
 return [ el for el in list1 if not el in list2]
########################
import FWCore.ParameterSet.Config as cms
########################
def debugCollection(coll,path,hists,process,prefix=""):
 newmod = cms.EDAnalyzer('CandViewHistoAnalyzer', src = cms.InputTag(coll), histograms = hists); setattr(process,prefix+coll+"TestAnalyzer",newmod); path += getattr(process,prefix+coll+"TestAnalyzer")
class AddFilterAndCreatePath(object):
 def __init__(self,additionalPaths = False):
  self.additionalPaths = additionalPaths
 def AddFilter(self,filter,path,process):
  path += filter
  if self.additionalPaths == True:
   newPath = cms.Path(path._seq)
   setattr(process,path.label()+"_"+filter.label()+"Path",newPath)
#####################
AddFilters = AddFilterAndCreatePath(True)

