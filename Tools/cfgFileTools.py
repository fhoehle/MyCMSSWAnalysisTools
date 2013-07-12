import FWCore as myFWCore
#####
class findEDFilters (object):
  def __init__(self):
    self.listFilters = []
  def enter(self,item)
    print type(item)
    if isinstance(item,myFWCore.ParameterSet.Modules.EDFilter):
      self.listFilters.append(item)
  def leave(self,item):
    pass
