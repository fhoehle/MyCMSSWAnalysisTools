# run cmssw analysis
import sys
class sample():
  def __init__(self,name):
    self.name = name
  def xSec(self,xSec):
    self.xSec = float(xSec)
  def numEvts(self,numEvts):
    self.numEvts=float(numEvts)
  def intLumi(self):
    if self.numEvts and self.xSec:
      return  self.numEvts/self.xSec
def runOverSample (cfg,samp):
  pass

