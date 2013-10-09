class runRangeManager(object):
  def __init__(self,l,debug=False):
    from copy import deepcopy
    self.listR = deepcopy(l)
    if not isinstance(self.listR[0],runRange):
      tmpList = deepcopy(self.listR);self.listR=[]
      for r in tmpList:
        self.listR.append(runRange(r[0],r[-1]))
    self.debug=debug
  def calcTriggerRunRanges(self):
    self.listR.sort(key=lambda r:r.min)
    from copy import deepcopy
    runRanges = deepcopy(self.listR)
    mins=list(set([ r.min for r in runRanges]));mins.sort()
    maxs=list(set([ r.max for r in runRanges]));maxs.sort();
    openRanges = [r for r in runRanges if r.min==mins[0]]
    lowerLimit=mins.pop(0)
    borders=list(set(mins+maxs));borders.sort();
    for r in openRanges:
      runRanges.remove(r)
    self.ranges=[]
    while len(borders) != 0:
      if self.debug:
        print "debug begin borders ",borders," mins ", mins ," maxs ",maxs," lowerLimit ",lowerLimit, " openRanges ",[ [r.min,r.max] for r in openRanges ]," ranges ",self.ranges
      if (len(mins) and borders[0] == mins[0]) or (borders[0] == maxs[0]):
        if len(mins) and borders[0] == mins[0]:
          for r in runRanges:
            if r.min == borders[0]:
              openRanges.append(r)
              runRanges.remove(r) 
          self.ranges.append([lowerLimit,mins[0] - 1])
          lowerLimit = int(mins[0])
          mins.remove(mins[0])
        if borders[0] == maxs[0]:
          for r in openRanges:
	    if r.max ==  borders[0]:
	      openRanges.remove(r)
          self.ranges.append([lowerLimit,maxs[0]])  
          lowerLimit= maxs[0]+1
          if len(openRanges) == 0 and len(mins):
            lowerLimit =  mins[0];mins.remove(mins[0])
            borders.remove(lowerLimit)
            openRanges = [r for r in runRanges if r.min==lowerLimit]
          maxs.remove(maxs[0])
      else:
        print "something failed, runRangeManager"
      borders.pop(0)
      if self.debug:
        print "debug end borders ",borders," mins ", mins ," maxs ",maxs," lowerLimit ",lowerLimit, " openRanges ",[ [r.min,r.max] for r in openRanges ]," ranges ",self.ranges
#################################
class runRange(object):
  def __init__(self,min,max):
    self.min=min;self.max=max
  def __repr__(self):
    return 'runRange("%s","%s")' % (self.min, self.max)
def test():
  import copy
  r1 = runRange(165, 178)
  r2 = runRange(180, 190)
  r3 = runRange(167, 182)
  r4 = runRange(200, 220)
  r5 = runRange(182, 183)
  runRanges = [r1,r2,r3,r4,r5]
  t=runRangeManager(runRanges,True)
  t.calcTriggerRunRanges()
  print ([ [r.min,r.max] for r in runRanges])
  print t.ranges
