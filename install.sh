#!/bin/bash
pkgs=(
  "MyCrabTools  $CMSSW_BASE ./install.sh"
  "ParallelizationTools  $CMSSW_BASE ./install.sh"
  "PyRoot_Helpers $HOME"
)
cmsswVer=CMSSW_4_2_8_patch7
###################
function getGitPackage {

if [ -d "$1" ]; then
  echo "updating "$1
  cd $1
  git fetch
else
  echo "installing "$1
  git clone git@github.com:fhoehle/$1.git
  cd $1
fi
  
}

echo "Installing/Updating MyCMSSWAnalysisTools "
#
if [[ ! "$CMSSW_BASE" =~ "$cmsswVer" ]]; then
  echo "missing CMSSW_BASE cmsenv"
fi
cd $CMSSW_BASE
set -e
# install my packages
for idx in ${!pkgs[*]}; do
  cd `echo ${pkgs[$idx]} | awk '{print $2}'`
  getGitPackage `echo ${pkgs[$idx]} | awk '{print $1}'`
  if  [ "X`echo ${pkgs[$idx]} | awk '{print $4}'`" != "X" ]; then
   git checkout `echo ${pkgs[$idx]} | awk '{print $4}'`
  fi
  if  [ "X`echo ${pkgs[$idx]} | awk '{print $3}'`" != "X" ]; then
    echo "calling additional command "`echo ${pkgs[$idx]} | awk '{print $3}'`
    pwd
    eval `echo ${pkgs[$idx]} | awk '{print $3}'`
  fi
  cd $CMSSW_BASE
done
cd $CMSSW_BASE/src 
git cms-addpkg FWCore/PythonUtilities
cd $CMSSW_BASE
cd $CMSSW_BASE/src
git cms-addpkg PhysicsTools/Utilities
git am --signoff < $CMSSW_BASE/MyCMSSWAnalysisTools/copyPickMerge_patch.txt
cd $CMSSW_BASE
scram b -j 5 
