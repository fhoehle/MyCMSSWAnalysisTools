#!/bin/bash
pkgs=(
  "MyCrabTools ./ ./intall.sh" 
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
  cd $CMSSW_BASE/`echo ${pkgs[$idx]} | awk '{print $2}'`
  getGitPackage `echo ${pkgs[$idx]} | awk '{print $1}'`
  git checkout `echo ${pkgs[$idx]} | awk '{print $4}'`
  if  [ "X`echo ${pkgs[$idx]} | awk '{print $3}'`" != "X" ]; then
    echo "calling additional command "`echo ${pkgs[$idx]} | awk '{print $3}'`
    eval `echo ${pkgs[$idx]} | awk '{print $3}'`
  fi
  cd $CMSSW_BASE
done
cd $CMSSW_BASE/src 
addpkg FWCore/PythonUtilities
cd $CMSSW_BASE
cd $HOME
git clone git@github.com:fhoehle/PyRoot_Helpers.git
cd $CMSSW_BASE

