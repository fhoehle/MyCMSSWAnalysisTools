#!/bin/bash
pkgs=(
  "MyCrabTools  $CMSSW_BASE ./install.sh"
  "ParallelizationTools  $CMSSW_BASE ./install.sh"
  "PyRoot_Helpers $HOME"
)
cmsswVer=CMSSW_5_3_
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
  exit 1
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
function getCMSGitPackage {
  cd $CMSSW_BASE/src
  pkg=`echo $1 | sed 's/^\([^\/]\+\)\/[^\/]\+\/*$/\1/'`
  echo "pkg "$pkg
  subPkg=`echo $1 | sed 's/^[^\/]\+\/\([^\/]\+\)\/*$/\1/'`
  echo "subPkg "$subPkg
  if [ ! -d "$CMSSW_BASE/src/.git/info/sparse-checkout" ]; then
    sparseSubPkg=""
    sparsePkg=""
  else
    sparseSubPkg=$(grep -F "$1" $CMSSW_BASE/src/.git/info/sparse-checkout || echo "")
    sparsePkg=$(grep -F "$pkg" $CMSSW_BASE/src/.git/info/sparse-checkout || echo "")
  fi
  if [ -d "$1" ] && [ "$sparseSubPkg" != "" ]; then
    echo "cms package already there $1"
  elif [ -d "$1" ] && [ "$sparseSubPkg" == "" ]; then
    echo "Warning package there but not CMS git"
  elif [ -d "$pkg" ] && [ "$sparsePkg" != "" ]; then
    echo "package $pkg is know, using git cms-addpkg"
    git cms-addpkg $1
  elif [ -d "$pkg" ] && [ "$sparsePkg" == "" ]; then
    tmpSrc=$CMSSW_BASE/tmpSrc
    mkdir -p $tmpSrc
    pkgbck=${tmpSrc}/${pkg}_bck
    echo "backupCopy of content of $pkg in $pkgbck"
    mv $pkg $pkgbck
    git cms-addpkg $1
    cp -r $pkgbck/$pkg/* $pkg/
    rm -rf $pkgbck
  elif [ ! -d "$pkg" ]; then
    echo "installing via git cms-addpkg"
    git cms-addpkg $1
  else
    echo "this should not happend, trying to get $1"
    ls
  fi
  cd $CMSSW_BASE
}
echo "start"
getCMSGitPackage FWCore/PythonUtilities
getCMSGitPackage PhysicsTools/Utilities
cd $CMSSW_BASE/src
git am --signoff < $CMSSW_BASE/MyCMSSWAnalysisTools/copyPickMerge_patch.txt
cd $CMSSW_BASE
################
if [ -d "$CMSSW_BASE/src/RecoLuminosity/LumiDB" ]; then 
  if [-d "$CMSSW_BASE/src/RecoLuminosity/LumiDB/.git" ]; then
    echo " RecoLuminosity/LumiDB already there and is git repo, check tags just updating"
    cd $CMSSW_BASE/src/RecoLuminosity/LumiDB/
    git fetch
  else
    echo "warning RecoLuminosity/LumiDB already there, no git repo !!!"
    echo "git clone https://github.com/cms-sw/RecoLuminosity-LumiDB.git $CMSSW_BASE/src/RecoLuminosity/LumiDB AND git checkout V04-02-10"
  fi
else
  git clone https://github.com/cms-sw/RecoLuminosity-LumiDB.git $CMSSW_BASE/src/RecoLuminosity/LumiDB
  cd $CMSSW_BASE/src/RecoLuminosity/LumiDB
  git checkout V04-02-10
fi
cd $CMSSW_BASE
scram b -j 5 
