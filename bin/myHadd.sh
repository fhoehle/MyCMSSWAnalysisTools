#!/bin/bash
allArgs=("$@")
#echo ${allArgs[@]}
target=${allArgs[0]}
echo "target "$target
files=(${allArgs[@]:1})
if [ -f $PWD"/"$target ] ; then
   echo "$target already exists"
   exit 1 
fi
#echo "files "${files[@]}
chunkSize=20
noFiles=${#files[@]}
echo "noF "$noFiles
mergeSteps=$(($noFiles/$chunkSize+1))
echo "mergeSteps "$mergeSteps
targetTmpOld="";targetTmp="";targetTmpPattern=$target"_tmp"
for ((i=0; i<${mergeSteps}; i++));
do
  echo "merging "$i
  begin=$(($i*$chunkSize))
  #end=$((($i+1)*$chunkSize))
  end=$chunkSize
  echo "begin "$begin" end "$end
  targetTmpOld=$targetTmp
  targetTmp=$targetTmpPattern$i
  haddArgs=""
  echo "files "${files[@]:$begin:$end}
  echo "oldTarget "$targetTmpOld
  if [ $i -eq $(($mergeSteps-1)) ] ; then
    haddArgs=$target" "${files[@]:$begin:$end}" "$targetTmpOld
  else
    haddArgs=$targetTmp" "${files[@]:$begin:$end}" "$targetTmpOld
  fi
  hadd $haddArgs 
done
rm "$targetTmpPattern"*
