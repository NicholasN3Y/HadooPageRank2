#!/bin/bash

inputFile=$1

if [[ $# -lt 1 ]] ; then
    echo "Usage run.sh <inputFilePath>"
    exit 1
fi

cd src/assign2/cs4225
echo -e  "\n########## compiling java code ##########\n"
javac -cp `hadoop classpath` PageRanker.java -d ../../../bin
echo -e  "\n########## java classes compiled ##########\n"
cd ../../../bin
echo -e  "\n########## creating jar file ##########\n"
jar -cvf PageRanker.jar .
echo -e  "\n########## clean previous hadoop Job output directory ##########\n"
hadoop fs -rm -R a0112224/assignmnet_2/input
hadoop fs -rm -R a0112224/assignment_2/output
echo -e  "\n########## start hadoop MapReduce Job ##########\n"
hadoop jar PageRanker.jar assign2.cs4225.PageRanker $inputFile
echo -e "\nIf results show less than ${K}, means that remaining files have 0 correlation."
echo -e  "\n########## get output results from hadoop fs ##########\n"
rm ../final_results.txt
hadoop fs -get a0112224/assignment_2/output/part-r-00000 ../final_results.txt
cd ..
echo -e "\nSorted final results can be found in final_results.txt"
echo -e  "\n########## DONE! ##########\n"


