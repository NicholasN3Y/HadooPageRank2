#!/bin/bash

inputFile=$1

if [[ $# -lt 1 ]] ; then
    echo "Usage run.sh <inputFilePath>"
    exit 1
fi

#cd src/assign2/cs4225
#echo -e  "\n########## compiling java code ##########\n"
#javac -cp `hadoop classpath` PageRankerV2.java -d ../../../bin
#echo -e  "\n########## java classes compiled ##########\n"
#cd ../../../bin
cd bin
#echo -e  "\n########## creating jar file ##########\n"
#jar -cvf PageRankerV2.jar .
#echo -e  "\n########## clean previous hadoop Job output directory ##########\n"
hadoop fs -rm -R a0112224/assignment_2/inputV2
hadoop fs -rm -R a0112224/assignment_2/outputV2
echo -e  "\n########## start hadoop MapReduce Job ##########\n"
hadoop jar PageRankerV2.jar assign2.cs4225.PageRankerV2 $inputFile
echo -e  "\n########## get output results from hadoop fs ##########\n"
rm ../final_results.txt
hadoop fs -get a0112224/assignment_2/outputV2/part-r-00000 ../final_results.txt
cd ..
echo -e "\nSorted final results can be found in final_results.txt"
echo -e  "\n########## DONE! ##########\n"


