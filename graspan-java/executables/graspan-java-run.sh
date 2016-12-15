#!/bin/bash
JARFILE="graspan.jar"
partGenClassPath="edu.uci.ics.cs.graspan.dispatcher.PartGenClient"
erAdderAndSorterClassPath="edu.uci.ics.cs.graspan.dispatcher.PartERuleAdderAndSorterClient"
graph=$1
numParts=$2
hasEdgeVals=$3
firstVID=$4
grammar=$5

echo "Preprocessing..."

echo "Preparing grammar files..."
 cp $grammar $graph.grammar
 cp $grammar $graph.eRulesAdded.grammar
  
echo "Generating initial partitions..."
 java -Xmx6G -cp $JARFILE $partGenClassPath $graph $numParts $hasEdgeVals $firstVID -ea  2> pp.pgen1.output
#java -Xloggc:addEedges.gctimes.output -XX:+PrintGCTimeStamps -Xmx6G -cp $JARFILE $classPath ppconfig -ea 2> pp.eadd.output

echo "# of Partitions Created:"
numPartsActual=$(ls *.partition.*.degrees | wc -l)
echo $numPartsActual

echo "Adding Erules to partitions and sorting them..."
java -Xmx6G -cp $JARFILE $erAdderAndSorterClassPath $graph $numPartsActual $hasEdgeVals $firstVID -ea 2> pp.erAndSort.output

echo "Generating final partitions..."
java -Xmx6G -cp $JARFILE $partGenClassPath $graph".eRulesAdded" $numPartsActual $hasEdgeVals $firstVID -ea 2> pp.pgen2.output

echo "# of Partitions Created:"
numPartsActual=$(ls *.partition.*.degrees | wc -l)
echo $(( numPartsActual / 2 ))

echo "Computing..."

CompPath="edu.uci.ics.cs.graspan.dispatcher.ComputationClient"
graph=$graph".eRulesAdded"
numThreads=8
partSizeAfterNewEdges=50 #millions of edges

java -Xloggc:comp.gctimes.output -XX:+PrintGCTimeStamps -Xmx6G -cp $JARFILE $CompPath $graph $numPartsActual $numThreads $partSizeAfterNewEdges -ea 2> comp.output







