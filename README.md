# Graspan Java

Welcome to the home repository of Graspan Java.

Graspan is a disk-based parallel graph system that uses an edge-pair centric computation model to compute dynamic transitive closures on large program graphs. Graspan has been implemented in two languages: Java and C++. This repository provides the Java implementation of Graspan.

This Readme (under revision) provides a how-to-use guide for Graspan Java. To see how to use the C++ version of Graspan, see [here](https://github.com/Graspan/graspan-cpp). 

For a detailed description of our system, please see the preliminary version of our paper [here](http://www.ics.uci.edu/~guoqingx/papers/wang-asplos17.pdf), which has been accepted in ASPLOS 2017. In addition, a tutorial of Graspan is scheduled to be presented in ASPLOS 2017. If you are interested, please visit our tutorial [page](http://www.ics.uci.edu/~guoqingx/asplos-tutorial/main.html). 

## Getting Started

Using Graspan Java is very simple, and no compilation is necessary. 

First ensure you have **JDK 1.6** (or a later version of JDK) installed in your machine. 

Then copy the **executables** folder in the **src** folder of the **graspan-java** repository, into any location in your machine. The folder contains:

* *graspan.jar* - executable of the graspan-java project
* *graspan-java-run.sh* - script for running graspan-java
* *rules_pt* - sample grammar file containing rules for points-to analysis
* *rules_np* - sample grammar file containing rules for dataflow analysis

Now you will need a graph on which Graspan can perform computations. 
Copy any graph from our datasets [here](https://drive.google.com/drive/folders/0B8bQanV_QfNkbDJsOWc2WWk4SkE?usp=sharing) inside the **executables** folder in your machine. 

Then execute the **graspan-java-run.sh** script in your command line specifying, 

1. the graph file,
2. the number of partitions graspan should generate from the graph during preprocessing, prior to computation, 
3. whether or not the graph has edge values (yes or no),
4. the grammar file,

as shown below, 
```
./graspan-java-run.sh <graph_filename> <num_of_partitions> <has_edge_values?(yes/no)> <grammar_filename> 
```

Here is an **example**,
```
./graspan-java-run.sh mygraph 5 yes rules_pt  
```

After running the above command, you can monitor the progress of the computation by viewing the generated **comp.output** file. After computation ends, the comp.output file will show the number of edges generated and the total computation time. The **.partition.** output files will contain the partitioned graph with new edges. 

## Project Contributors

* [**Kai Wang**](http://www.ics.uci.edu/~wangk7/) - *PhD Student, UCI* 
* [**Aftab Hussain**](http://www.ics.uci.edu/~aftabh/) - *PhD Student, UCI* 
* [**Zhiqiang Zuo**](http://zuozhiqiang.bitbucket.io/) - *Postdoc Scholar, UCI* 
* [**Harry Xu**](http://www.ics.uci.edu/~guoqingx/) - *Assistant Professor, UCI* 
* [**Ardalan Sani**](http://www.ics.uci.edu/~ardalan/) - *Assistant Professor, UCI* 
* **John Thorpe** - *Undergraduate Student, UCI*
* **Sung-Soo Son** - *Visiting Undergraduate Student, UCI*
* [**Khanh Ngyuen**](http://www.ics.uci.edu/~khanhtn1/) - *PhD Student, UCI*
