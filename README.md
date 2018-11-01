## Introduction
Spark has distributed collection of items called as RDDs (Resilient Distributed Dataset). They can be created from the Hadoop InputFormats.

## Prerequsites
1. Have Spark installed in you systems.
2. Make sure you changed the permissions to modify the database. 

## Aggregate:

A new RDD from the text of the "winemag_data" file in the directory. For reading the content from the file:

```
>val inputFile = sc.textFile("C:/44564/scala_sample/winemag_data.txt")
```
The filter command is used to filter the rows if they are the null or not. It returns a new dataset formed by selecting those elements of the source on which second column is null or not and returns true.
```
>val filcountry = inputFile.filter(rec => (rec.split(",")(1)!=null))
```

The below line defines "tmp1" as the result of a map transformation by passing each element of the source.
```
>val tmp1 = filcountry.map(_.split(",")).map( p=>p(1))
```
The following is the command for calculating the aggregate count of the countries

```
>val fin = tmp1.flatMap(line => line.split("\n")).map(word => (word,1)).reduceByKey(_ + _);
```
The following command writes the elements of the dataset as a text file (or set of text files) in "show-spark" directory in the local filesystem. Spark will call toString on each element to convert it to a line of text in the file.
```
>fin.saveAsTextFile("c:/tmp/show-spark/output")
```

If you want to check the output for first 100 lines you can use the below code
```
>val tmp1 = filcountry.map(_.split(",")).map( p=>p(1)).take(100).foreach(println)
```
Using the command below you end up with rdd to be divided exactly to 5 partitions of roughly equal sizes.

```
>val repart = counts.repartition(5)
```

## Wordcount:

The following are the command to count the words in a text file.

```
>val inputFile = sc.textFile("C:/44564/scala_sample/winemag_data.txt")
```

```
>val counts = inputFile.flatMap(line => line.split(",")).map(word => (word,1)).reduceByKey(_ + _);
```

```
>counts.toDebugString
```

```
>counts.cache
```

For storing the values as partitions 
```
>counts.saveAsTextFile("c:/tmp/show-spark/output")
```

## Other commands

spark.read.csv("C:/44564/scala_sample/winemag_data.txt")

.show()

spark.read.option("header",true).csv("C:/44564/scala_sample/winemag_data.txt")

val counts1 = country.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_ + _);

val country = wineVal.filter(rec => (rec.split(",")(4) == "15"))

counts.saveAsTextFile("/tmp/show-spark/output1")


new PrintWriter("countries.txt") {
  fin.foreach {
    case (k, v) =>
      write(k + ":" + v)
      write("\n")
  }
  close()
}

## Data Chart for the obtained partitions

![screenshot 253](https://user-images.githubusercontent.com/31740161/47864632-1af10180-ddc8-11e8-9658-9c8a389a4189.png)  

![screenshot 254](https://user-images.githubusercontent.com/31740161/47864692-3e1bb100-ddc8-11e8-9e87-b055429ebda2.png)

## References:

1. https://spark.apache.org/docs/2.1.0/programming-guide.html#resilient-distributed-datasets-rdds

2. https://www.youtube.com/watch?v=MkdPchZDlNE

3. https://www.youtube.com/watch?v=M8m_HOXX2Uo&t=124s

4. http://vishnuviswanath.com/spark_rdd.html
