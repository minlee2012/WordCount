/**
* Min Lee
* Professor Grace Yang
* Big Data Analytics - COSC 282
*
*/
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.util.matching.Regex

object wordCount {
	def main(args: Array[String]) { 
		
		val conf = new SparkConf().setMaster("spark://Mins-MacBook-Pro.local:7077").setAppName("wordCount")
		val sc = new SparkContext(conf)//instantiates spark context

		//loading in files
		val file = sc.textFile("one.txt")
		val file2 = sc.textFile("two.txt")

		//Mapping all words regardless of repeats
		//Flattening the file into a flatmap in which I replace all characters that are NOT letters or spaces (I'm not counting
		//numbers as words). Then, I split the objects by spaces and then mapped them.
		val fileData = file.flatMap(line => line.replaceAll("[^a-zA-Z\\s]", "").split(" ")).map(word=>(word,1))
		val file2Data = file2.flatMap(line => line.replaceAll("[^a-zA-Z\\s]", "").split(" ")).map(word=>(word,1))
		
		//Remapping all of the words as lowercase to handle different cases
		val fileWordCount = fileData.map { case (key, value) => key.toLowerCase -> value }
		val file2WordCount = file2Data.map { case (key, value) => key.toLowerCase -> value }

		//I kept a separate number count because while numbers aren't being counted as words, they still take up characters
		//and some might be interested in that data.
		val fileNumCount = file.flatMap(line => line.replaceAll("[^0-9\\s]", "").split(" ")).map(word=>(word,1))
		val file2NumCount = file2.flatMap(line => line.replaceAll("[^0-9\\s]", "").split(" ")).map(word=>(word,1))

		//Finding the distinct words
		val fileDistinctCount = fileWordCount.distinct()
		fileDistinctCount.collect()
		val file2DistinctCount = file2WordCount.distinct()
		file2DistinctCount.collect()

		//Combining the two transformed RDDs to write into a file
		val combinedRDD = fileWordCount.union(file2WordCount)
		//Unfortunately, I was unable to just pull the values of the two maps, so now it's a little redundant and I've 
		//essentially mapped a map. Still, as far as functionality goes, I do count the repeated members, so please 
		//be nice :)
		val combinedDistinctRDD = combinedRDD.map(word=>(word,1)).reduceByKey(_+_)

		//OUTPUTS
		print("Word Count for file 1: ")
		println(fileWordCount.count())
		print("Word Count for file 2: ")
		println(file2WordCount.count())

		print("Unique words in file 1: ")
		println(fileDistinctCount.count())
		print("Unique words in file 2: ")
		println(file2DistinctCount.count())

		combinedDistinctRDD.saveAsTextFile("../wcOutput")

		sc.stop()//stops spark context

	}//main
}//wordCount