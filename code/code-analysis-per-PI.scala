/**
* This program analyzes the correlation between the total publication number, 
* maximum IF, the total grant money and the affiliated institutes of each PI 
* from 2000 to 2012, and tests two machine learning models to predict the 
* publication number from grant number and size.
*
* @Author: Zhe Xu
*/

import scala.math
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics

val data=sc.textFile("/user/cloudera/final-data")
val data_s=data.map(line=>line.split(","))
val c1=data_s.map(line=>(line(2).split(";"), line(5),line(6),line(7)))
// Extract projects held by only one PI. 
val c2=c1.filter(line=>line._1.size==1) 
val c3=c1.filter(line=>line._1.size>1)
// If more than one PI hold a grant, keep only the "contact" PI. 
val c4=c3.map(line=>(line._1.filter(x=>x.contains("contact")),line._2,line._3,line._4))
val c5=c4.filter(line=>line._1.size>0)
val c6=c5.map(line=>(line._1.head.substring(0,line._1.head.indexOf(" ")),line._2,line._3,line._4))
val c8=c2.map(line=>(line._1.head, line._2,line._3,line._4))
val c9=c6.union(c8).map(line=>(line._1,(line._2.toDouble,line._3.toInt,line._4.toDouble)))
// Calculate the total publication number, total grants, and maximum IF. 
val reduced1=c9.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2,math.max(x._3,y._3)))

// Calculate the correlation between publication number and grant size. 
val pub_num=reduced1.map(line=>(line._2._2,line._2._1))
val reduced2=pub_num.reduceByKey((x,y)=>(x+y)/2)
reduced2.sortByKey().map(r=>r._1+","+r._2).saveAsTextFile("/user/cloudera/PubNumPI")
val col1=reduced2.map(line=>line._1.toDouble)
val col2=reduced2.map(line=>line._2)
val pcc1=Statistics.corr(col1,col2,"pearson")
// pcc1: Double = 0.6133594576651206 

// Calculate the correlation between publication IF and grant size. 
val pub_if=reduced1.map(line=>(line._2._3,line._2._1))
val pub_if2=pub_if.map(line=>((line._1/2).toInt,line._2))
val reduced3=pub_if2.reduceByKey((x,y)=>(x+y)/2)
reduced3.sortByKey().map(r=>r._1+","+r._2).saveAsTextFile("/user/cloudera/PubIFPI")
val col1=reduced3.map(line=>line._1.toDouble)
val col2=reduced3.map(line=>line._2)
val pcc1=Statistics.corr(col1,col2,"pearson")
// pcc1: Double = 0.2644803505165473 
 
// Identify the affiliated institutes for each PI. 
val pi_list=sc.textFile("/user/cloudera/pi_school")
val pi_s=pi_list.distinct()
val pi_s1=pi_s.map(line=>line.split(",")).filter(line=>line.size==2)
val pi_s2=pi_s1.map(line=>(line(1),line(0)))
val pi_c1=pi_s2.map(line=>(line._1.split(";"),line._2))
val pi_c2=pi_c1.filter(line=>line._1.size==1 && line._1(0)!="").map(line=>(line._1.head,line._2))
val pi_c3=pi_c1.filter(line=>line._1.size>1)
val pi_c4=pi_c3.map(line=>(line._1.filter(x=>x.contains("contact")),line._2))
val pi_c5=pi_c4.filter(line=>line._1.size>0 && line._1(0)!="")
val pi_c6=pi_c5.map(line=>(line._1.head.substring(0,line._1.head.indexOf(" ")),line._2))
val pi_c7=pi_c2.union(pi_c6)
val pi_school=pi_total.join(pi_c7)
pi_school.map(r=>r._1+","+r._2._1._1+","+r._2._1._2+","+r._2._1._3+","+r._2._2).saveAsTextFile("/user/cloudera/PI_Sch")

// Test ML models 
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

// Prepare data for training and testing. 
val parsedData=pi_total.map(line=>LabeledPoint(line._2._2.toDouble, Vectors.dense(line._2._1,line._2._4.toDouble))).cache()
val splits=parsedData.randomSplit(Array(0.9,0.1))
val training=splits(0).cache
val test=splits(1).cache

// Linear regression. 
val numIterations = 500
val stepSize = 0.0000000000000001
val algorithm = new LinearRegressionWithSGD()
algorithm.setIntercept(true)
algorithm.optimizer.setNumIterations(numIterations).setStepSize(stepSize)
val model = algorithm.run(training)
val prediction = model.predict(test.map(_.features))
val predictionAndLabel = prediction.zip(test.map(_.label))
val MSE = predictionAndLabel.map{case(v, p) => math.pow((v - p), 2)}.mean()

// Random forest. 
import org.apache.spark.mllib.tree.configuration.Algo
import org.apache.spark.mllib.tree.impurity.Variance

val algorithm = Algo.Regression
val impurity = Variance
val maximumDepth = 4
val treeCount = 100
val featureSubsetStrategy = "auto"
val seed = 5043

import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.RandomForest

val model = RandomForest.trainRegressor(training, new Strategy(algorithm, impurity, maximumDepth), treeCount, featureSubsetStrategy, seed)
val prediction = model.predict(test.map(_.features))
val predictionAndLabel = prediction.zip(test.map(_.label))
val MSE = predictionAndLabel.map{case(v, p) => math.pow((v - p), 2)}.mean()

