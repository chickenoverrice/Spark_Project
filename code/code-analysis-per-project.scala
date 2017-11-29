/**
* This program analyzes the correlation between the average publication number, 
* maximum IF, and the average grant money of each project from 2000 to 2012, 
* with three different models and calculates the pearson's correlation coefficient.
*
* @Author: Zhe Xu
*/

import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.stat.Statistics

val data=sc.textFile("/user/cloudera/final-data")
val data_s=data.map(line=>line.split(","))

// Publication_number--Money for all projects and all years. 
val pub_num=data_s.map(line=>(line(6), line(5).toDouble))
val reduced1=pub_num.reduceByKey((x,y)=>(x+y)/2)
reduced1.map(r=>r._1+","+r._2).saveAsTextFile("/user/cloudera/PubNumPro")

val col1=reduced1.map(line=>line._1.toDouble)
val col2=reduced1.map(line=>line._2)
val pcc1=Statistics.corr(col1,col2,"pearson")
// pcc1: Double = 0.32665637404772363   

// Publication_number--Money for each study section for all years. 
val pub_num_sec=data_s.map(line=>((line(4),line(6)),line(5).toDouble))
val reduced2=pub_num_sec.reduceByKey((x,y)=>(x+y)/2)
reduced2.sortByKey().map(r=>r._1._1+","+r._1._2+","+r._2).saveAsTextFile("/user/cloudera/PubNumSec")

// Publication_IF--Money for all projects and all years. 
val pub_if=data_s.map(line=>(line(7).toDouble, line(5).toDouble))
val pub_if2=pub_if.map(line=>((line._1/2).toInt,line._2))       // Bin data (every 2 points of IF) 
val reduced3=pub_if2.reduceByKey((x,y)=>(x+y)/2)
reduced3.sortByKey().map(r=>r._1+","+r._2).saveAsTextFile("/user/cloudera/PubIFPro")

val col3=reduced3.map(line=>line._1.toDouble)
val col4=reduced3.map(line=>line._2)
val pcc2=Statistics.corr(col3,col4,"pearson")
// pcc2: Double = -0.06321118401580224 

// Publication_IF--Money for each study section for all years. 
val pub_if_sec=data_s.map(line=>((line(4),line(7).toDouble), line(5).toDouble))
val pub_if_sec2=pub_if_sec.map(line=>((line._1._1,(line._1._2/2).toInt),line._2)) // Bin 2 
val reduced4=pub_if_sec2.reduceByKey((x,y)=>(x+y)/2)
reduced4.sortByKey().map(r=>r._1._1+","+r._1._2+","+r._2).saveAsTextFile("/user/cloudera/PubIFSec")

// Publication_IF--Money by project supporting length for all years. 
val data_s2=data_s.map(line=>(line(0).split('-')(1).substring(0,2).toInt,line(5).toDouble, line(6).toInt, line(7).toDouble))
val data_s3=data_s2.map(line=>(line._1,(line._2,line._3,line._4)))
val data_r1=data_s3.reduceByKey((x,y)=>((x._1+y._1)/2,(x._2+y._2)/2,(x._3+y._3)/2)) 
data_r1.sortByKey().map(r=>r._1+","+r._2._1+","+r._2._2+","+r._2._3).saveAsTextFile("/user/cloudera/PubLen")


