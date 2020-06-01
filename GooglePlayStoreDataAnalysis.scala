package com.xuvantum.spark
//Average app rating category wise
//Which app category has highest and lowest rating

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object GPlayAppAnalysis {
  case class App(Category:String, Rating:Float)
  
  def main(args:Array[String]):Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
      .builder()
      .appName("GooglePlayApps Data analysis")
      .config("spark.sql.warehouse","file:///C:tmp")
      .master("local[*]")
      .getOrCreate()
      
    
    val data = spark.sparkContext.textFile("../Data_Folder/googleplaystore.csv")
     
     //data contains header so i am removing the header
    val head = data.first();
    val skipped_header = data.filter(x => x!=head)
    //    skipped_header.take(2)
    //res1: Array[String] = Array(Photo Editor & Candy Camera & Grid & ScrapBook,ART_AND_DESIGN,4.1,159,19M,"10,000+",Free,0,Everyone,Art & Design,"January 7, 2018",1.0.0,4.0.3 and up, Coloring book moana,ART_AND_DESIGN,3.9,967,14M,"500,000+",Free,0,Everyone,Art & Design;Pretend Play,"January 15, 2018",2.0.0,4.0.3 and up)

    
    // Now converting dataframe to dataset with case class "App"
    val catNrat = skipped_header.map(x=>App(x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(1), x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)(2).toFloat))
    
    //Now we will convert catNrat to dataset
    import spark.implicits._
    val dss = catNrat.toDS().cache()
    val  ds = dss.filter(x => x.Rating !=19.0)
    ds.count()  //10841
    
    //Since this Dataset contains some NaN value, which will create problem while calculating average
    val ds_without_NaN = ds.na.drop()
    ds_without_NaN.count()  //9367
    
    ds_without_NaN.createOrReplaceTempView("apptable")
    val unsorted_res = spark.sql("SELECT category, AVG(rating) as Avg_Rating from apptable GROUP BY Category ORDER BY Category").cache()
    val avg_rating_cat_wise = unsorted_res.collect()
    
    println("Printint Average App Rating Category Wise")
    avg_rating_cat_wise.foreach(println)
    println("********************************************")
    
   
    println("App category having highest average rating")
    val highest = unsorted_res.sort(desc("Avg_Rating")).take(2)
    println(highest(0))
    
    println("App category having lowest average rating")
    val lowest = unsorted_res.sort("Avg_Rating").take(1)
    println(lowest(0))
    
    spark.close()
    
              
  }
}
