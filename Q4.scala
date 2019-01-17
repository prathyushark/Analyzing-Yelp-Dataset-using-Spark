val user = sc.textFile("hdfs://cshadoop1/yelp/user/user.csv").map(line=>line.split("\\^"))
val review = sc.textFile("hdfs://cshadoop1/yelp/review/review.csv").map(line=>line.split("\\^"))

val userData = user.map(line=>(line(0),line(1).toString))
val reviewCount = review.map(line=>(line(1),1)).reduceByKey((a,b)=>a+b).distinct
val output = userData.join(reviewCount).distinct.collect()
val finalOutput = output.sortWith(_._2._2>_._2._2).take(10)
val display = finalOutput.map(line=>(line._1,line._2._1,line._2._2))
sc.parallelize(display).coalesce(1,true).saveAsTextFile("hdfs://cshadoop1/user/pxk166230/assignment3/q4")

System.exit(0)