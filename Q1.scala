val business = sc.textFile("hdfs://cshadoop1/yelp/business/business.csv").map(line=>line.split("\\^")); 
val review = sc.textFile("hdfs://cshadoop1/yelp/review/review.csv").map(line=>line.split("\\^"))
val businessKVpairs = business.map(line=>(line(0),line(1).toString+" "+line.last.toString)).distinct
val reviewTotal = review.map(line=>(line(2),line(3).toDouble)).reduceByKey((a,b)=>a+b).distinct
val businessTotal = review.map(line=>(line(2),1)).reduceByKey((a,b)=>a+b)
val businessReviews = reviewTotal.join(businessTotal)
val avgerageReview = businessReviews.map(a=>(a._1,a._2._1/a._2._2))
val output = businessKVpairs.join(avgerageReview)
val sortOutput = output.collect().sortWith(_._2._2 > _._2._2)
val top10 = sortOutput.take(10)
sc.parallelize(top10).coalesce(1,true).saveAsTextFile("hdfs://cshadoop1/user/pxk166230/assignment3/q1")
System.exit(0)