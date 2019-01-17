val business = sc.textFile("hdfs://cshadoop1/yelp/business/business.csv").map(line=>line.split("\\^"))
val review = sc.textFile("hdfs://cshadoop1/yelp/review/review.csv").map(line=>line.split("\\^"))

val getBusinesses = business.filter(line=>line(1).contains("TX")).map(line=>(line(0).toString,line(1).toString))
val reviewCount = review.map(line=>(line(2),1)).reduceByKey((a,b)=>a+b)
val output = getBusinesses.join(reviewCount)
val display = output.map(line=>(line._1,line._2._2))
display.saveAsTextFile("hdfs://cshadoop1/user/pxk166230/assignment3/q5")

System.exit(0)