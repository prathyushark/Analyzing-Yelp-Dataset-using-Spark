val business = sc.textFile("hdfs://cshadoop1/yelp/business/business.csv").map(line=>line.split("\\^"))
val review = sc.textFile("hdfs://cshadoop1/yelp/review/review.csv").map(line=>line.split("\\^"))

val getBusinesses = business.filter(line=>line(1).contains("Stanford")).map(line=>(line(0).toString,line(1).toString))
val getReviews = review.map(line=>(line(2).toString,(line(1).toString,line(3).toDouble)))

val output = getReviews.join(getBusinesses) 

val display = output.map(line=>(line._2._1._1,line._2._1._2))
display.saveAsTextFile("hdfs://cshadoop1/user/pxk166230/assignment3/q3")
System.exit(0)