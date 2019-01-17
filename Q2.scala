val user = sc.textFile("hdfs://cshadoop1/yelp/user/user.csv").map(line=>line.split("\\^"))
val review = sc.textFile("hdfs://cshadoop1/yelp/review/review.csv").map(line=>line.split("\\^"))
val reviewTotal = review.map(line=>(line(1),line(3).toDouble)).reduceByKey((a,b)=>a+b).distinct 
val userReviewCount = review.map(line=>(line(1),1)).reduceByKey((a,b)=>a+b).distinct
val userReviews = reviewTotal.join(userReviewCount)
val avgUserReview = userReviews.map(a=>(a._1,a._2._1/a._2._2))

println("Enter the User name:(e.g. Matt J.)")
val username = Console.readLine()

val getUser = user.filter(line=>line(1).contains(username)).map(line=>(line(0).toString,line(1).toString))
val userData = user.map(line=>(line(0).toString,line(1).toString))
val output = avgUserReview.join(userData)
val finalOutput = output.join(getUser)
val avgRating = finalOutput.collect()
sc.parallelize(avgRating).coalesce(1,true).saveAsTextFile("hdfs://cshadoop1/user/pxk166230/assignment3/q2")

System.exit(0)

