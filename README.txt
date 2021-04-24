Demonstrate that when an exact analysis is too costly (hence, unfeasible for large inputs), resorting to careful approximation strategies might yield a substantial gain in performance at the expense of a limited loss of accuracy. 

>Points can be represented as instances of the class org.apache.spark.mllib.linalg.Vector and can be manipulated through static methods offered by the class org.apache.spark.mllib.linalg.Vectors. For example, method Vectors.dense(x) transforms an array x of double into an instance of class Vector, while method Vectors.sqdist(x,y) computes the (d(x,y))^2 between two Vector x and y, where "d(.,.)" is the standard Euclidean L2-distance. 

>Time measurements. Measuring times when using RDDs in Spark requires some care, due to the lazy evaluation mechanism, namely the fact that RDD transformations are executed only when an action (e.g., counting the number of elements of the RDD) requires the transformed data. 

>Broadcast variables. When read-only global data declared in the main program must be used by an RDD transformation (e.g., by map, flatMap or flatMapToPair methods) it is convenient to declare them as broadcast variables, which Spark distributes efficiently to the workers executing the transformation. 


Input:

    1.A path to a text file containing point set in Euclidean space partitioned into k clusters. Each line of the file contains, separated by commas, 	the coordinates of a point and the ID of the cluster 
    2.The number of clusters k (an integer).
    3.The expected sample size per cluster t (an integer).

Algorithm:

    1.Read the input data. In particular, the clustering must be read into an RDD of pairs (point,ClusterID) called fullClustering which must be 	cached and partitioned into a reasonable number of partitions, e.g., 4-8. (Hint: to this purpose, you can use the code and the suggestions 	provided in the file Input.java).
    2.Compute the size of each cluster and then save the k sizes into an array or list represented by a Broadcast variable named sharedClusterSizes. 	(Hint: to this purpose it is very convenient to use the RDD method countByValue() whose description is found in the Spark Programming Guide)
    3.Extract a sample of the input clustering, where from each cluster C, each point is selected independently with probability min{t/|C|, 1} 	(Poisson Sampling). Save the sample, whose expected size is at most t*k, into a local structure (e.g., ArrayList in java or list in Python) 	represented by a Broadcast variable named clusteringSample. (Hint: the sample can be extracted with a simple map operation on the RDD 	fullClustering, using the cluster sizes computed in Step 2).
    4.Compute the approximate average silhouette coefficient of the input clustering and assign it to a variable approxSilhFull. (Hint: to do so, you 	can first transform the RDD fullClustering by mapping each element (point, clusterID) of fullClustering to the approximate silhouette 	coefficient of 'point' computed as explained here exploiting the sample, and taking the average of all individual approximate silhouette 	coefficients). 
    5.Compute (sequentially) the exact silhouette coefficient of the clusteringSample and assign it to a variable exactSilhSample.
    6.Print the following values: (a) value of approxSilhFull, (b) time to compute approxSilhFull (Step 4),  (c) value of exactSilhSample, (d) time 	to compute exactSilhSample (Step 5). Times must be in ms. Use the following output format

Report:
	1.Table given in this word file with the results of the experiments indicated in the document.