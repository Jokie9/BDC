// GROUP 22 : FEDERICA VETTOR, GABRIELE ZANATTA
import org.apache.avro.hadoop.io.AvroKeyValue;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import java.util.ArrayList;
import java.io.IOException;
import java.util.*;


public class G22HW2 {
    public static void main(String[] args) throws IOException {

            // CHECK INPUT PARAMETERS: path file, k, t
            if (args.length != 3) {
                throw new IllegalArgumentException("USAGE: file_path num_cluster sample_size_cluster");
            }

            // SPARK SETUP
            // SETUP WITH Master set to Local machine
            SparkConf conf = new SparkConf(true).setAppName("G22HW2").setMaster("local[*]");
            // SETUP WITHOUT Master set to Local machine
            //SparkConf conf = new SparkConf(true).setAppName("G22HW2");

            JavaSparkContext sc = new JavaSparkContext(conf);
            sc.setLogLevel("WARN");

            // INPUT READING
            // Read input file
            JavaPairRDD<Vector,Integer> data = sc.textFile(args[0]).mapToPair(x -> strToTuple(x));
            // Read number of clusters
            int k = Integer.parseInt(args[1]);
            // Read sample size of cluster
            int t = Integer.parseInt(args[2]);


            // SET GLOBAL VARIABLES
            //rdd
            JavaPairRDD<Vector, Tuple2<Float, Float>> approxParamR1;
            JavaPairRDD<Vector, Float> approxParamR2;
            JavaPairRDD<Vector, Float> approxSilh;
            JavaPairRDD<Vector, Integer> temp;
            //variables
            int p = 8;  //number of partitions
            Double exactSilhSample = 0d;  //exact silhouette of the sample
            Double approxSilhFull = 0d;   //approximation silhouette of the full clustering
            Double clusterSampleSize = 0d;//size of cluster sample
            //broadcast variable
            Broadcast<ArrayList<Tuple2<Integer, Long>>> sharedClusterSizes = sc.broadcast(new ArrayList<Tuple2<Integer, Long>>());
            Broadcast<List<Tuple2<Vector, Integer>>> clusteringSample;

            // SUBDIVIDE INTO p RANDOM PARTITIONS
            JavaPairRDD<Vector, Integer> fullClustering = data.repartition(p).cache();

            //SAVE CLUSTERS SIZE
            fullClustering.values().countByValue().forEach((key,value) -> sharedClusterSizes.value().add(new Tuple2<>(key, value)));
            //sort the order of the clusters
            sharedClusterSizes.value().sort((o1, o2) -> {
                if(o1._1() < o2._1())
                    return -1;
                else
                    return 1;
            });

            //EXTRACT SAMPLE OF THE INPUT CLUSTERING
            //add randomly the point of each clusters in the array of sample
            clusteringSample = sc.broadcast(fullClustering.mapPartitionsToPair(element -> {
                ArrayList<Tuple2<Vector, Integer>> array = new ArrayList();
                while(element.hasNext()){
                    Tuple2<Vector, Integer> tuple = element.next();
                    Long size = sharedClusterSizes.value().get(tuple._2())._2();
                    Random r = new Random();
                    Double f = r.nextDouble();
                    if( f < ((double)t/size))
                        array.add(tuple);
                }
                return array.iterator();
            }).collect());

            //APPROXIMATE AVERAGE SILHOUETTE COEFFICIENT
            long startA = System.currentTimeMillis();
            //code
            JavaPairRDD<Integer, Double> temp3 = fullClustering.mapPartitionsToPair(element->{
                ArrayList<Tuple2<Integer, Double>> array = new ArrayList();
                //variables to compute the silhouette value for each point
                //arrays contain for each iteration (w.r.t. one point) the value of sum and fractions for each cluster
                Double[] sums = new Double[k];
                Double[] fractions = new Double[k];
                //for each point
                Double ap;
                Double bp;
                Double sp;
                //sum of all the silhoutte values
                Double silhoutte = 0d;
                Integer fullClusterPointID;
                Vector fullClusterPoint;
                //compute 1/min{t, clusterSize}
                for(int i = 0; i < k; i++)
                    fractions[i] = 1/Double.min(t, sharedClusterSizes.value().get(i)._2());
                //compute the sum of distances
                while(element.hasNext()){
                    Tuple2<Vector, Integer> tuple = element.next();
                    fullClusterPoint = tuple._1();
                    fullClusterPointID = tuple._2();
                    for(int i = 0; i<k; i++)
                        sums[i] = 0d;
                    Iterator<Tuple2<Vector, Integer>> clusteringSampleIter = clusteringSample.value().iterator();
                    while(clusteringSampleIter.hasNext()){
                        Tuple2<Vector, Integer> sampleTuple = clusteringSampleIter.next();
                        Vector sampleClusterPoint = sampleTuple._1();
                        Integer sampleClusterPointID = sampleTuple._2();
                        sums[sampleClusterPointID] = sums[sampleClusterPointID] + Vectors.sqdist(fullClusterPoint, sampleClusterPoint);
                    }
                    for(int i = 0; i<k; i++)
                        sums[i] = sums[i]*fractions[i];
                    //compute parameters 'a' and 'b' of the point
                    ap = sums[fullClusterPointID];
                    bp=Double.MAX_VALUE;
                    for(int i = 0;i < k; i++){
                        if(i != fullClusterPointID && bp > sums[i])
                            bp = sums[i];
                    }
                    //compute the silhouette of the point
                    sp = (bp - ap)/Double.max(ap,bp);
                    silhoutte = silhoutte + sp;
                }
                array.add(new Tuple2<>(0, silhoutte));
                return array.iterator();
            });

            //compute the average approximate Silhouette of full clustering
            Iterator<Tuple2<Integer, Double>> iter = temp3.collect().iterator();
            while(iter.hasNext()){
                approxSilhFull = approxSilhFull + iter.next()._2();
            }
            approxSilhFull = approxSilhFull/fullClustering.count();

            //end code
            long endA = System.currentTimeMillis();

            //SAVE THE SIZE OF SAMPLE CLUSTERS
            Double[] sampleClusterSize = new Double[k];
            for(int i=0; i<k; i++)
                sampleClusterSize[i]=0d;
            Iterator<Tuple2<Vector, Integer>> iteratorSample = clusteringSample.value().iterator();
            while(iteratorSample.hasNext()){
                Tuple2<Vector, Integer> tuple = iteratorSample.next();
                sampleClusterSize[tuple._2()]++;
            }

            //EXACT AVERAGE SILHOUETTE COEFFICIENT
            long startE = System.currentTimeMillis();
            //code
            Iterator<Tuple2<Vector,Integer>> iter1 = clusteringSample.value().iterator();
            while(iter1.hasNext()){
                Tuple2<Vector, Integer> tuple1 = iter1.next();
                Vector clusterPoint1 = tuple1._1();
                Integer clusterID1 = tuple1._2();
                Double[] sums = new Double[k];
                Double ap;
                Double bp;
                Double sp;

                for(int i = 0; i<k; i++)
                    sums[i] = 0d;
                //compute the sum of distances
                Iterator<Tuple2<Vector,Integer>> iter2 = clusteringSample.value().iterator();
                while(iter2.hasNext()){
                    Tuple2<Vector, Integer> tuple2 = iter2.next();
                    Vector clusterPoint2 = tuple2._1();
                    Integer clusterID2 = tuple2._2();
                    sums[clusterID2] = sums[clusterID2] + Vectors.sqdist(clusterPoint1, clusterPoint2);
                }
                //compute the fractions
                for(int i = 0; i<k; i++)
                    sums[i] = sums[i]/sampleClusterSize[i];
                //compute the parameters 'a' and 'b' for each point
                ap = sums[clusterID1];
                bp=Double.MAX_VALUE;
                for(int i = 0;i < k; i++){
                    if(i != clusterID1 && bp > sums[i])
                        bp = sums[i];
                }
                //compute the silhouette value for each point
                sp = (bp - ap)/Double.max(ap,bp);
                //compute the sum of all the silhouette values 'sp'
                exactSilhSample = exactSilhSample + sp;
            }
            for(int i = 0; i< k; i++)
                clusterSampleSize = clusterSampleSize + sampleClusterSize[i];
            //compute the average exact silhouette value of sample clustering
            exactSilhSample = exactSilhSample/clusterSampleSize;

            //end code
            long endE = System.currentTimeMillis();


            //TIME
            long timeExactSilhSample = (endE-startE);
            long timeApproxSilhFull = (endA-startA);

            // PRINTING THE OUTPUT
            System.out.println("Value of approxSilhFull = " + approxSilhFull);
            System.out.println("Time to compute approxSilhFull = " + timeApproxSilhFull +"ms");
            System.out.println("Value of exactSilhSample = " + exactSilhSample);
            System.out.println("Time to compute exactSilhSample = " + timeExactSilhSample +"ms");
        }



        public static Tuple2<Vector, Integer> strToTuple (String str){
            String[] tokens = str.split(",");
            double[] data = new double[tokens.length];
            for (int i = 0; i < tokens.length-1; i++) {
                data[i] = Double.parseDouble(tokens[i]);
            }
            Vector point = Vectors.dense(data);
            Integer cluster = Integer.valueOf(tokens[tokens.length-1]);
            Tuple2<Vector, Integer> pair = new Tuple2<>(point, cluster);
            return pair;
        }

}

