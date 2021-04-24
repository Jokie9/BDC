import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;
import java.util.ArrayList;

import java.io.IOException;
import java.util.*;

// GROUP 22 : FEDERICA VETTOR, GABRIELE ZANATTA


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
        JavaPairRDD<Vector,Integer> fullClustering = sc.textFile(args[0]).mapToPair(x -> strToTuple(x));
        // Read number of clusters
        int k = Integer.parseInt(args[2]);
        // Read sample size of cluster
        int t = Integer.parseInt(args[3]);


        // SET GLOBAL VARIABLES
        //number of partitions
        int p = 5;
        int[] clusterSizes = new int[p];
        Broadcast<int[]> sharedClusterSizes = sc.broadcast(clusterSizes);
        Broadcast<ArrayList<Tuple2<Vector, Integer>>> clusteringSample = sc.broadcast();
        int exactSilhSample = 0;
        int approxSilhFull = 0;

        // Subdivide RDD into p random partitions
        JavaPairRDD<Vector, Integer> RawData = fullClustering.repartition(p).cache();

        //SAVE CLUSTERS SIZE


        //EXTRACT SAMPLE OF THE INPUT CLUSTERING


        //APPROXIMATE AVERAGE SILHOUETTE COEFFICIENT
        long startA = System.currentTimeMillis();
        //code
        long endA = System.currentTimeMillis();




        //EXACT AVERAGE SILHOUETTE COEFFICIENT
        long startE = System.currentTimeMillis();
        //code
        long endE = System.currentTimeMillis();


        //TIME
        long timeExactSilhSample = (endE-startE);
        long timeApproxSilhFull = (endA-startA);

        // PRINTING THE OUTPUT
        System.out.println("Value of approxSilhFull:" + approxSilhFull +"\n");
        System.out.println("Time to compute approxSilhFull:" + timeApproxSilhFull +"\n");
        System.out.println("Value of exactSilhSample:" + exactSilhSample +"\n");
        System.out.println("Time to compute exactSilhSample:" + timeExactSilhSample +"\n");
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
