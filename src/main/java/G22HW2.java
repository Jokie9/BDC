
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
        int k = Integer.parseInt(args[1]);
        // Read sample size of cluster
        int t = Integer.parseInt(args[2]);


        // SET GLOBAL VARIABLES
        JavaPairRDD<Vector, Tuple2<Float, Float>> approxParamR1;
        JavaPairRDD<Vector, Float> approxParamR2;
        JavaPairRDD<Vector, Float> approxSilh;
        JavaPairRDD<Vector, Integer> temp;
        //number of partitions
        int p = 8;

        Broadcast<ArrayList<Tuple2<Integer, Long>>> sharedClusterSizes = sc.broadcast(new ArrayList<Tuple2<Integer, Long>>());
        Broadcast<List<Tuple2<Vector, Integer>>> clusteringSample;

        Double exactSilhSample = 0d;
        Double approxSilhFull = 0d;
        Long numberOfPoints;

        // Subdivide RDD into p random partitions
        JavaPairRDD<Vector, Integer> data = fullClustering.repartition(p).cache();

        //SAVE CLUSTERS SIZE
        data.values().countByValue().forEach((key,value) -> sharedClusterSizes.value().add(new Tuple2<>(key, value)));


        //EXTRACT SAMPLE OF THE INPUT CLUSTERING
        temp = fullClustering.flatMapToPair(element -> {
            ArrayList<Tuple2<Vector, Integer>> array = new ArrayList();
            Long size = sharedClusterSizes.value().get(element._2())._2();
            Random r = new Random();
            Double f = r.nextDouble();

            if( f < ((double)t/size))
                array.add(element);

            return array.iterator();
        });
        clusteringSample = sc.broadcast(temp.collect());


        JavaPairRDD<Integer, Tuple2<Integer, Vector>> temp2 = fullClustering.flatMapToPair( element -> {
            ArrayList<Tuple2<Integer, Vector>> array = new ArrayList();
            array.add(new Tuple2<>(element._2(), element._1()));
            return array.iterator();
        })
        .groupByKey()
        .flatMapToPair(element -> {
            ArrayList<Tuple2<Integer, Tuple2<Integer, Vector>>> array = new ArrayList();
            Long size = sharedClusterSizes.value().get(element._1())._2();
            Float ti = Float.min(size, t);
            Random r = new Random();


            if( ti == Float.parseFloat(size.toString())) {
                for (Vector e : element._2()) {
                    array.add(new Tuple2<>(0, new Tuple2<>(element._1(), e)));
                }
            }else{
                for (Vector e : element._2()) {
                    Float f = r.nextFloat();
                    if (f < (ti / size)) {
                        array.add(new Tuple2<>(0, new Tuple2<>(element._1(), e)));
                    }
                }
            }
            return array.iterator();
        });

        //APPROXIMATE AVERAGE SILHOUETTE COEFFICIENT
        long startA = System.currentTimeMillis();
        //code
        JavaPairRDD<Integer, Double> temp3 = temp2.groupByKey()
        .flatMapToPair(element->{
            ArrayList<Tuple2<Integer, Double>> array = new ArrayList();
            Double[] sums = new Double[k];
            Double[] fractions = new Double[k];
            Double ap;
            Double bp;
            Double sp;
            Integer cluster1;
            Vector v1;

            for(int i = 0; i < k; i++)
                fractions[i] = 1/Double.min(t, sharedClusterSizes.value().get(i)._2());

            for(Tuple2<Integer, Vector> tuple1 : element._2()){
                cluster1 = tuple1._1();
                v1 = tuple1._2();
                for(int i = 0; i<k; i++)
                    sums[i] = 0d;
                for(Tuple2<Integer, Vector> tuple2 : element._2()){
                    Integer cluster2 = tuple2._1();
                    Vector v2 = tuple2._2();
                    sums[cluster2] = sums[cluster2] + Vectors.sqdist(v1,v2);
                }
                for(int i = 0; i<k; i++)
                    sums[i] = sums[i]*fractions[i];

                ap = sums[cluster1];
                if(cluster1 == 0)
                    bp = sums[1];
                else
                    bp = sums[0];

                for(int i = 0;i < k; i++){
                    if(i != cluster1 && bp > sums[i])
                        bp = sums[i];
                }

                sp = (bp - ap)/Double.max(ap,bp);

                array.add(new Tuple2<>(0, sp));
            }

            return array.iterator();
        });

        numberOfPoints = temp3.count();
        List<Tuple2<Integer,Double>> list = temp3.collect();
        Iterator<Tuple2<Integer, Double>> iter = list.iterator();
        while(iter.hasNext()){
            Double d = iter.next()._2();
            approxSilhFull = approxSilhFull + d;
        }

        approxSilhFull = approxSilhFull/numberOfPoints;
        //end code
        long endA = System.currentTimeMillis();




        //EXACT AVERAGE SILHOUETTE COEFFICIENT
        long startE = System.currentTimeMillis();
        //code
        List<Tuple2<Vector, Integer>> fullList = fullClustering.collect();
        Iterator<Tuple2<Vector, Integer>> fullIter = fullList.iterator();

        Double[] sums = new Double[k];
        Double[] fractions = new Double[k];
        Double sp = 0d;
        for(int i = 0; i < k; i++)
            fractions[i] = 1/Double.min(t, sharedClusterSizes.value().get(i)._2());

        while(fullIter.hasNext()){
            Double ap;
            Double bp;
            Tuple2<Vector, Integer> tuple1 = fullIter.next();
            Integer cluster1 = tuple1._2();
            Vector v1 = tuple1._1();
            Iterator<Tuple2<Vector, Integer>> fIter = fullList.iterator();

            for(int i = 0; i<k; i++)
                sums[i] = 0d;

            while(fIter.hasNext()){
                Tuple2<Vector, Integer> tuple2 = fIter.next();
                Integer cluster2 = tuple2._2();
                Vector v2 = tuple2._1();
                sums[cluster2] = sums[cluster2] + Vectors.sqdist(v1,v2);
            }
            for(int i = 0; i<k; i++)
                sums[i] = sums[i]*fractions[i];

            ap = sums[cluster1];
            if(cluster1 == 0)
                bp = sums[1];
            else
                bp = sums[0];

            for(int i = 0;i < k; i++){
                if(i != cluster1 && bp > sums[i])
                    bp = sums[i];
            }

            sp = sp + ((bp - ap)/Double.max(ap,bp));

        }

        exactSilhSample = sp/fullList.size();

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

