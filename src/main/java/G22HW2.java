import com.twitter.chill.Tuple1LongSerializer;
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


        Random r = new Random();

        Broadcast<ArrayList<Tuple2<Integer, Long>>> sharedClusterSizes = sc.broadcast(new ArrayList<Tuple2<Integer, Long>>());
        Broadcast<List<Tuple2<Vector, Integer>>> clusteringSample;

        float exactSilhSample = 0;
        float approxSilhFull = 0;

        // Subdivide RDD into p random partitions
        JavaPairRDD<Vector, Integer> data = fullClustering.repartition(p).cache();

        //SAVE CLUSTERS SIZE
        data.values().countByValue().forEach((key,value) -> sharedClusterSizes.value().add(new Tuple2<>(key, value)));
        sharedClusterSizes.value().forEach(element ->{
            System.out.println(element);
        });

        //EXTRACT SAMPLE OF THE INPUT CLUSTERING
        temp = fullClustering.flatMapToPair(element -> {
            ArrayList<Tuple2<Vector, Integer>> array = new ArrayList();
            Long size = sharedClusterSizes.value().get(element._2())._2();
            Double f = r.nextDouble();

            if( f < ((double)t/size))
                array.add(element);

            return array.iterator();
        });
        clusteringSample = sc.broadcast(temp.collect());

        System.out.println("Full sample size = "+clusteringSample.getValue().size()+" \t Max sample size = "+t*k);


        JavaPairRDD<Integer, Vector> temp2 = fullClustering.flatMapToPair( element -> {
            ArrayList<Tuple2<Integer, Vector>> array = new ArrayList();
            array.add(new Tuple2<>(element._2(), element._1()));
            return array.iterator();
        })
        .groupByKey()
        .flatMapToPair(element -> {
            ArrayList<Tuple2<Integer, Vector>> array = new ArrayList();
            Long size = sharedClusterSizes.value().get(element._1())._2();
            Float ti = Float.min(size, t);

            if( ti == Float.parseFloat(size.toString())) {
                for (Vector e : element._2()) {
                    array.add(new Tuple2<>(element._1(), e));
                }
            }else{
                for (Vector e : element._2()) {
                    Float f = r.nextFloat();
                    if (f < (ti / size)) {
                        array.add(new Tuple2<>(element._1(), e));
                    }
                }
            }
            return array.iterator();
        });


        //APPROXIMATE AVERAGE SILHOUETTE COEFFICIENT
        long startA = System.currentTimeMillis();
        //code
       /* approxParamR1 = clusteringSample
                //R1 Map Phase: compute sum for ap and bp for each point
                .flatMapValues((element) -> {
                    ArrayList<Tuple2<Vector, Tuple2<Float, Float>>> pairs = new ArrayList<>(); //pairs (point, sum_ap, sum_bp )
                    Iterator<Tuple2<Vector, Integer>> p = element.iterator();
                    float sumA = 0;
                    float sumB= 0;
                    while (p.hasNext())
                    {
                        Iterator<Tuple2<Vector, Integer>> d = element.iterator();
                        while (d.hasNext()) {
                            if (p.next()._2().equals(d.next()._2()))
                                sumA = (float) (sumA + Vectors.sqdist(p.next()._1(), d.next()._1()));
                            //manca sumB che è il minimo della somma delle distanze tra p e tutti gli altri punti per ogni cluster
                        }
                        pairs.add(new Tuple2<Vector, Tuple2<Float, Float>>(p.next()._1(), sumA, sumB));
                    }
                    return pairs.iterator();
                });
                //R1 Reduce: compute ap and bp
                .flatMapValues((coef) -> {
                    //ap= 1/|Ci| * |Ci|/t * sum(vectors.sqdist(p,ti))   for each ti in sample for Ci
                    //bp= min ( 1/|Cj| * |Cj|/t * sum(vectors.sqdist(p,ti)) )  for each ti in sample for Cj different to Ci

                 });

        approxParamR2 = approxParamR1
                //R2 Map: empty
                //R2 Reduce: for each point compute sp
                .flatMapValues((sil) -> {
                    float approxSil = 0;
                    if (ap>bp)
                        approxSil = (bp-ap) / ap;
                    else
                        approxSil = (bp-ap)/ bp;

                    pairs.add(new Tuple2<>(point, approxSil));
                });


                //compute total approx Silhouette
                approxSilhFull = sumOfSilh/(t*k);
    */
        //end code
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

