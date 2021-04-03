import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.io.IOException;
import java.util.*;

// GROUP 22 : FEDERICA VETTOR, GABRIELE ZANATTA

public class G22HW1 {

    public static void main(String[] args) throws IOException {

        // CHECK INPUT PARAMETERS: K, T, path file
        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: num_partitions num_products file_path");
        }

        // SPARK SETUP
        // SETUP WITH Master set to Local machine
        SparkConf conf = new SparkConf(true).setAppName("G22HW1").setMaster("local[*]");
        // SETUP WITHOUT Master set to Local machine
        //SparkConf conf = new SparkConf(true).setAppName("G22HW1");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // INPUT READING
        // Read number of partitions
        int K = Integer.parseInt(args[0]);
        // Read number of products
        int T = Integer.parseInt(args[1]);
        // Read input file and subdivide it into K random partitions
        JavaRDD<String> RawData = sc.textFile(args[2]).repartition(K).cache();

        // SET GLOBAL VARIABLES
        JavaPairRDD<String, Float> normalizedRating;
        JavaPairRDD<String, Float> maxNormRatings;

        //MAPREDUCE ALGORITHM
        normalizedRating = RawData
                //R1 MAP PHASE: create a Tuple2 < userID , Tuple2 < productID, rating > >
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(",");
                    // pairs -> (UserID, (ProductID, Rating))
                    ArrayList<Tuple2<String, Tuple2<String, Float>>> pairs = new ArrayList<>();
                    for(int i = 0; i < tokens.length; i = i+4){
                        pairs.add(new Tuple2<> (tokens[i+1], new Tuple2<>(tokens[i], Float.parseFloat(tokens[i+2]))));
                    }
                    return pairs.iterator();
                })
                //R1 REDUCE PHASE: return PairRDD with Tuple2< Product, normRating >
                .groupByKey()
                .flatMapToPair((element) -> {

                    ArrayList<Tuple2<String, Float>> pairs = new ArrayList<>();
                    Iterator<Tuple2<String, Float>> list = element._2().iterator();

                    // sumOfRatings = sum of ratings of a specific user
                    Float sumOfRatings = 0F;
                    // sumOfRatings = number of ratings of a specific user
                    Float numOfRatings = 0F;
                    Float avg;

                    while (list.hasNext()){

                        // tuple -> Tuple2 < ProductID, Rating >
                        Tuple2<String, Float> tuple = list.next();

                        Float rating = tuple._2();

                        sumOfRatings = sumOfRatings + rating;
                        numOfRatings = numOfRatings + 1F;
                    }

                    avg = sumOfRatings/numOfRatings;

                    list=element._2().iterator();

                    while (list.hasNext()){

                        // tuple -> Tuple2 < ProductID, Rating >
                        Tuple2<String, Float> tuple = list.next();

                        String productID = tuple._1();
                        // normRating = rating of the product - average rating of the user
                        Float normRating = tuple._2() - avg;

                        pairs.add(new Tuple2<>(productID, normRating));
                    }

                   return pairs.iterator();
                });

        maxNormRatings = normalizedRating
                //R2 MAP PHASE: propagate
                //R2 REDUCE PHASE: return PairRDD with Tuple2< Product, maxNormRating >
                .groupByKey()
                .flatMapToPair((element) -> {

                    ArrayList<Tuple2<String, Float>> pairs = new ArrayList<>();
                    Iterator<Float> list = element._2().iterator();

                    // max = set to -6F because minimum possible rating is -5F
                    Float max = -6F;

                    // calculate max
                    while (list.hasNext()){

                        Float f = list.next();
                        if( f > max)
                            max = f;
                    }

                    pairs.add(new Tuple2<>(element._1(), max));

                    return pairs.iterator();
                });

        List<Tuple2<Float, String>> list;
        list = maxNormRatings
                //R3 MAP PHASE: switch key with value
                .flatMapToPair( element -> {

                    ArrayList<Tuple2<Float, String>> pairs = new ArrayList<>();

                    pairs.add(new Tuple2<>(element._2(), element._1()));


                    return pairs.iterator();
                })
                //R3 REDUCE PHASE: sort in descending order and take the first T positions
                .sortByKey(false).take(T);


        // PRINTING THE OUTPUT: first T Product
        // FIRST LINE OF THE OUTPUT
        System.out.println("INPUT PARAMETERS: K="+K+" T="+T+" file="+args[2]+"\n");
        System.out.println("OUTPUT");
        // PRINTING THE FIRST T PRODUCT WITH THE LARGEST MAXIMUM NORMALIZED RATING (ONE PER LINE)
        list.forEach(data -> System.out.println("Product "+data._2() + " maxNormRating " + data._1()));
    }
}
