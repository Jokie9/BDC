/*
ASSIGNMENT. You must develop a Spark program to identify the T best products from a review dataset of an online retailer.

DATA FORMAT: a review dataset is provided as a file with one review per row. A review consists of 4 comma-separated fields: ProductID (string), UserID (string),
Rating (integer in [1,5] represented as a real), Timestamp (integer).
An example of review is: B00005N7P0,AH2IFH762VY5U,5.0,1005177600

TASK: you must write a program GxxHW1.java (for Java users) or GxxHW1.py (for Python users), where xx is your two-digit group number, which receives in input two integers K and T,
and path to a file storing a review dataset, and does the following things:

1. Reads the input set of reviews into an RDD of strings called RawData (each review is read as a single string), and subdivides it into K partitions.

2. Transform the RDD RawData into an RDD of pairs (String,Float) called normalizedRatings, so that for each string of RawData representing a review (ProductID,UserID,Rating,Timestamp),
NormalizedRatings contains the pair (ProductID,NormRating), where NormRating=Rating-AvgRating and AvgRating is the average rating of all reviews by the user "UserID".
To accomplish this step you can safely assume that there are a few reviews for each user. Note that normalizedRatings may contain several pairs for the same product,
one for each existing review for that product!

3. Transform the RDD normalizedRatings into an RDD of pairs (String,Float) called maxNormRatings which,
for each ProductID contains exactly one pair (ProductID, MNR) where MNR is the maximum normalized rating of product "ProductID".
The maximum should be computed either using the reduceByKey method or the mapPartitionsToPair/mapPartitions method. (Hint: get inspiration from the WordCountExample program).

4. Print the T products with largest maximum normalized rating, one product per line. (Hint: use a combination of sortByKey and take methods.)
To test your program you can use file input_20K.csv, which contains 20000 reviews. The output on this dataset is shown in file output_20K.txt. For your homework adopt the same output format.

SUBMISSION INSTRUCTIONS. Each group must submit a single file (GxxHW1.java or GxxHW1.py depending on whether you are Java or Python users, where xx is your ID group).
Only one student per group must submit the files in Moodle Exam using the link provided in the Homework1 section.
Make sure that your code is free from compiling/run-time errors and that you use the file/variable names in the homework description, otherwise your score will be penalized.
If you have questions about the assignment, contact the teaching assistants (TAs) by email to bdc-course@dei.unipd.it .
The subject of the email must be "HW1 - Group xx", where xx is your ID group. If needed, a zoom meeting between the TAs and the group will be organized.
*/
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

// INPUT FILE: input_20K.csv
// OUTPUT FILE: output_20K.txt

public class G22HW1 {

    public static void main(String[] args) throws IOException {

        // CHECK INPUT PARAMETERS: K, T, path file
        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: num_partitions num_products file_path");
        }

        // SPARK SETUP
        SparkConf conf = new SparkConf(true).setAppName("G22HW1");
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
        String outputFileName = "output_20K.txt";
        JavaPairRDD<String, Long> count;


        //MAPREDUCE ALGORITHM
        count = RawData
                //R1 MAP PHASE: copy of RawData in (UserID, RATE)
                .flatMapToPair((document) -> {
                    String prod="";
                    String user="";
                    String[] tokens = document.split(",");
                    HashMap<String, Long> rev_prod = new HashMap<>();  //creo due map una che usa come chiave userID e uno il prodotto
                    HashMap<String, Long> rev_user = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        prod=token;
                        user=token;
                        rev_prod.put(prod, 1L + rev_prod.getOrDefault(token, 0L));
                        rev_user.put(, 1L + rev_user.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : reviews.entrySet()) {   //???????
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                //R1 REDUCE PHASE: compute avg for each UserID (no repetition)
                .mapPartitionsToPair((element) -> {
                    HashMap<String, Long> counts = new HashMap<>();
                    while (element.hasNext()){
                        Tuple2<String, Long> tuple = element.next();
                        counts.put(tuple._1(), tuple._2() + counts.getOrDefault(tuple._1(), 0L));
                    }
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                //R2 MAP PHASE: normalize (ProductID, NormRate)
                //usare .filter(f) per scegliere UserID e calcolare rate-avg in un ciclo 
                .mapPartitionsToPair((norm) -> {
                    HashMap<String, Long> normpair = new HashMap<>();
                    
                    }
                })
                //R2 REDUCE PHASE: compute max NormRate for each ProductID (no repetition ID)
                .groupByKey()
                .mapValues((rate) -> {
                    long max = 0;
                    for (long c : rate) {
                        if (c>max)
                                max=c;
                    }
                    return max;
                });

        //Sort pairs by value
        count=count.sortBy(pairs._2,false);


        // CREATION OF OUTPUT FILE: first T Product
        try {
            File outputFile = new File(outputFileName);
            if (outputFile.createNewFile()) {
                System.out.println("File created: " + outputFile.getName());
            } else {
                System.out.println("File already exists. It will be overwritten");
            }
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        try {
            //DELETING THE CONTENT OF THE FILE IF IT ALREADY EXISTS
            new FileWriter(outputFileName, false).close();
            FileWriter myWriter = new FileWriter(outputFileName, true);
            // FIRST LINE OF THE FILE
            myWriter.write("INPUT PARAMETERS: K="+K+" T="+T+" file="+args[2]+"\n");
            // WRITE HERE THE FIRST T PRODUCT WITH THE LARGEST MAXIMUM NORMALIZED RATING (ONE PER LINE)

            myWriter.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }


    }


}
