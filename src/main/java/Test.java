import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Test {
    public static void main(String[] args) {

        Logger.getLogger("org.apache").setLevel(Level.ERROR);

        SparkConf conf= new SparkConf().setAppName("Testing").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        // Example: Parallelized Collections
        List<Integer> list =  Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> distData = jsc.parallelize(list);
        Integer sum = distData.reduce((a, b) -> a + b);
        System.out.printf("Parallel Sum: %s%n", sum);
    }
}
