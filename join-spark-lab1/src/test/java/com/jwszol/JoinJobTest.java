package com.jwszol;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by kubaw on 29/05/17.
 */
public class JoinJobTest {
    private static final long serialVersionUID = 1L;
    private transient JavaSparkContext sc;

    @Before
    public void setUp() {
        sc = new JavaSparkContext("local", "SparkJoinsTest");
    }

    @After
    public void tearDown() {
        if (sc != null){
            sc.stop();
        }
    }

    @Test
    public void testExampleJob() {
        JoinJob job = new JoinJob(sc);
        JavaPairRDD<Integer, String> results = job.joinData("./src/main/resources/transactions.txt", "./src/main/resources/users.txt");

        System.out.println(results.collect().get(0)._1);
        System.out.println(results.collect().get(0)._2);
        System.out.println(results.collect().get(1)._1);
        System.out.println(results.collect().get(1)._2);

    }
}
