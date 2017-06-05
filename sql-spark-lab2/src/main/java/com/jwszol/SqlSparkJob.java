package com.jwszol;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Int;

import static org.apache.spark.sql.functions.col;

/**
 * Created by kubaw on 04/06/17.
 */


public class SqlSparkJob {

    private Dataset<Row> dfUsers = null;
    private Dataset<Row> dfTrans = null;
    
    private SparkSession spark = null;

    public SqlSparkJob(SparkSession spark){
        this.spark = spark;
        this.dfUsers = this.spark.read().option("delimiter","|").option("header","true").csv("./src/main/resources/users.txt");
        this.dfTrans = this.spark.read().option("delimiter","|").option("header","true").csv("./src/main/resources/transactions.txt");
    }

    public void getSchema(){
        dfUsers.printSchema();
    }

    public void getEmail(){
        dfUsers.select(col("email")).show();
    }

    public Dataset<Row> addValueToId(){
        // Register the DataFrame as a SQL temporary view
        Dataset<Row> tempdfUsers =  dfUsers.select(col("email"), col("id").plus(1).name("new_id"));
        tempdfUsers.registerTempTable("users2");

        Dataset<Row> sqldfUsers = this.spark.sql("SELECT * FROM users2");
        sqldfUsers.show();
        return sqldfUsers;
    }

    public void selectUsersGt2(Dataset<Row> users){
        users.createOrReplaceTempView("users3");

        Dataset<Row> limitedUsersList = this.spark.sql("SELECT * from users3 WHERE new_id > 2");
        limitedUsersList.show();
    }

    public void printTransSchema(){
        this.dfTrans.show();
    }

    public void joinData(){
        Dataset<Row> joinDs = this.dfTrans.join(dfUsers, this.dfTrans.col("user_id").equalTo(this.dfUsers.col("id")),"leftouter");
        joinDs.show();
    }

}
