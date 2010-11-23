package com.riptano.cassandra.hector.example;


import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

/**
 * Shows off the new ExecutionResult hierarchy  
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.riptano.cassandra.hector.example.ResultDetailsDemo"
 *  
 * @author zznate
 *
 */
public class ResultDetailsDemo {

    private static StringSerializer stringSerializer = StringSerializer.get();

    public static void main(String[] args) throws Exception {
        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");

        Keyspace keyspaceOperator = HFactory.createKeyspace("Keyspace1", cluster);
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
            // add 10 rows
            for (int i = 0; i < 10; i++) {            
                mutator.addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_0", "fake_value_0_" + i))
                .addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_1", "fake_value_1_" + i))
                .addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_2", "fake_value_2_" + i));            
            }
            MutationResult me = mutator.execute();
            System.out.println("MutationResult from 10 row insertion: " + me);
            
            RangeSlicesQuery<String, String, String> rangeSlicesQuery = 
                HFactory.createRangeSlicesQuery(keyspaceOperator, stringSerializer, stringSerializer, stringSerializer);
            rangeSlicesQuery.setColumnFamily("Standard1");            
            rangeSlicesQuery.setKeys("", "");
            rangeSlicesQuery.setRange("", "", false, 3);
            
            rangeSlicesQuery.setRowCount(10);
            QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
            System.out.println("Result from rangeSlices query: " + result.toString());   
            
            ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspaceOperator);
            columnQuery.setColumnFamily("Standard1").setKey("fake_key_0").setName("fake_column_0");
            QueryResult<HColumn<String, String>> colResult = columnQuery.execute();
            System.out.println("Execution time: " + colResult.getExecutionTimeMicro());
            System.out.println("CassandraHost used: " + colResult.getHostUsed());
            System.out.println("Query Execute: " + colResult.getQuery());
            
        } catch (HectorException he) {
            he.printStackTrace();
        }
        cluster.getConnectionManager().shutdown();
    }
}
