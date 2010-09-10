package com.riptano.cassandra.hector.example;

import me.prettyprint.cassandra.model.HectorException;
import me.prettyprint.cassandra.model.KeyspaceOperator;
import me.prettyprint.cassandra.model.MutationResult;
import me.prettyprint.cassandra.model.Mutator;
import me.prettyprint.cassandra.model.OrderedRows;
import me.prettyprint.cassandra.model.RangeSlicesQuery;
import me.prettyprint.cassandra.model.Result;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

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

        KeyspaceOperator keyspaceOperator = HFactory.createKeyspaceOperator("Keyspace1", cluster);
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
            Result<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
            System.out.println("Result from rangeSlices query: " + result.toString());            
            
        } catch (HectorException he) {
            he.printStackTrace();
        }
    }
}
