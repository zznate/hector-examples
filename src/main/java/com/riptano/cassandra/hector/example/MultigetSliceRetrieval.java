package com.riptano.cassandra.hector.example;


import java.util.ArrayList;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Use MultigetSliceQuery when you want to get the same columns from a known set of keys
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.riptano.cassandra.hector.example.MultigetSliceRetrieval"
 * 
 * @author zznate
 *
 */
public class MultigetSliceRetrieval {

    private static StringSerializer stringSerializer = StringSerializer.get();

    public static void main(String[] args) throws Exception {

        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");

        Keyspace keyspaceOperator = HFactory.createKeyspace("Keyspace1", cluster);

        try {
            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);

            for (int i = 0; i < 20; i++) {            
                mutator.addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_0", "fake_value_0_" + i))
                .addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_1", "fake_value_1_" + i))
                .addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_2", "fake_value_2_" + i));            
            }
            mutator.execute();
            
            MultigetSliceQuery<String, String, String> multigetSliceQuery = 
                HFactory.createMultigetSliceQuery(keyspaceOperator, stringSerializer, stringSerializer, stringSerializer);
            multigetSliceQuery.setColumnFamily("Standard1");            
            multigetSliceQuery.setKeys("fake_key_0", "fake_key_1","fake_key_2", "fake_key_3", "fake_key_4");

            // set null range for empty byte[] on the underlying predicate
            multigetSliceQuery.setRange(null, null, false, 3);
            System.out.println(multigetSliceQuery);

            QueryResult<Rows<String, String, String>> result = multigetSliceQuery.execute();
            Rows<String, String, String> orderedRows = result.get();
           
            System.out.println("Contents of rows: \n");                       
            for (Row<String, String, String> r : orderedRows) {
                System.out.println("   " + r);
            }
            System.out.println("Should have 5 rows: " + orderedRows.getCount());

            
        } catch (HectorException he) {
            he.printStackTrace();
        }
    }

}

