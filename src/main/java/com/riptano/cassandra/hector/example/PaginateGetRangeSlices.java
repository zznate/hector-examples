package com.riptano.cassandra.hector.example;

import me.prettyprint.cassandra.model.HectorException;
import me.prettyprint.cassandra.model.KeyspaceOperator;
import me.prettyprint.cassandra.model.Mutator;
import me.prettyprint.cassandra.model.OrderedRows;
import me.prettyprint.cassandra.model.RangeSlicesQuery;
import me.prettyprint.cassandra.model.Result;
import me.prettyprint.cassandra.model.Row;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * A simple example showing what it takes to page over results using
 * get_range_slices.
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.riptano.cassandra.hector.example.PaginateGetRangeSlices"
 * 
 * @author zznate
 *
 */
public class PaginateGetRangeSlices {
    
    private static StringSerializer stringSerializer = StringSerializer.get();
    
    public static void main(String[] args) throws Exception {
        
        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");

        KeyspaceOperator keyspaceOperator = HFactory.createKeyspaceOperator("Keyspace1", cluster);
                
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);

            for (int i = 0; i < 20; i++) {            
                mutator.addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_0", "fake_value_0_" + i))
                .addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_1", "fake_value_1_" + i))
                .addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_2", "fake_value_2_" + i));            
            }
            mutator.execute();
            
            RangeSlicesQuery<String, String, String> rangeSlicesQuery = 
                HFactory.createRangeSlicesQuery(keyspaceOperator, stringSerializer, stringSerializer, stringSerializer);
            rangeSlicesQuery.setColumnFamily("Standard1");            
            rangeSlicesQuery.setKeys("", "");
            rangeSlicesQuery.setRange("", "", false, 3);
            
            rangeSlicesQuery.setRowCount(11);
            Result<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
            OrderedRows<String, String, String> orderedRows = result.get();
            
            
            Row<String,String,String> lastRow = orderedRows.peekLast();

            System.out.println("Contents of rows: \n");                       
            for (Row<String, String, String> r : orderedRows) {
                System.out.println("   " + r);
            }
            System.out.println("Should have 11 rows: " + orderedRows.getCount());
            
            rangeSlicesQuery.setKeys(lastRow.getKey(), "");
            orderedRows = rangeSlicesQuery.execute().get();
            
            System.out.println("2nd page Contents of rows: \n");
            for (Row<String, String, String> row : orderedRows) {
                System.out.println("   " + row);
            }
            
        } catch (HectorException he) {
            he.printStackTrace();
        }
    }

}
