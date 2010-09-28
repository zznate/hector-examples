package com.riptano.cassandra.hector.example;


import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;

/**
 * Use get_range_slices to retrieve the keys without deserializing the columns.
 * For clear results, it's best to run this on an empty ColumnFamily.
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.riptano.cassandra.hector.example.GetRangeSlicesKeysOnly"
 * 
 * @author zznate
 *
 */
public class GetRangeSlicesKeysOnly {

    private static StringSerializer stringSerializer = StringSerializer.get();
    
    public static void main(String[] args) throws Exception {
        
        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");

        Keyspace keyspaceOperator = HFactory.createKeyspace("Keyspace1", cluster);
                
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);

            for (int i = 0; i < 5; i++) {            
                mutator.addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_0", "fake_value_0_" + i))
                .addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_1", "fake_value_1_" + i))
                .addInsertion("fake_key_" + i, "Standard1", HFactory.createStringColumn("fake_column_2", "fake_value_2_" + i));            
            }
            mutator.execute();
            
            RangeSlicesQuery<String, String, String> rangeSlicesQuery = 
                HFactory.createRangeSlicesQuery(keyspaceOperator, stringSerializer, stringSerializer, stringSerializer);
            rangeSlicesQuery.setColumnFamily("Standard1");            
            rangeSlicesQuery.setKeys("fake_key_", "");
            //rangeSlicesQuery.setReturnKeysOnly();
            
            rangeSlicesQuery.setRowCount(5);
            QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
            OrderedRows<String, String, String> orderedRows = result.get();            
            
            Row<String,String,String> lastRow = orderedRows.peekLast();

            System.out.println("Contents of rows: \n");                       
            for (Row<String, String, String> r : orderedRows) {
                System.out.println("   " + r);
            }
            
            
        } catch (HectorException he) {
            he.printStackTrace();
        }
    }
        
}
