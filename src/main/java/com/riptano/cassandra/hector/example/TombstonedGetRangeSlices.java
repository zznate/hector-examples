package com.riptano.cassandra.hector.example;

import me.prettyprint.cassandra.serializers.StringSerializer;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceQuery;

/**
 * Shows what tombstoned data looks like in the result sets of 
 * get_range_slices and get_slice 
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.riptano.cassandra.hector.example.TombstonedGetRangeSlices"
 *  
 * @author zznate
 *
 */
public class TombstonedGetRangeSlices {
    
    private static StringSerializer stringSerializer = StringSerializer.get();
    // - add data from mutator
    // - delete the odd rows
    // - display results

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
            mutator.execute();
            mutator.discardPendingMutations();
            // delete the odd rows
            for (int i = 0; i < 10; i++) {
                if ( i % 2 == 0 ) continue;
                mutator.addDeletion("fake_key_"+i, "Standard1", null, stringSerializer);             
            }
            mutator.execute();
            
            RangeSlicesQuery<String, String, String> rangeSlicesQuery = 
                HFactory.createRangeSlicesQuery(keyspaceOperator, stringSerializer, stringSerializer, stringSerializer);
            rangeSlicesQuery.setColumnFamily("Standard1");            
            rangeSlicesQuery.setKeys("", "");
            rangeSlicesQuery.setRange("", "", false, 3);
            
            rangeSlicesQuery.setRowCount(10);
            QueryResult<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
            OrderedRows<String, String, String> orderedRows = result.get();
            for (Row<String, String, String> row : orderedRows) {
                int keyNum = Integer.valueOf(row.getKey().substring(9));
                System.out.println("+-----------------------------------");
                if ( keyNum % 2 == 0 ) {
                    System.out.println("| result key:" + row.getKey() + " which should have values: " + row.getColumnSlice());    
                } else {
                    System.out.println("| TOMBSTONED result key:" + row.getKey() + " has values: " + row.getColumnSlice());
                }
                SliceQuery<String, String, String> q = HFactory.createSliceQuery(keyspaceOperator, stringSerializer, stringSerializer, stringSerializer);
                q.setColumnFamily("Standard1");
                q.setRange("", "", false, 3);
                q.setKey(row.getKey());
                
                QueryResult<ColumnSlice<String, String>> r = q.execute();
                System.out.println("|-- called directly via get_slice, the value is: " +r);
                // For a tombstone, you just get a null back from ColumnQuery
                System.out.println("|-- try the first column via getColumn: " + HFactory.createColumnQuery(keyspaceOperator, 
                        stringSerializer, stringSerializer, stringSerializer).setColumnFamily("Standard1").setKey(row.getKey()).setName("fake_column_0").execute());
                
                System.out.println("|-- verify on CLI with: get Keyspace1.Standard1['" + row.getKey() + "'] ");
            }


        } catch (HectorException he) {
            he.printStackTrace();
        }

    }
}
