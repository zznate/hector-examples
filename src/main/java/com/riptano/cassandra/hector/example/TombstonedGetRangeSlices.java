package com.riptano.cassandra.hector.example;

import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.model.ColumnSlice;
import me.prettyprint.cassandra.model.HectorException;
import me.prettyprint.cassandra.model.KeyspaceOperator;
import me.prettyprint.cassandra.model.Mutator;
import me.prettyprint.cassandra.model.OrderedRows;
import me.prettyprint.cassandra.model.RangeSlicesQuery;
import me.prettyprint.cassandra.model.Result;
import me.prettyprint.cassandra.model.Row;
import me.prettyprint.cassandra.model.SliceQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Cluster;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.utils.StringUtils;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

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

        KeyspaceOperator keyspaceOperator = HFactory.createKeyspaceOperator("Keyspace1", cluster);
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
            Result<OrderedRows<String, String, String>> result = rangeSlicesQuery.execute();
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
                
                Result<ColumnSlice<String, String>> r = q.execute();
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
