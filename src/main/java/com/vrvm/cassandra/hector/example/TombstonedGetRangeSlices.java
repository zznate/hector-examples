package com.vrvm.cassandra.hector.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import me.prettyprint.cassandra.model.HectorException;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.utils.StringUtils;

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
 * mvn -e exec:java -Dexec.mainClass="com.vrvm.cassandra.hector.example.TombstonedGetRangeSlices"
 *  
 * @author zznate
 *
 */
public class TombstonedGetRangeSlices {

    public static void main(String[] args) throws Exception {
        
        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        CassandraClient client = pool.borrowClient("localhost", 9160);
        Keyspace keyspace = null;
        try {
            keyspace = client.getKeyspace("Keyspace1");
            // Insert 10 rows with 3 columns each of dummy data
            ColumnPath cp = new ColumnPath("Standard1");
            List<String> keySet = new ArrayList<String>(10);
            for (int i = 0; i < 10; i++) {          
                String key = "fake_key_" + i;
                cp.setColumn(StringUtils.bytes("fake_column_0"));
                keyspace.insert(key, cp, StringUtils.bytes("fake_value_0_" + i));
                
                cp.setColumn(StringUtils.bytes("fake_column_1"));                
                keyspace.insert(key, cp, StringUtils.bytes("fake_value_1_" + i));
                
                cp.setColumn(StringUtils.bytes("fake_column_2"));
                keyspace.insert(key, cp, StringUtils.bytes("fake_value_2_" + i));
                keySet.add(key);
            }
            cp.unsetColumn();
            // now delete the odd rows
            for (int i = 0; i < 10; i++) {
                if ( i % 2 == 0 ) continue;
                keyspace.remove("fake_key_"+i, cp);
            }
            
            ColumnParent columnParent = new ColumnParent("Standard1");                
            SlicePredicate sp = new SlicePredicate();
            sp.addToColumn_names(StringUtils.bytes("fake_column_"));
           
            Map<String, List<Column>> results = keyspace.multigetSlice(keySet, columnParent, sp);            
            
            // setup slicing and predicate for the verification query
            SliceRange sliceRange = new SliceRange(new byte[0], new byte[0], false, 3);
            SlicePredicate slicePredicate = new SlicePredicate();
            slicePredicate.setSlice_range(sliceRange);            
            
            // setup ColumnPath with the first column for verification
            ColumnPath verifyColumnPath = new ColumnPath("Standard1");
            verifyColumnPath.setColumn(StringUtils.bytes("fake_column_0"));
            
            // what various methods look like with tombstoned data
            for (String key : keySet) {
                int keyNum = Integer.valueOf(key.substring(9));
                System.out.println("+-----------------------------------");
                if ( keyNum % 2 == 0 ) {
                    System.out.println("| result key:" + key + " which should have values: " + results.get(key));    
                } else {
                    System.out.println("| TOMBSTONED result key:" + key + " has values: " + results.get(key));
                }
                
                System.out.println("|-- called directly via get_slice, the value is: " +keyspace.getSlice(key, columnParent, slicePredicate));
                try {
                    System.out.println("|-- try the first column via getColumn: " + keyspace.getColumn(key, verifyColumnPath));
                } catch (HectorException he) {
                    System.out.println("|-- try the first column via getColumn: [a NotFoundException was caught]");
                }
                System.out.println("|-- verify on CLI with: get Keyspace1.Standard1['" + key + "'] ");                
            }
            // 

        } finally {
            pool.releaseClient(keyspace.getClient());
        }
    }
}
