package com.vrvm.cassandra.hector.example;

import me.prettyprint.cassandra.utils.StringUtils;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

/**
 * A simple example showing what it takes to page over results using
 * get_range_slices.
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.vrvm.cassandra.hector.example.PaginateGetRangeSlices"
 * 
 * @author zznate
 *
 */
public class PaginateGetRangeSlices {
    public static void main(String[] args) throws Exception {
        
        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        CassandraClient client = pool.borrowClient("localhost", 9160);
        Keyspace keyspace = null;
        try {
            keyspace = client.getKeyspace("Keyspace1");
            // Insert 20 rows with 3 columns each of dummy data
            for (int i = 0; i < 20; i++) {
                ColumnPath cp = new ColumnPath("Standard1");
                cp.setColumn(StringUtils.bytes("fake_column_0"));
                keyspace.insert("fake_key_"+i, cp, StringUtils.bytes("fake_value_0_" + i));
                
                cp.setColumn(StringUtils.bytes("fake_column_1"));                
                keyspace.insert("fake_key_"+i, cp, StringUtils.bytes("fake_value_1_" + i));
                
                cp.setColumn(StringUtils.bytes("fake_column_2"));
                keyspace.insert("fake_key_"+i, cp, StringUtils.bytes("fake_value_2_" + i));
            }
            
            ColumnParent columnParent = new ColumnParent("Standard1");                
            SlicePredicate sp = new SlicePredicate();            
            SliceRange sliceRange = new SliceRange(new byte[0], new byte[0], false, 3);
            sp.setSlice_range(sliceRange);
            
            KeyRange keyRange = new KeyRange();
            keyRange.setCount(11);            
            keyRange.setStart_key("");
            keyRange.setEnd_key("");
            
            Map<String, List<Column>> results = keyspace.getRangeSlices(columnParent, sp, keyRange);

            Set<String> keySet = results.keySet();
            String last = new ArrayDeque<String>(keySet).peekLast();
            keySet.remove(last);
            
            System.out.println("first page:");
            for (String key : keySet) {
                System.out.println("result key:" + key + " results: " + results.get(key));
            }
            // 
            keyRange.setStart_key(last);            
            results = keyspace.getRangeSlices(columnParent, sp, keyRange);
            keySet = results.keySet();            
            
            System.out.println("second page:");
            for (String key : keySet) {
                System.out.println("result key:" + key + " results: " + results.get(key));
            }
            

        } finally {
            pool.releaseClient(keyspace.getClient());
        }
    }

}
