package com.vrvm.cassandra.hector.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.utils.StringUtils;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

/**
 * Use get_range_slices to retrieve the keys without deserializing the columns.
 * For clear results, it's best to run this on an empty ColumnFamily.
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.vrvm.cassandra.hector.example.GetRangeSlicesKeysOnly"
 * 
 * @author zznate
 *
 */
public class GetRangeSlicesKeysOnly {

    public static void main(String[] args) throws Exception {
        
        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        CassandraClient client = pool.borrowClient("localhost", 9160);
        Keyspace keyspace = null;
        try {
            keyspace = client.getKeyspace("Keyspace1");
            // Insert 10 rows with 3 columns each of dummy data
            for (int i = 0; i < 10; i++) {
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
            sp.setColumn_names(new ArrayList<byte[]>());
            KeyRange keyRange = new KeyRange();
            keyRange.setCount(10);
            // prefix matching is okay here regardless of partitioner
            keyRange.setStart_key("fake_key_");
            keyRange.setEnd_key("");
            Map<String, List<Column>> results = keyspace.getRangeSlices(columnParent, sp, keyRange);
            Set<String> keySet = results.keySet();
            
            // setup slicing and predicate for the verification query
            SliceRange sliceRange = new SliceRange(new byte[0], new byte[0], false, 3);
            SlicePredicate slicePredicate = new SlicePredicate();
            slicePredicate.setSlice_range(sliceRange);            
            
            for (String key : keySet) {
                System.out.println("result key:" + key + " which should have null: " + results.get(key));
                System.out.println("|-- called directly via get_slice, the value is: " +keyspace.getSlice(key, columnParent, slicePredicate));
                System.out.println("|-- verify on CLI with: get Keyspace1.Standard1['" + key + "'] ");
            }

        } finally {
            pool.releaseClient(keyspace.getClient());
        }
    }
        
}
