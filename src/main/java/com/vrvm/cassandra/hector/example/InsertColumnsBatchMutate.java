package com.vrvm.cassandra.hector.example;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.service.BatchMutation;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.utils.StringUtils;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;

/**
 * Uses BatchMutation to insert several rows under to same key. The
 * get_slice method is then used with a SliceRange and SlicePredicate
 * to retrieve the rows.
 *  
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.vrvm.cassandra.hector.example.InsertColumnsBatchMutate"
 * 
 * @author zznate
 *
 */
public class InsertColumnsBatchMutate {
    public static void main(String[] args) throws Exception {
        
        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        CassandraClient client = pool.borrowClient("localhost", 9160);
        Keyspace keyspace = null;
        try {
            keyspace = client.getKeyspace("Keyspace1");
            
            Column columnFirst = new Column(StringUtils.bytes("first"), 
                    StringUtils.bytes("John"), 
                    keyspace.createTimestamp());
            Column columnLast = new Column(StringUtils.bytes("last"), 
                    StringUtils.bytes("Smith"), 
                    keyspace.createTimestamp());
            Column columnMiddle = new Column(StringUtils.bytes("middle"), 
                    StringUtils.bytes("Q"), 
                    keyspace.createTimestamp());
            
            BatchMutation batchMutation = new BatchMutation();            
            batchMutation.addInsertion("jsmith", Arrays.asList("Standard1"), columnFirst);
            batchMutation.addInsertion("jsmith", Arrays.asList("Standard1"), columnLast);
            batchMutation.addInsertion("jsmith", Arrays.asList("Standard1"), columnMiddle);

            keyspace.batchMutate(batchMutation);
            
            ColumnParent columnParent = new ColumnParent("Standard1");
            SliceRange sliceRange = new SliceRange(new byte[0], new byte[0], false, 3);
            SlicePredicate slicePredicate = new SlicePredicate();
            slicePredicate.setSlice_range(sliceRange);
            List<Column> cols = keyspace.getSlice("jsmith", columnParent, slicePredicate);            
            
            System.out.println("Read from cassandra: " + cols);            
            System.out.println("Verify on CLI with:  get Keyspace1.Standard1['jsmith'] ");

        } finally {
            pool.releaseClient(keyspace.getClient());
        }
    }
}
