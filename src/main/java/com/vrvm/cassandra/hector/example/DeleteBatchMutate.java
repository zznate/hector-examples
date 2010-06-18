package com.vrvm.cassandra.hector.example;

import java.util.Arrays;

import me.prettyprint.cassandra.service.BatchMutation;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.utils.StringUtils;

import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.SlicePredicate;

/**
 * Uses BatchMutation object to delete 2 of the 3 rows inserted from the
 * {@link InsertColumnsBatchMutate} example, leaving only the "first" column.
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.vrvm.cassandra.hector.example.DeleteBatchMutate"
 * 
 * @author zznate
 *
 */
public class DeleteBatchMutate {
    public static void main(String[] args) throws Exception {
        
        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        CassandraClient client = pool.borrowClient("localhost", 9160);
        Keyspace keyspace = null;
        try {
            keyspace = client.getKeyspace("Keyspace1");
            
            SlicePredicate slicePredicate = new SlicePredicate();
            slicePredicate.addToColumn_names(StringUtils.bytes("last"));
            slicePredicate.addToColumn_names(StringUtils.bytes("middle"));
            Deletion deletion = new Deletion(keyspace.createTimestamp());
            deletion.setPredicate(slicePredicate);            
            
            BatchMutation batchMutation = new BatchMutation();            
            batchMutation.addDeletion("jsmith", Arrays.asList("Standard1"), deletion);
            
            keyspace.batchMutate(batchMutation);
            
            System.out.println("Deletion successful.");            
            System.out.println("Verify on CLI with:  get Keyspace1.Standard1['jsmith'] ");
            System.out.println("Should only return the 'first' column.");
        } finally {
            pool.releaseClient(keyspace.getClient());
        }
    }
}
