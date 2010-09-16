package com.riptano.cassandra.hector.example;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.BatchMutation;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.Cluster;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.utils.StringUtils;
import me.prettyprint.hector.api.factory.HFactory;

import org.apache.cassandra.thrift.Clock;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.SlicePredicate;

/**
 * Uses BatchMutation object to delete 2 of the 3 rows inserted. Uses the low-level API
 * directly to give an example of what is happening under Mutator class.
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.riptano.cassandra.hector.example.DeleteBatchMutate"
 * 
 * @author zznate
 *
 */
public class DeleteBatchMutate {
    private static StringSerializer stringSerializer = StringSerializer.get();
    
    public static void main(String[] args) throws Exception {
        
        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");
        CassandraClient client = cluster.borrowClient();
        Keyspace keyspace = client.getKeyspace("Keyspace1");
        try {
            Clock clock = keyspace.createClock();
            Column columnFirst = new Column(StringUtils.bytes("first"),
                    StringUtils.bytes("John"),
                    clock);
            Column columnLast = new Column(StringUtils.bytes("last"),
                    StringUtils.bytes("Smith"),
                    clock);
            Column columnMiddle = new Column(StringUtils.bytes("middle"),
                    StringUtils.bytes("Q"),
                    clock);
            // create the batchMutation
            BatchMutation<String> batchMutation = new BatchMutation<String>(stringSerializer);            
            batchMutation.addInsertion("jsmith", Arrays.asList("Standard1"), columnFirst);
            batchMutation.addInsertion("jsmith", Arrays.asList("Standard1"), columnLast);
            batchMutation.addInsertion("jsmith", Arrays.asList("Standard1"), columnMiddle);
            keyspace.batchMutate(batchMutation);
            
            // create the SlicePredicate for verification and deletion
            SlicePredicate slicePredicate = new SlicePredicate();
            slicePredicate.addToColumn_names(StringUtils.bytes("last"));
            slicePredicate.addToColumn_names(StringUtils.bytes("middle"));
            
            List<Column> cols = keyspace.getSlice("jsmith", new ColumnParent("Standard1"), slicePredicate);
            System.out.println("Verify insertion of columns we are about to delete: " + cols);
            
            // re-init the BatchMutation for deletion
            batchMutation = new BatchMutation<String>(stringSerializer);
            
            Deletion deletion = new Deletion(keyspace.createClock());
            deletion.setPredicate(slicePredicate);            
            
                        
            batchMutation.addDeletion("jsmith", Arrays.asList("Standard1"), deletion);
            
            keyspace.batchMutate(batchMutation);
            
            System.out.println("Deletion successful.");
            
            cols = keyspace.getSlice("jsmith", new ColumnParent("Standard1"), slicePredicate);
            System.out.println("Verify deletion of columns (should have an empty result): " + cols);
            
            
            System.out.println("Verify on CLI with:  get Keyspace1.Standard1['jsmith'] ");
            System.out.println("Should only return the 'first' column.");
        } finally {
            // release off the keyspace-ref in case of failover
            cluster.releaseClient(keyspace.getClient());
        }
        
    }
}
