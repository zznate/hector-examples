package com.vrvm.cassandra.hector.example;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.utils.StringUtils;

import org.apache.cassandra.thrift.ColumnPath;

/**
 * Deletes the "first" Column for the key "jsmith" from Standard1 ColumnFamily.
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.vrvm.cassandra.hector.example.DeleteSingleColumn"
 * 
 * @author zznate
 */
public class DeleteSingleColumn {
    
    public static void main(String[] args) throws Exception {
        
        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        CassandraClient client = pool.borrowClient("localhost", 9160);
        Keyspace keyspace = null;
        try {
            keyspace = client.getKeyspace("Keyspace1");
            ColumnPath columnPath = new ColumnPath("Standard1");
            // leave out the line below to delete all columns for the "jsmith" key
            columnPath.setColumn(StringUtils.bytes("first"));
            keyspace.remove("jsmith", columnPath);
            
            System.out.println("Deletion successful");
            System.out.println("Verify on CLI with:  get Keyspace1.Standard1['jsmith'] ");
            System.out.println("Should return 0 results.");
        } finally {
            // return client to pool. do it in a finally block to make sure it's executed
            pool.releaseClient(keyspace.getClient());
        }
    }
}
