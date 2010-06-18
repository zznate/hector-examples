package com.vrvm.cassandra.hector.example;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.utils.StringUtils;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;

/**
 * Inserts the value "John" under the Column "first" for the 
 * key "jsmith" in the Standard1 ColumnFamily 
 * 
 * To run this example from maven: 
 * mvn -e exec:java -Dexec.mainClass="com.vrvm.cassandra.hector.example.InsertSingleColumn"
 * 
 * @author zznate
 *
 */
public class InsertSingleColumn {
    
    public static void main(String[] args) throws Exception {
        
        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        CassandraClient client = pool.borrowClient("localhost", 9160);
        Keyspace keyspace = null;
        try {
            keyspace = client.getKeyspace("Keyspace1");
            ColumnPath columnPath = new ColumnPath("Standard1");                                    
            columnPath.setColumn(StringUtils.bytes("first"));
            keyspace.insert("jsmith", columnPath, StringUtils.bytes("John"));
            
            Column col = keyspace.getColumn("jsmith", columnPath);
            String columnValue = StringUtils.string(col.getValue());
            
            System.out.println("Read from cassandra: " + columnValue);            
            System.out.println("Verify on CLI with:  get Keyspace1.Standard1['jsmith'] ");

        } finally {
            pool.releaseClient(keyspace.getClient());
        }
    }
}
