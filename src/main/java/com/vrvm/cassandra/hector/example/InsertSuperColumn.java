package com.vrvm.cassandra.hector.example;

import java.util.Iterator;

import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.utils.StringUtils;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.SuperColumn;

/**
 * Like {@link InsertSingleColumn} except with a SuperColumn 
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.vrvm.cassandra.hector.example.InsertSuperColumn"
 * 
 * @author zznate
 *
 */
public class InsertSuperColumn {
    public static void main(String[] args) throws Exception {
        
        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        CassandraClient client = pool.borrowClient("localhost", 9160);
        Keyspace keyspace = null;
        try {
            keyspace = client.getKeyspace("Keyspace1");
            
            ColumnPath columnPath = new ColumnPath("Super1");
            columnPath.setColumn(StringUtils.bytes("first"));
            columnPath.setSuper_column(StringUtils.bytes("jsmith"));
            keyspace.insert("billing", columnPath, StringUtils.bytes("John"));            
                        
            SuperColumn sc = keyspace.getSuperColumn("billing", columnPath);            
            Iterator<Column> columnIterator = sc.getColumnsIterator();
            
            while(columnIterator.hasNext()) {
                Column column = columnIterator.next();
                System.out.println("Read from cassandra: sc: "
                        + StringUtils.string(sc.getName())
                        + " col:" 
                        + StringUtils.string(column.getName()) 
                        + " val:" + StringUtils.string(column.getValue()));
            }
            System.out.println("Verify on CLI with:  get Keyspace1.Super1['billing']['jsmith'] ");

        } finally {
            pool.releaseClient(keyspace.getClient());
        }
    }
}
