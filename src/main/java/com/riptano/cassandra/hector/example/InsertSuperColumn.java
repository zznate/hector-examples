package com.riptano.cassandra.hector.example;

import java.util.Arrays;
import java.util.Iterator;


import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.utils.StringUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HSuperColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SuperColumnQuery;

import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.SuperColumn;

/**
 * Like {@link InsertSingleColumn} except with a SuperColumn 
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.riptano.cassandra.hector.example.InsertSuperColumn"
 * 
 * @author zznate
 *
 */
public class InsertSuperColumn {
    
    private static StringSerializer stringSerializer = StringSerializer.get();
    
    public static void main(String[] args) throws Exception {
        
        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");

        Keyspace keyspaceOperator = HFactory.createKeyspace("Keyspace1", cluster);
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
            mutator.insert("billing", "Super1", HFactory.createSuperColumn("jsmith", 
                    Arrays.asList(HFactory.createStringColumn("first", "John")), 
                    stringSerializer, stringSerializer, stringSerializer));
            
            SuperColumnQuery<String, String, String, String> superColumnQuery = 
                HFactory.createSuperColumnQuery(keyspaceOperator, stringSerializer, stringSerializer, 
                        stringSerializer, stringSerializer);
            superColumnQuery.setColumnFamily("Super1").setKey("billing").setSuperName("jsmith");

            QueryResult<HSuperColumn<String, String, String>> result = superColumnQuery.execute();

            System.out.println("Read HSuperColumn from cassandra: " + result.get());            
            System.out.println("Verify on CLI with:  get Keyspace1.Super1['billing']['jsmith'] ");
            
        } catch (HectorException e) {
            e.printStackTrace();
        } 

    }
}
