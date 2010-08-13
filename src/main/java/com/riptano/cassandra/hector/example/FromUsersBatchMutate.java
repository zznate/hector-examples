package com.riptano.cassandra.hector.example;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.service.BatchMutation;
import me.prettyprint.cassandra.service.CassandraClient;
import me.prettyprint.cassandra.service.CassandraClientPool;
import me.prettyprint.cassandra.service.CassandraClientPoolFactory;
import me.prettyprint.cassandra.service.Keyspace;
import me.prettyprint.cassandra.utils.StringUtils;

import org.apache.cassandra.thrift.Column;
import org.apache.commons.lang.RandomStringUtils;

/**
 * Uses BatchMutation from a use case off of hector-users@googlegroups.com
 * Original author: Shamik Bandopadhyay
 *  
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.vrvm.cassandra.hector.example.FromUsersBatchMutate"
 * 
 * @author zznate
 *
 */
public class FromUsersBatchMutate {

    public static void main(String[] args) throws Exception {

        CassandraClientPool pool = CassandraClientPoolFactory.INSTANCE.get();
        CassandraClient client = pool.borrowClient("localhost", 9160);
        Keyspace keyspace = null;
        try {
            keyspace = client.getKeyspace("Keyspace1");

            List<String> columnFamilies = Arrays.asList("Standard1");
            BatchMutation batchMutation = new BatchMutation();
            int counter = 0;
            for (int i = 0; i <= 25000; i++) {
                for (int j = 0; j < 5; j++) {
                    Column lang = new Column(StringUtils.bytes("language_" + j),
                            StringUtils.bytes("English"),
                            keyspace.createTimestamp());
                    String key = String.format("%d_key_%s", counter, RandomStringUtils.randomAlphanumeric(6));
                    batchMutation.addInsertion(key, columnFamilies, lang);
                    Column query = new Column(StringUtils.bytes("query_" + j),
                            StringUtils.bytes("search for Auto CAD_" +
                                    i), keyspace.createTimestamp());
                    batchMutation.addInsertion(key,
                            columnFamilies, query);
                    Column url = new Column(StringUtils.bytes("url_" +
                            j),
                            StringUtils.bytes("www.autodesk.com"),
                            keyspace.createTimestamp());
                    batchMutation.addInsertion(key,
                            columnFamilies, url);
                    Column authorCol = new Column(StringUtils.bytes("position_" + j),
                            StringUtils.bytes(String.valueOf(i)),
                            keyspace.createTimestamp());
                    batchMutation.addInsertion(key,
                            columnFamilies, authorCol);
                    Column userId = new Column(StringUtils.bytes("userId_" + j),
                            StringUtils.bytes("bandops_" + i),
                            keyspace.createTimestamp());
                    batchMutation.addInsertion(key,
                            columnFamilies, userId);
                }
                
                if (i % 500 == 0) {
                    System.out.println(String.format("current counter %1$d with 'i' of %2$d", counter, i));                    
                    keyspace.batchMutate(batchMutation);
                    batchMutation = new BatchMutation();
                }

                counter++;
            }
            keyspace.batchMutate(batchMutation);
        } finally {
            pool.releaseClient(keyspace.getClient());
        }
    }
}
