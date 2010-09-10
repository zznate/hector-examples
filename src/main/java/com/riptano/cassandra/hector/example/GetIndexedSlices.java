package com.riptano.cassandra.hector.example;

import me.prettyprint.cassandra.model.HectorException;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.model.IndexedSlicesQueryTest;
import me.prettyprint.cassandra.model.KeyspaceOperator;
import me.prettyprint.cassandra.model.Mutator;
import me.prettyprint.cassandra.model.OrderedRows;
import me.prettyprint.cassandra.model.Result;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * Shows off the usage of the shiny new Column indexing feature via
 * get_indexed_slices.
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.riptano.cassandra.hector.example.GetIndexedSlices"
 * 
 * @author zznate
 *
 */
public class GetIndexedSlices {

    private static StringSerializer stringSerializer = StringSerializer.get();
    
    public static void main(String[] args) throws Exception {
        
        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");

        KeyspaceOperator keyspaceOperator = HFactory.createKeyspaceOperator("Keyspace1", cluster);
                
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, stringSerializer);
            
            for (int i = 0; i < 20; i++) {            
                mutator.addInsertion("fake_key_" + i, "Indexed1", HFactory.createStringColumn("fake_column_0", "fake_value_0_" + i))
                .addInsertion("fake_key_" + i, "Indexed1", HFactory.createStringColumn("fake_column_1", "fake_value_1_" + i))
                .addInsertion("fake_key_" + i, "Indexed1", HFactory.createStringColumn("fake_column_2", "fake_value_2_" + i))
                .addInsertion("fake_key_" + i, "Indexed1", HFactory.createColumn("birthdate",new Long(i%5),stringSerializer,LongSerializer.get()));
            }
            mutator.execute();
     
            IndexedSlicesQuery<String, String, Long> indexedSlicesQuery = 
                HFactory.createIndexedSlicesQuery(keyspaceOperator, stringSerializer, stringSerializer, LongSerializer.get());
            indexedSlicesQuery.addEqualsExpression("birthdate", 4L);
            indexedSlicesQuery.setColumnNames("birthdate","fake_column_0");
            indexedSlicesQuery.setColumnFamily("Indexed1");
            indexedSlicesQuery.setStartKey("");
            
            Result<OrderedRows<String, String, Long>> result = indexedSlicesQuery.execute();
            
            System.out.println("The results should only have 4 entries: " + result.get());
            
        } catch (HectorException he) {
            he.printStackTrace();
        }
    }
}
