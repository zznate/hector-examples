package com.riptano.cassandra.hector.example;

import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;

/**
 * Shows off the usage of the shiny new Column indexing feature via
 * get_indexed_slices. Note the use of stacked index clauses on a column that is not 
 * indexed (birthmonth). All get_indexed_slices queries have to contain an EQ 
 * expression though. 
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

        Keyspace keyspace = HFactory.createKeyspace("Keyspace1", cluster);
                
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspace, stringSerializer);
            
            for (int i = 0; i < 24; i++) {            
                mutator.addInsertion("fake_key_" + i, "Indexed1", HFactory.createStringColumn("fake_column_0", "fake_value_0_" + i))
                .addInsertion("fake_key_" + i, "Indexed1", HFactory.createStringColumn("fake_column_1", "fake_value_1_" + i))
                .addInsertion("fake_key_" + i, "Indexed1", HFactory.createStringColumn("fake_column_2", "fake_value_2_" + i))
                .addInsertion("fake_key_" + i, "Indexed1", HFactory.createColumn("birthdate",new Long(1974+(i%2)),stringSerializer,LongSerializer.get()))
                .addInsertion("fake_key_" + i, "Indexed1", HFactory.createColumn("birthmonth",new Long(i%12),stringSerializer,LongSerializer.get()));
            }
            mutator.execute();
     
            IndexedSlicesQuery<String, String, Long> indexedSlicesQuery = 
                HFactory.createIndexedSlicesQuery(keyspace, stringSerializer, stringSerializer, LongSerializer.get());
            indexedSlicesQuery.addEqualsExpression("birthdate", 1975L);
            indexedSlicesQuery.addGtExpression("birthmonth", 6L);
            indexedSlicesQuery.addLtExpression("birthmonth", 8L);
            indexedSlicesQuery.setColumnNames("birthdate","birthmonth");
            indexedSlicesQuery.setColumnFamily("Indexed1");
            indexedSlicesQuery.setStartKey("");
            
            QueryResult<OrderedRows<String, String, Long>> result = indexedSlicesQuery.execute();
            
            System.out.println("The results should only have 4 entries: " + result.get());
            
        } catch (HectorException he) {
            he.printStackTrace();
        }
        cluster.getConnectionManager().shutdown(); 
    }
}
