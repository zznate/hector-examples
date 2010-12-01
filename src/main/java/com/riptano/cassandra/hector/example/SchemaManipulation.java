package com.riptano.cassandra.hector.example;

import java.util.Arrays;
import java.util.List;

import me.prettyprint.cassandra.model.BasicColumnDefinition;
import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ColumnIndexType;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;

/**
 * Creates a keyspace and adds a column family. This column family
 * contains an index on the column named 'birthdate' which is exptected
 * to be a Long.  
 * 
 * To run this example from maven:
 * mvn -e exec:java -Dexec.mainClass="com.riptano.cassandra.hector.example.SchemaManipulation"
 * 
 * @author zznate
 *
 */
public class SchemaManipulation {
    
    private static final String DYN_KEYSPACE = "DynamicKeyspace";
    private static final String DYN_CF = "DynamicCf";
    
    private static StringSerializer stringSerializer = StringSerializer.get();
    
    public static void main(String[] args) throws Exception {
        
        Cluster cluster = HFactory.getOrCreateCluster("TestCluster", "localhost:9160");
                
        try {
            cluster.dropKeyspace(DYN_KEYSPACE);
            
            BasicColumnDefinition columnDefinition = new BasicColumnDefinition();
            columnDefinition.setName(stringSerializer.toByteBuffer("birthdate"));
            columnDefinition.setIndexType(ColumnIndexType.KEYS);
            columnDefinition.setValidationClass(ComparatorType.LONGTYPE.getClassName());
            
            BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
            columnFamilyDefinition.setKeyspaceName(DYN_KEYSPACE);
            columnFamilyDefinition.setName(DYN_CF);
            columnFamilyDefinition.addColumnDefinition(columnDefinition);
            
            ColumnFamilyDefinition cfDef = new ThriftCfDef(columnFamilyDefinition);
            
            KeyspaceDefinition keyspaceDefinition = 
                HFactory.createKeyspaceDefinition(DYN_KEYSPACE, "org.apache.cassandra.locator.SimpleStrategy", 1, Arrays.asList(cfDef));
                                               
            cluster.addKeyspace(keyspaceDefinition);
            
            // insert some data
            
            List<KeyspaceDefinition> keyspaces = cluster.describeKeyspaces();
            for (KeyspaceDefinition kd : keyspaces) {
                if ( kd.getName().equals(DYN_KEYSPACE) ) {
                    System.out.println("Name: " +kd.getName());
                    System.out.println("RF: " +kd.getReplicationFactor());
                    System.out.println("strategy class: " +kd.getStrategyClass());
                    List<ColumnFamilyDefinition> cfDefs = kd.getCfDefs();
                    ColumnFamilyDefinition def = cfDefs.get(0);
                    System.out.println("  CF Name: " +def.getName());
                    System.out.println("  CF Metadata: " +def.getColumnMetadata());
                }
            }
            
            
        } catch (HectorException he) {
            he.printStackTrace();
        }
        cluster.getConnectionManager().shutdown(); 
    }

}
