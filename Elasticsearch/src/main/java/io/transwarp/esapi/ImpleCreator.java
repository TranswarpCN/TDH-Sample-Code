package io.transwarp.esapi;

import com.google.common.collect.Maps;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by 杨发林 on 2016/11/22.
 */
public class ImpleCreator implements Creator {
    /*
    log
     */
    private static final Log log = LogFactory.getLog(ImpleCreator.class);
    /*
    index name
     */
    private final String indexName;
    /*
    type name
     */
    private final String typeName;
    /*
    elasticsearch client
     */
    private Client client;
    /*
    bulk import type name
     */
    private static final String TYPE_NAME_KEY = "bulk.import.type.name";
    /*
    bulk import shard number
     */
    private static final String SHARD_NUMBER_KEY = "bulk.import.shard.number";
    /*
    bulk import replica number
     */
    private static final String REPLICA_NUMBER_KEY = "bulk.import.replica.number";
    /*
    bulk import schema
     */
    private static final String SCHEMA_KEY = "bulk.import.schema";
    /*
    bulk import sort fields
     */
    private static final String SORT_FIELDS_KEY = "bulk.import.sort.fields";
    /*
    bulk import field separator
     */
    private static final String FILE_FIELDS_SEPARATOR="bulk.import.fields.separator";
    /*
    bulk import analyze fields
     */
    private static final String ANALYZE_FIELDS_KEY = "bulk.import.analyze.fields";

    /*
    field type
     */
    enum TYPE {
        STRING,
        INT
    }

    /*
     column information
     */
    static class Column {
        /*
        field column name
         */
        public final String name;
        /*
        field column type
         */
        public final TYPE type;
        /*
        need sort?
         */
        public final boolean needSort;
        /*
         need analyze?
         */
        public final boolean needAnalyze;
        /*
        construction function
         */
        private Column(String name, TYPE type, boolean needSort, boolean needAnalyze) {
            this.name = name;
            this.type = type;
            this.needSort = needSort;
            this.needAnalyze = needAnalyze;
        }
        /*
        column type
         */
        public String typeStr() {
            switch (type) {
                case STRING:
                    return "string";
                case INT:
                    return "long";
                default:
                    return null;
            }
        }
        /*
         build a column
         */
        public static Column buildColumn(String column, String sorts[], String analyzes[]) {
            String ss[] = column.split(":");
            if (ss.length != 2) {
                throw new IllegalArgumentException("invalid column : " + column);
            }

            String columnName = ss[1];
            TYPE type = TYPE.valueOf(ss[0]);
            boolean needSort = false;
            boolean needAnalyze = false;
            for (int i = 0; i < sorts.length; ++i) {
                if (columnName.equalsIgnoreCase(sorts[i])) {
                    needSort = true;
                    break;
                }
            }
            for (int i = 0; i < analyzes.length; ++i) {
                if (columnName.equalsIgnoreCase(analyzes[i])) {
                    needAnalyze = true;
                    break;
                }
            }
            return new Column(columnName, type, needSort, needAnalyze);
        }
    }


    private static Column[] fields;
    /*
    init field columns
     */
    private static void initSchema() {
        String schema = System.getProperty(SCHEMA_KEY);
        String sortFields = System.getProperty(SORT_FIELDS_KEY, "");
        String analyzeFields = System.getProperty(ANALYZE_FIELDS_KEY, "");

        if (schema == null) {
            throw new NullPointerException("missing config : " + SCHEMA_KEY);
        }

        String sorts[] = sortFields.split(",");
        String analyzes[] = analyzeFields.split(",");
        String columns[] = schema.split(",");

        fields = new Column[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            fields[i] = Column.buildColumn(columns[i], sorts, analyzes);
        }
    }

    static {
        initSchema();
    }
    /*
    construction function
    init index type name
    init index name
     */

    ImpleCreator() {
        this.typeName = System.getProperty(TYPE_NAME_KEY, "default");
        this.indexName = System.getProperty(Importer.BULK_IMPORT_INDEX_NAME_KEY);
        if (this.indexName == null) {
            throw new RuntimeException("missing config " + Importer.BULK_IMPORT_INDEX_NAME_KEY);
        }
    }

    /*
     实现接口Creator的setClient方法
     */
    public void setClient(Client client) {
        this.client = client;
    }

    /*
     实现接口的create方法
     */
    public IndexRequest create(String line, int lineNo) {

        //String[] array = line.split("\\\\|\\\\+\\\\+\\\\|");
        String[] array = line.split(System.getProperty(FILE_FIELDS_SEPARATOR));

        IndexRequestBuilder indexRequestBuilder = client.prepareIndex(indexName, typeName);
        Map<String, Object> map = Maps.newHashMap();
        int min = Math.min(array.length, fields.length);

        for (int i = 0; i < min; ++i) {
            switch (fields[i].type) {
                case INT:
                    try {
                        map.put(fields[i].name, Long.parseLong(array[i]));
                    } catch (NumberFormatException e) {
                        System.err.println("field " + fields[i].name + " is no int, value " + array[i]);
                    }
                    break;
                default:
                    map.put(fields[i].name, array[i]);
            }
        }
        indexRequestBuilder.setSource(map);

        return indexRequestBuilder.request();
    }
    /*
    实现接口creator的createIndex方法
     */
    public void createIndex(String indexName) throws IOException {

        IndicesAdminClient adminClient = client.admin().indices();
        // build mapping
        XContentBuilder content = XContentFactory.jsonBuilder().startObject().startObject(typeName);

        content = content.startObject("_source").field("enabled", true).endObject();
        content = content.startObject("_all").field("enabled", false).endObject();
        content.startObject("properties");
        content = content.startObject("content");
        content.field("type", "string");
        content.field("index", "no");
        content.field("store", false);
        content = content.endObject();

        for (int i = 0; i < fields.length; ++i) {
            content = content.startObject(fields[i].name);
            content.field("type", fields[i].typeStr());
            if (fields[i].needAnalyze && fields[i].type == TYPE.STRING) {
                content.field("index", "analyzed");
            } else {
                content.field("index", "not_analyzed");
            }
            if (fields[i].needSort) {
                content.field("doc_values", true);
            } else {
                content.field("doc_values", false);
            }
            content.field("store", false);
            content = content.endObject();
        }

        content = content.endObject().endObject();

        Settings.Builder sb = Settings.settingsBuilder()
                .put("index.refresh_interval", "60s")
                .put("index.translog.durability", "async");

        String shardNum = System.getProperty(SHARD_NUMBER_KEY, "32");
        sb.put("index.number_of_shards", Integer.parseInt(shardNum));

        String replicaNum = System.getProperty(REPLICA_NUMBER_KEY, "0");
        sb.put("index.number_of_replicas", Integer.parseInt(replicaNum));

        // build setting
        // create index
        CreateIndexRequestBuilder builder = adminClient.prepareCreate(indexName);
        builder.addMapping(typeName, content);
        builder.setSettings(sb.build());
        builder.execute().actionGet();
    }
}
