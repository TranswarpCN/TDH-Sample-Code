package io.transwarp.esapi;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;

import java.io.IOException;

/**
 * Created by 杨发林 on 2016/11/22.
 */

public interface Creator {


    /*
    set elasticsearch client
     */
    public void setClient(Client client);
    /*
      TODO
     */
    public IndexRequest create(String line, int lineNo);
    /*
    set the index name
     */
    public void createIndex(String indexName) throws IOException;
}
