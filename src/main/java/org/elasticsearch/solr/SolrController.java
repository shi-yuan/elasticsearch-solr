package org.elasticsearch.solr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.atomic.AtomicInteger;

@Controller
public class SolrController {

    private static final Logger logger = LoggerFactory.getLogger(SolrController.class);

    private String requestHandler;
    private String uniqueKey;
    private int rows = 5000;

    private volatile BulkProcessor bulkProcessor;

    private ObjectMapper objectMapper;
    private CloseableHttpClient httpClient;

    static final String ES_INDEX_NAME = "resume";
    static final String ES_INDEX_TYPE = "resume";

    static final String DEFAULT_UNIQUE_KEY = "id";
    static final String DEFAULT_QUERY = "*:*";
    static final String SOLR_URL = "http://localhost:8888/solr/resume-all";

    /**
     * 导入solr数据到es
     */
    @RequestMapping(value = "/es/import", method = RequestMethod.GET)
    @ResponseBody
    public String importToES() {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        Client client = new TransportClient(settings).addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        this.bulkProcessor = BulkProcessor.builder(client, new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                logger.info("Going to execute new bulk composed of {} actions", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                logger.info("Executed bulk composed of {} actions", request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.warn("Error executing bulk: {}", failure);
            }
        }).setBulkActions(1000).setConcurrentRequests(10).setFlushInterval(TimeValue.timeValueSeconds(10)).build();

        this.httpClient = HttpClients.createDefault();
        this.uniqueKey = DEFAULT_UNIQUE_KEY;
        this.requestHandler = "select";

        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        start();

        return "success";
    }

    public void start() {
        /*if (!client.admin().indices().prepareExists(indexName).execute().actionGet().isExists()) {
            CreateIndexRequestBuilder createIndexRequest = client.admin().indices().prepareCreate(indexName);
            createIndexRequest.execute().actionGet();
        }*/
        StringBuilder baseSolrQuery = createSolrQuery();
        Long numFound = 0L;
        int startParam;
        AtomicInteger start = new AtomicInteger(0);
        while ((startParam = start.getAndAdd(rows)) == 0 || startParam < numFound) {
            String solrQuery = baseSolrQuery.toString() + "&start=" + startParam;
            CloseableHttpResponse httpResponse = null;
            try {
                logger.info("Sending query to Solr: {}", solrQuery);
                httpResponse = httpClient.execute(new HttpGet(solrQuery));
                if (httpResponse.getStatusLine().getStatusCode() != 200) {
                    logger.error("Solr returned non ok status code: {}", httpResponse.getStatusLine().getReasonPhrase());
                    EntityUtils.consume(httpResponse.getEntity());
                    continue;
                }

                JsonNode jsonNode = objectMapper.readTree(EntityUtils.toString(httpResponse.getEntity()));
                JsonNode response = jsonNode.get("response");
                JsonNode numFoundNode = response.get("numFound");
                numFound = numFoundNode.asLong();
                if (logger.isWarnEnabled() && numFound == 0) {
                    logger.warn("The solr query {} returned 0 documents", solrQuery);
                }

                for (JsonNode docNode : response.get("docs")) {
                    try {
                        JsonNode uniqueKeyNode = docNode.get(uniqueKey);
                        if (uniqueKeyNode == null) {
                            logger.error("The uniqueKey value is null");
                        } else {
                            String id = uniqueKeyNode.asText();
                            ((ObjectNode) docNode).remove(uniqueKey);
                            IndexRequest indexRequest = Requests.indexRequest(ES_INDEX_NAME).type(ES_INDEX_TYPE).id(id);
                            indexRequest.source(objectMapper.writeValueAsString(docNode));
                            bulkProcessor.add(indexRequest);
                        }
                    } catch (IOException e) {
                        logger.warn("Error while importing documents from solr to elasticsearch: {}", e);
                    }
                }
            } catch (IOException e) {
                logger.error("Error while executing the solr query [{}]: {}", solrQuery, e);
                bulkProcessor.close();
                try {
                    httpClient.close();
                } catch (IOException ioe) {
                    logger.warn(e.getMessage(), ioe);
                }
                // if a query fails the next ones are most likely going to fail too
                return;
            } finally {
                if (httpResponse != null) {
                    try {
                        httpResponse.close();
                    } catch (IOException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        }

        //
        bulkProcessor.close();
        try {
            httpClient.close();
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
        }
        logger.info("Data import from solr to elasticsearch completed");
    }

    protected StringBuilder createSolrQuery() {
        StringBuilder queryBuilder = new StringBuilder(SOLR_URL);
        if (Strings.hasLength(requestHandler)) {
            if (queryBuilder.charAt(queryBuilder.length() - 1) != '/') {
                queryBuilder.append("/");
            }
            queryBuilder.append(requestHandler);
        }
        queryBuilder.append("?q=").append(encode(DEFAULT_QUERY)).append("&wt=json").append("&rows=").append(rows);
        return queryBuilder;
    }

    private static String encode(String value) {
        try {
            return URLEncoder.encode(value, "utf-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}