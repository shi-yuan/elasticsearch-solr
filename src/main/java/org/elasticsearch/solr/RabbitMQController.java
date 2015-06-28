package org.elasticsearch.solr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

@Controller
public class RabbitMQController {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMQController.class);

    private String requestHandler;
    private int rows = 5000;

    private ObjectMapper objectMapper;
    private CloseableHttpClient httpClient;

    private static final String DEFAULT_UNIQUE_KEY = "id";
    private static final String DEFAULT_QUERY = "*:*";
    private static final String SOLR_URL = "http://localhost:8888/solr/resume-all";

    private Channel channel;
    private Connection connection;
    private static final String QUEUE_NAME = "resume_queue";

    /**
     * 导入solr数据到rabbitmq
     */
    @RequestMapping(value = "/amq/import", method = RequestMethod.GET)
    @ResponseBody
    public String importToRabbitMQ() throws IOException, TimeoutException {
        // Create a connection factory
        ConnectionFactory factory = new ConnectionFactory();

        // hostname of your rabbitmq server
        factory.setHost("localhost");

        // getting a connection
        connection = factory.newConnection();

        // creating a channel
        channel = connection.createChannel();

        // declaring a queue for this channel. If queue does not exist, it will be created on the server.
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        this.httpClient = HttpClients.createDefault();
        this.requestHandler = "select";

        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        start();

        return "success";
    }

    public void start() throws IOException, TimeoutException {
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
                        if (null != docNode.get(DEFAULT_UNIQUE_KEY)) {
                            channel.basicPublish("", QUEUE_NAME, null, objectMapper.writeValueAsBytes(docNode));
                        } else {
                            logger.error("The uniqueKey value is null");
                        }
                    } catch (IOException e) {
                        logger.warn("Error while importing documents from solr to rabbitMQ: {}", e);
                    }
                }
            } catch (IOException e) {
                logger.error("Error while executing the solr query [{}]: {}", solrQuery, e);
                close();
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
        close();
        try {
            httpClient.close();
        } catch (IOException e) {
            logger.warn(e.getMessage(), e);
        }
        logger.info("Data import from solr to rabbitMQ completed");
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

    public void close() throws IOException, TimeoutException {
        this.channel.close();
        this.connection.close();
    }
}