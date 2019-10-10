import com.fasterxml.jackson.databind.ObjectMapper;
import net.sf.json.JSONObject;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.*;

public class EsUtil {

    private final static ObjectMapper mapper = new ObjectMapper();


    public static Set<String> ipSet=new HashSet<String>();


    public static Header[] defaultHeaders = new Header[]{new BasicHeader("header", "value")};

    public static Map<String, String> getMapCount(RestHighLevelClient client) {
        Map<String, String> map = new HashMap<String, String>();
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("mark");
        searchRequest.types("user");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(10000);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse searchResponse = client.search(searchRequest, defaultHeaders);
            for (SearchHit searchHit : searchResponse.getHits()) {
                try {
                    AllMark mark = mapper.readValue(searchHit.getSourceAsString(), AllMark.class);
                    map.put(mark.getIp() + "," + mark.getFilename(), searchHit.getId() + "," + mark.getRank());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return map;
    }

    public static synchronized void createIpIndexEs(String ipIndex, RestHighLevelClient client) {
        if (checkIndexExist(client, ipIndex)) {
            return;
        }
        CreateIndexRequest request = new CreateIndexRequest(ipIndex);
        XContentBuilder builder = null;
        try {
            builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("number_of_shards", "5")
                    .field("max_result_window", "10000000")
                    .startObject("analysis")
                    .startObject("normalizer")
                    .startObject("my_normalizer")
                    .field("type", "custom")
                    .field("filter", "lowercase")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }

        request.settings(builder);
        XContentBuilder mapping2 = null;
        try {
            mapping2 = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("filename").field("type", "keyword").endObject()
                    .startObject("message")
                    .field("type", "keyword")
                    .field("ignore_above", "256")
                    .field("normalizer", "my_normalizer")
                    .endObject()
                    .startObject("rank").field("type", "long").endObject()
                    .startObject("time").field("type", "long").endObject()
                    .endObject()
                    .endObject();
            request.mapping("user", mapping2);
            if(!ipSet.contains(ipIndex)){
                client.indices().create(request, EsUtil.defaultHeaders);
            }
            ipSet.add(ipIndex);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (mapping2 != null) {
                mapping2.close();
            }
        }


    }

    public static RestHighLevelClient getClient() {
        Properties props = PropUtil.loadProperties("/es.properties");
        String ipString = props.getProperty("ip");
        String[] ipArray = ipString.split(";");
        HttpHost[] httpArray = new HttpHost[ipArray.length];
        for (int i = 0; i < ipArray.length; i++) {
            httpArray[i] = new HttpHost(ipArray[i], 9200, "http");
        }
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(httpArray));
        return client;
    }

    public static void createMarkIndex(RestHighLevelClient client) {
        if (checkIndexExist(client, "mark")) {
            return;
        }
        CreateIndexRequest request = new CreateIndexRequest("mark");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 1)
        );
        XContentBuilder mapping = null;
        try {
            mapping = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("ip").field("type", "keyword").endObject()
                    .startObject("filename").field("type", "keyword").endObject()
                    .startObject("rank").field("type", "long").endObject()
                    .endObject()
                    .endObject();
            request.mapping("user", mapping);
            client.indices().create(request, defaultHeaders);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (mapping != null) {
                mapping.close();
            }
        }
    }

    public static void createIPIndex(RestHighLevelClient client, String index) {
        if (checkIndexExist(client, index)) {
            return;
        }
        CreateIndexRequest request = new CreateIndexRequest(index);

        XContentBuilder builder = null;
        try {
            builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("number_of_shards", "5")
                    .field("max_result_window", "10000000")
                    .startObject("analysis")
                    .startObject("normalizer")
                    .startObject("my_normalizer")
                    .field("type", "custom")
                    .field("filter", "lowercase")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }

        request.settings(builder);
        XContentBuilder mapping2 = null;
        try {
            mapping2 = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("filename").field("type", "keyword").endObject()
                    .startObject("message")
                    .field("type", "keyword")
                    .field("ignore_above", "256")
                    .field("normalizer", "my_normalizer")
                    .endObject()
                    .startObject("rank").field("type", "long").endObject()
                    .startObject("time").field("type", "long").endObject()
                    .endObject()
                    .endObject();
            request.mapping("user", mapping2);
            client.indices().create(request, defaultHeaders);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (mapping2 != null) {
                mapping2.close();
            }
        }
    }


    public static boolean checkIndexExist(RestHighLevelClient client, String index) {
        try {
            Response response = client.getLowLevelClient().performRequest("HEAD", index);
            boolean exist = response.getStatusLine().getReasonPhrase().equals("OK");
            return exist;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static void saveData(String index, Template template, RestHighLevelClient client) {
        IndexRequest indexRequest = new IndexRequest(index, "user");
        indexRequest.source(JSONObject.fromObject(template), XContentType.JSON);
        try {
            client.index(indexRequest, defaultHeaders);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void updateData(String id, long rank, RestHighLevelClient client) {
        UpdateRequest updateRequest = new UpdateRequest();
        try {
            updateRequest.index("mark").type("user").id(id).
                    doc(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("rank", rank)
                            .endObject());
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            client.update(updateRequest, defaultHeaders);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        RestHighLevelClient client = EsUtil.getClient();
        createMarkIndex(client);
        //updateData();
    }

    public static void timeTask(final RestHighLevelClient client) {
        final long timeInterval = 1000;
        Runnable runnable = new Runnable() {
            public void run() {
                while (true) {
                    updateMarkIndex(client);
                    try {
                        Thread.sleep(timeInterval * 30);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
        };
        Thread thread = new Thread(runnable);
        thread.start();

    }

    public static void updateMarkIndex(RestHighLevelClient client) {
        Map<String, String> markMap = FlinkToEs.markMap;
        for (String value : markMap.values()) {
            if (value != null) {
                String[] array = value.split(",");
                UpdateRequest updateRequest = new UpdateRequest();
                try {
                    updateRequest.index("mark1").type("user").id(array[1])
                            .doc(XContentFactory.jsonBuilder()
                                    .startObject()
                                    .field("rank", Long.parseLong(array[0]))
                                    .endObject());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    client.update(updateRequest, EsUtil.defaultHeaders);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
