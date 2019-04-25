package org.research.testing;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class BulkQueryES {
    public static void main(String[] args) throws IOException {
//        for (int i = 0; i < 4635334; i += 10000) {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost("localhost", 9200, "http")));
        String field = "";
//        AggregationBuilder aggs = AggregationBuilders.terms("related").field(field).size(100000);
        QueryBuilder querys = QueryBuilders.matchAllQuery();
//        QueryBuilder q2 = QueryBuilders.rangeQuery("timestamp").gte(start).lte(end);
//        QueryBuilder q3 = QueryBuilders.matchPhrasePrefixQuery("location.province", province);
//        QueryBuilder q4 = QueryBuilders.matchPhrasePrefixQuery("ann_category.scope", scope);
//        QueryBuilder q5 = QueryBuilders.matchPhrasePrefixQuery("ann_sentiment", sentiment);
//        BoolQueryBuilder querys = QueryBuilders.boolQuery();
//        querys.must(q1);
//        querys.must(q2);
//        querys.must(q3);
//        querys.must(q4);
//        querys.must(q5);
        System.out.println(querys.toString());
//        System.out.println(aggs.toString());

        try {
            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            SearchRequest searchRequest = new SearchRequest("facebook-fanpage");
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(querys).size(10000);
            searchRequest.source(searchSourceBuilder);

            SearchResponse sr = client.search(searchRequest);
            String scrollId = sr.getScrollId();
            SearchHit[] searchHits = sr.getHits().getHits();

            int i = 0;
            String path = "";


            while (searchHits != null && searchHits.length > 0) {


                for (SearchHit searchHit : searchHits) {

                    if (i == 0) {
//                        path = "/home/dev/reja/id1.text";
                        path = "/home/research/id/id1.text";
                    } else if ((i % 10000) == 0) {
//                        path = "/home/dev/reja/id"+i+".text";
//                        System.out.println(i % 10000);
                        path = "/home/research/id/id" + i + ".text";
                    }
                    else if (((i % 10000) == 5334) && (i == 4635334)) {
                        path = "/home/research/id/id" + i + ".text";
                        System.out.println("==== " + i % 10000);
                    }

                    try {
                        String data = searchHit.getSourceAsMap().get("id").toString();
                        System.out.println(data);

                        File f1 = new File(path);
                        FileWriter fileWritter = new FileWriter(f1.getPath(), true);
                        BufferedWriter bw = new BufferedWriter(fileWritter);
                        bw.write(data);
                        bw.newLine();
                        bw.close();
                        System.out.println("Done");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    i++;
                }
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                sr = client.searchScroll(scrollRequest);
                scrollId = sr.getScrollId();
                searchHits = sr.getHits().getHits();


            }


        } catch (Exception e) {
            e.getStackTrace();
        } finally {
            client.close();
        }
//        }

    }
}
