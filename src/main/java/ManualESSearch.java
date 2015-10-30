import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

import java.util.Scanner;

/**
 * Created by adyachenko on 22.10.15.
 */
public class ManualESSearch {

    public static void searchESByQuery (String input, Client client) {
        Integer i = new Integer(0);
            QueryBuilder fqb = QueryBuilders.queryStringQuery(input);
            SearchResponse scrollResp = client.prepareSearch("marketing*")
                    .setSearchType(SearchType.SCAN)
                    .setScroll(new TimeValue(60000))
                    .setQuery(fqb)
                            //.addFields ("modified")
                    .setSize(5).execute().actionGet();
            System.out.println(scrollResp.getHits().getTotalHits());
            i = 20;
            while (true) {
                for (SearchHit hit : scrollResp.getHits().getHits()) {
                    System.out.println("############################### START OF ITEM #############################################");
                    System.out.println("ID: " + hit.getId());
                    //System.out.println("Version: " + hit.getVersion());
                    //System.out.println("URL: " + hit.getSource().get("url"));
                    //System.out.println("Https: " + hit.getSource().get("https"));
                    //System.out.println("Title: " + hit.getSource().get("title"));
                    //System.out.println("Description: " + hit.getSource().get("meta-tag.description"));
                    //System.out.println("Keywords: " + hit.getSource().get("meta-tag.keywords"));
                    //System.out.println("Emails:" + hit.getSource().get("emails"));
                    //System.out.println("Phones: " + hit.getSource().get("phones"));
                    //System.out.println("Partners: " + hit.getSource().get("partners"));
                    //System.out.println("Content: " + hit.getSource().get("content"));
                    //System.out.println("HTML:" + hit.getSource().get("rawcontent"));
                    //System.out.println("Modified: " + hit.getSource().get("modified"));
                    //System.out.println("Language: " + hit.getSource().get("mainLang"));
                    //System.out.println("IP:" + hit.getSource().get("ip"));
                    System.out.println("################################# END OF ITEM ############################################");
                }
                scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
                if (scrollResp.getHits().getHits().length == 0) {
                    break;
                }
                i = i * 2;
            }

        System.out.println();
    }

    public static String readQuery () {
        Scanner in = new Scanner(System.in);
        System.out.print("Enter query: ");
//        int i = in.nextInt();
        String input = in.nextLine();
        return input;
    }

    public static void main (String[] args) {
        try (Node node = NodeToES.connectASNode()) {
            Client client = node.client();

            while (true) {
                String query = readQuery();
                if (query.equals("quit")) {
                    break;
                }
                long startTime = System.nanoTime();
                searchESByQuery(query, client);
                long endTime = System.nanoTime();
                long duration = (endTime - startTime)/1000000;
                System.out.println("Query time: "+duration + " ms");
            }
        }
        System.out.println("Exiting...");
        //node.close();
    }
}
