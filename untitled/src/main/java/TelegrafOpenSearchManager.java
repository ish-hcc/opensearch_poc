import java.io.*;
import java.nio.file.*;
import org.apache.http.HttpHost;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;

public class TelegrafOpenSearchManager {

    // 기본 설정 값들
    private static final String TELEGRAF_CONF = "/etc/telegraf/telegraf.conf";
    private static final String TELEGRAF_CONF_BAK = "/etc/telegraf/telegraf.conf.bak";
    private static String syslogFile = "/var/log/syslog"; // Syslog 파일 위치
    private static String opensearchHost = "http://192.168.110.14:9200"; // OpenSearch 서버 URL

    // 입력 변수들
    private static final String VM_ID = "my-vm-test-1";
    private static final String MCI_GROUP_ID = "MCI-1";
    private static final String INDEX_NAME = "telegraf-test";
    private static final String TEMPLATE_NAME = "telegraf-*";
    private static final String USERNAME = ""; // Optional
    private static final String PASSWORD = ""; // Optional

    public static void main(String[] args) {
        // 루트 권한 체크
        if (!isRoot()) {
            System.err.println("This program must be run as root.");
            System.exit(1);
        }

        // Command line arguments processing (optional)
        if (args.length > 0) {
            opensearchHost = args[0];
        }
        if (args.length > 1) {
            syslogFile = args[1];
        }

        try {
            // 백업 파일이 없으면 설정 파일을 백업
            File backupFile = new File(TELEGRAF_CONF_BAK);
            if (!backupFile.exists()) {
                Files.copy(Paths.get(TELEGRAF_CONF), Paths.get(TELEGRAF_CONF_BAK), StandardCopyOption.REPLACE_EXISTING);
            }

            // 기존 설정 파일 삭제
            File configFile = new File(TELEGRAF_CONF);
            if (configFile.exists()) {
                if (!configFile.delete()) {
                    System.err.println("Failed to delete existing telegraf configuration file.");
                    return;
                }
            }

            // 새 설정 작성
            writeNewConfig();

            // Telegraf 재시작
            Runtime.getRuntime().exec("systemctl restart telegraf");

            // OpenSearch에서 로그 검색
            searchLogs();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean isRoot() {
        return System.getProperty("user.name").equals("root");
    }

    private static void writeNewConfig() throws IOException {
        String grokPattern = determineGrokPattern();

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(TELEGRAF_CONF))) {
            writer.write("# Global Agent Configuration\n");
            writer.write("[agent]\n");
            writer.write("  interval = \"1s\"\n");
            writer.write("  round_interval = true\n");
            writer.write("  metric_batch_size = 1000\n");
            writer.write("  metric_buffer_limit = 10000\n");
            writer.write("  collection_jitter = \"0s\"\n");
            writer.write("  flush_interval = \"1s\"\n");
            writer.write("  flush_jitter = \"0s\"\n");
            writer.write("  precision = \"\"\n");
            writer.write("  debug = true\n");
            writer.write("  quiet = false\n");
            writer.write("  logfile = \"/var/log/telegraf/telegraf.log\"\n");
            writer.write("\n");

            writer.write("# Input Plugin: Tail (to read syslog from a file)\n");
            writer.write("[[inputs.tail]]\n");
            writer.write("  files = [\"" + syslogFile + "\"]\n");
            writer.write("  from_beginning = false\n");
            writer.write("  watch_method = \"inotify\"\n");
            writer.write("\n");
            writer.write("  # Data format to parse syslog entries\n");
            writer.write("  data_format = \"grok\"\n");
            writer.write("  grok_patterns = [\"" + grokPattern + "\"]\n");
            writer.write("\n");
            writer.write("  # Add these fields if you want to tag the logs\n");
            writer.write("  [inputs.tail.tags]\n");
            writer.write("    vm_id = \"" + VM_ID + "\"\n");
            writer.write("    mci_group_id = \"" + MCI_GROUP_ID + "\"\n");
            writer.write("\n");

            writer.write("# Output Plugin: Elasticsearch (for OpenSearch)\n");
            writer.write("[[outputs.opensearch]]\n");
            writer.write("  urls = [\"" + opensearchHost + "\"]\n");
            writer.write("  index_name = \"" + INDEX_NAME + "\"\n");
            writer.write("  template_name = \"" + TEMPLATE_NAME + "\"\n");
            if (!USERNAME.isEmpty() && !PASSWORD.isEmpty()) {
                writer.write("  username = \"" + USERNAME + "\"\n");
                writer.write("  password = \"" + PASSWORD + "\"\n");
            }
            writer.write("\n");
        }
    }

    private static String determineGrokPattern() {
        String osName = System.getProperty("os.name").toLowerCase();

        if (osName.contains("ubuntu") || osName.contains("debian")) {
            return "%{TIMESTAMP_ISO8601:timestamp} %{SYSLOGHOST:hostname} %{PROG:program}(?:\\[%{POSINT:pid}\\])?: %{GREEDYDATA:message}";
        } else if (osName.contains("redhat") || osName.contains("centos") || osName.contains("fedora")) {
            return "%{SYSLOGTIMESTAMP:timestamp} %{SYSLOGHOST:hostname} %{PROG:program}(?:\\[%{POSINT:pid}\\])?: %{GREEDYDATA:message}";
        } else {
            return "%{TIMESTAMP_ISO8601:timestamp} %{SYSLOGHOST:hostname} %{PROG:program}: %{GREEDYDATA:message}";
        }
    }

    private static void searchLogs() throws Exception {
        try (RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(HttpHost.create(opensearchHost)))) {

            // 인덱스 존재 여부 확인
            if (!isIndexExists(client, INDEX_NAME)) {
                createIndex(client, INDEX_NAME);
            }

            // 검색 요청 생성
            SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            // Term 쿼리 설정
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery()
                    .must(QueryBuilders.matchQuery("tag.mci_group_id", MCI_GROUP_ID))
                    .must(QueryBuilders.matchQuery("tag.vm_id", VM_ID)); // 추가된 VM_ID 필터링

            // 최근 1일 동안의 로그를 조회하는 범위 쿼리 설정
            RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("@timestamp")
                    .gte("now-1d/d")
                    .lte("now");

            // Bool 쿼리에 범위 쿼리 추가
            boolQueryBuilder.filter(rangeQueryBuilder);

            // 쿼리를 SearchSourceBuilder에 추가
            searchSourceBuilder.query(boolQueryBuilder);
            searchSourceBuilder.size(100); // 최대 100개 문서로 제한
            searchSourceBuilder.sort("@timestamp", SortOrder.DESC); // 최신 문서부터 정렬

            searchRequest.source(searchSourceBuilder);

            // 쿼리 출력
            System.out.println("Executing query: " + searchSourceBuilder.query().toString());

            // 검색 요청 실행
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

            // 결과 출력
            if (searchResponse.getHits().getTotalHits().value > 0) {
                searchResponse.getHits().forEach(hit -> System.out.println(hit.getSourceAsString()));
            } else {
                System.out.println("No results found.");
            }
        }
    }

    private static boolean isIndexExists(RestHighLevelClient client, String indexName) throws IOException {
        GetIndexRequest request = new GetIndexRequest(indexName);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    private static void createIndex(RestHighLevelClient client, String indexName) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(indexName);
        // 필요한 경우 인덱스 설정 및 매핑을 추가할 수 있습니다.
        // request.settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 2));
        // request.mapping("properties", "timestamp", "type=date");

        client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println("Index created: " + indexName);
    }
}
