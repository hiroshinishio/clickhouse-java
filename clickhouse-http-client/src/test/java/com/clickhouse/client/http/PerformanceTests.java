package com.clickhouse.client.http;

import com.clickhouse.client.ClickHouseClient;
import com.clickhouse.client.ClickHouseConfig;
import com.clickhouse.client.ClickHouseNode;
import com.clickhouse.client.ClickHouseNodeSelector;
import com.clickhouse.client.ClickHouseProtocol;
import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.ClickHouseResponse;
import com.clickhouse.client.config.ClickHouseClientOption;
import com.clickhouse.client.http.config.ClickHouseHttpOption;
import com.clickhouse.client.http.config.HttpConnectionProvider;
import com.clickhouse.config.ClickHouseOption;
import com.clickhouse.data.ClickHouseCompression;
import com.clickhouse.data.ClickHouseDataStreamFactory;
import com.clickhouse.data.ClickHouseFormat;
import com.clickhouse.data.ClickHousePipedOutputStream;
import com.clickhouse.data.format.BinaryStreamUtils;
import com.clickhouse.data.stream.NonBlockingPipedOutputStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;


public class PerformanceTests {

    private static final java.util.logging.Logger log = java.util.logging.Logger.getLogger(PerformanceTests.class.getName());
    private static final String TABLE_NAME = "big_dataset";

    private String host = "localhost";
    private int port = 8123;

    @Test(groups = "performance", enabled = true)
    public void testWriteBigDataset() {
        long start = System.currentTimeMillis();
        TestDataset dataset = prepareDataset(1000, 10000, 16);
//        for (List<Object> values : dataset.getData()) {
//            System.out.println(values);
//        }
        log.info("Dataset prepared in " + (System.currentTimeMillis() - start) + " ms");


        try (ClickHouseClient client = getClient(new ClickHouseConfig())) {
            client.read(getServer(host, port)).query("DROP TABLE IF EXISTS default." + TABLE_NAME).execute();

            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE default." + TABLE_NAME + " ( ");
            for (String column : dataset.getColumns()) {
                sb.append(column).append(" String,");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(") ENGINE = MergeTree ORDER BY tuple()");
            log.info("Creating table: " + sb.toString());
            client.read(getServer(host, port)).query(sb.toString()).execute();

        } catch (Exception e) {
            e.printStackTrace();
            log.severe("Failed to upload dataset: " + e.getMessage());
            Assert.fail(e.getMessage());
        }

        Map<ClickHouseOption, Serializable> map = new HashMap<>();
        map.put(ClickHouseClientOption.SOCKET_TIMEOUT, 120000);
//        map.put(ClickHouseClientOption.RESPONSE_BUFFERING, ClickHouseBufferingMode.PERFORMANCE);

        int buffK = (7 * 65500);
        map.put(ClickHouseClientOption.BUFFER_SIZE, buffK); // ((65000 / 8192) * 65000) = 455000
        map.put(ClickHouseClientOption.WRITE_BUFFER_SIZE, 65500);

        map.put(ClickHouseClientOption.SOCKET_RCVBUF, buffK);
        map.put(ClickHouseClientOption.SOCKET_SNDBUF, buffK);
        map.put(ClickHouseClientOption.SOCKET_TCP_NODELAY, true);
//        map.put(ClickHouseHttpOption.KEEP_ALIVE, false);
        map.put(ClickHouseClientOption.REQUEST_CHUNK_SIZE, buffK); // doesn't work with apache http client
        map.put(ClickHouseClientOption.DECOMPRESS, true);
        map.put(ClickHouseClientOption.DECOMPRESS_ALGORITHM, ClickHouseCompression.LZ4);
        map.put(ClickHouseClientOption.MAX_QUEUED_BUFFERS, 10);
//        map.put(ClickHouseClientOption.REQUEST_BUFFERING, ClickHouseBufferingMode.PERFORMANCE);
        map.put(ClickHouseHttpOption.REMEMBER_LAST_SET_ROLES, true);
//        map.put(ClickHouseClientOption.ASYNC, true);
        map.put(ClickHouseHttpOption.CONNECTION_PROVIDER, HttpConnectionProvider.HTTP_URL_CONNECTION);
//        map.put(ClickHouseClientOption.SSL, true);
//        map.put(ClickHouseClientOption.SSL_MODE, ClickHouseSslMode.STRICT);
//        map.put(ClickHouseClientOption.SSL_KEY, "./client.key" );
//        map.put(ClickHouseClientOption.SSL_CERTIFICATE, "./client.crt" );
//        map.put(ClickHouseClientOption.SSL_ROOT_CERTIFICATE, "./marsnet_ca.crt" );

        log.info("Creating client");
        try (ClickHouseClient client = getClient(new ClickHouseConfig(map))) {
            ClickHouseRequest.Mutation mutation =
                    client.read(ClickHouseNode.builder().host(host).port(ClickHouseProtocol.HTTP, port).build())

                            .write().table("default." + TABLE_NAME).format(ClickHouseFormat.RowBinary);

            delayForProfiler(0);
            log.info("Starting upload");
            long startUpload = System.currentTimeMillis();

            ClickHousePipedOutputStream out = ClickHouseDataStreamFactory.getInstance().createPipedOutputStream(client.getConfig());
            Future<ClickHouseResponse> response = mutation.data(out.getInputStream()).execute();
            long startWrite = System.currentTimeMillis();
            for (List<Object> row : dataset.getData()) {
                for (Object value : row) {
//                    System.out.print('.');
                    BinaryStreamUtils.writeString(out, (String) value);
                }
            }

            System.out.println();
            log.info("Dataset written in " + (System.currentTimeMillis() - startWrite) + " ms");
            out.close();
            ClickHouseResponse resp = response.get();
            log.info("Data upload summary: " + resp.getSummary());
            log.info("Dataset uploaded in " + (System.currentTimeMillis() - startUpload) + " ms");
        } catch (Exception e) {
            log.severe("Failed to upload dataset: " + e.getMessage());
            Assert.fail(e.getMessage(), e);
        }
    }

    @Test
    public void testReadBigDataset() {
        Map<ClickHouseOption, Serializable> map = new HashMap<>();
//        map.put(ClickHouseClientOption.COMPRESS, true); // is true by default

        log.info("Creating client");
        try (ClickHouseClient client = getClient(new ClickHouseConfig(map))) {

            long startRead = System.currentTimeMillis();
            ClickHouseResponse respone = client.read(getServer(host, port))
                    .query("SELECT * FROM " + TABLE_NAME)
                    .format(ClickHouseFormat.JSONEachRow)
                    .executeAndWait();
            log.info("Dataset read in " + (System.currentTimeMillis() - startRead) + " ms");
            log.info("Response: " + respone.getSummary());
            long startWrite = System.currentTimeMillis();


            final int COPY_BUF_SIZE = 10 * 1024 * 1024;
            // We can read the response in a blocking way
            OutputStream customOutput = new OutputStream() {
                StringBuilder sb = new StringBuilder(COPY_BUF_SIZE);
                int lineCount = 0;

                @Override
                public void write(int b) throws IOException {
                }

                @Override
                public void write(@NotNull byte[] buf, int off, int len) {
                    for (int i = off; i < len; i++) {
                        if (buf[i] == '\n') {
                            log.info("Line " + (lineCount++) + ": " + sb.toString());
                            sb.setLength(0);
                        } else {
                            sb.append((char) buf[i]);
                        }
                    }
                }
            };
            long bytesRead = respone.getInputStream()
                    .pipe(
                            // here I'm output to null stream but in case of service it should be a client output stream
                            NonBlockingPipedOutputStream.of(customOutput, COPY_BUF_SIZE));
            log.info("Bytes " + bytesRead + " read in " + (System.currentTimeMillis() - startWrite) + " ms");
        } catch (Exception e) {
            log.severe("Failed to upload dataset: " + e.getMessage());
            Assert.fail(e.getMessage(), e);
        }
    }

    private ClickHouseNode getServer(String host, int port) {
        return ClickHouseNode.builder().host(host).port(ClickHouseProtocol.HTTP, port).build();
    }

    private void delayForProfiler(int ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private TestDataset prepareDataset(int columns, int rows, int valueSize) {
        TestDataset dataset = new TestDataset();

        for (int i = 0; i < columns; i++) {
            dataset.getColumns().add("column" + i);
        }

        String value = RandomStringUtils.randomNumeric(valueSize);

        long start = System.currentTimeMillis();
        for (int i = 0; i < rows; i++) {

            List<Object> row = new ArrayList<>(columns);
            for (int j = 0; j < columns; j++) {
                row.add(value);
            }
            dataset.getData().add(row);
        }
        log.info("Dataset generated in " + (System.currentTimeMillis() - start) + " ms");

        return dataset;
    }


    private static class TestDataset {


        private List<String> columns = new ArrayList<>();

        private List<List<Object>> data = new ArrayList<>();

        public List<List<Object>> getData() {
            return data;
        }

        public List<String> getColumns() {
            return columns;
        }
    }


    @BeforeMethod
    public void setUp() {
//        clickhouse.start();
    }

    @BeforeMethod
    public void tearDown() {
//        clickhouse.stop();
    }


    protected ClickHouseClient getClient(ClickHouseConfig... configs) {
        return ClickHouseClient.builder().config(new ClickHouseConfig(configs))
                .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
                .build();
    }
}
