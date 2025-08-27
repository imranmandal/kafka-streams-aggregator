package com.kazam_dlg_ingest;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;
import org.apache.kafka.streams.KafkaStreams;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class HealthCheckServer {
    private final KafkaStreams streams;

    public HealthCheckServer(KafkaStreams streams) {
        this.streams = streams;
    }

    public void start() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);

        server.createContext("/health", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                String status = streams.state().toString(); // RUNNING, REBALANCING, ERROR
                String response = "{ \"status\": \"" + status + "\" }";

                exchange.sendResponseHeaders(200, response.getBytes().length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            }
        });

        server.setExecutor(null);
        server.start();
        System.out.println("\n🚀 Health check running at port:8080 endpoint:\"/health\" \n");
    }
}
