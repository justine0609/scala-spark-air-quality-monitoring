import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Minimal static file server for the dashboard.
 *
 * The dashboard loads output\*.json with fetch(), so it cannot be opened
 * as a file:// URL - it must be served over HTTP. Python is not always
 * installed on a grading machine, but a JDK is (Spark requires one), so
 * this class provides a dependency-free fallback.
 *
 * Run from the project root (the folder holding build.sbt):
 *   java dashboard\Server.java 8080        (JDK 11+, single-file mode)
 *   javac -encoding UTF-8 -d dashboard\.build dashboard\Server.java
 *   java -cp dashboard\.build Server 8080  (JDK 8)
 *
 * Keep this file ASCII-only.
 */
public class Server {

    public static void main(String[] args) throws Exception {
        int port = 8080;
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0].trim());
            } catch (NumberFormatException e) {
                port = 8080;
            }
        }

        final Path root = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize();

        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        } catch (IOException busy) {
            port = 8081;
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
        }
        final int boundPort = port;

        server.createContext("/", new HttpHandler() {
            public void handle(HttpExchange ex) throws IOException {
                String raw = ex.getRequestURI().getPath();
                if (raw == null || raw.isEmpty()) {
                    raw = "/";
                }
                if ("/".equals(raw)) {
                    ex.getResponseHeaders().set("Location", "/dashboard/index.html");
                    ex.sendResponseHeaders(302, -1);
                    ex.close();
                    return;
                }
                Path target = root.resolve(raw.substring(1)).normalize();
                if (!target.startsWith(root)) {
                    send(ex, 403, "text/plain; charset=utf-8", "forbidden".getBytes("UTF-8"));
                    return;
                }
                if (Files.isDirectory(target)) {
                    target = target.resolve("index.html");
                }
                if (!Files.isRegularFile(target)) {
                    send(ex, 404, "text/plain; charset=utf-8",
                            ("not found: " + raw).getBytes("UTF-8"));
                    return;
                }
                byte[] body = Files.readAllBytes(target);
                send(ex, 200, mimeOf(target.getFileName().toString()), body);
            }
        });
        server.setExecutor(null);
        server.start();

        System.out.println("===========================================================");
        System.out.println(" dashboard : http://127.0.0.1:" + boundPort + "/dashboard/index.html");
        System.out.println(" root      : " + root);
        System.out.println(" stop      : Ctrl+C or close this window");
        System.out.println("===========================================================");
    }

    private static void send(HttpExchange ex, int code, String type, byte[] body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", type);
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        if ("HEAD".equalsIgnoreCase(ex.getRequestMethod())) {
            ex.sendResponseHeaders(code, -1);
            ex.close();
            return;
        }
        ex.sendResponseHeaders(code, body.length);
        OutputStream os = ex.getResponseBody();
        os.write(body);
        os.close();
    }

    private static String mimeOf(String name) {
        String n = name.toLowerCase();
        if (n.endsWith(".html") || n.endsWith(".htm")) {
            return "text/html; charset=utf-8";
        }
        if (n.endsWith(".css")) {
            return "text/css; charset=utf-8";
        }
        if (n.endsWith(".js")) {
            return "application/javascript; charset=utf-8";
        }
        if (n.endsWith(".json")) {
            return "application/json; charset=utf-8";
        }
        if (n.endsWith(".png")) {
            return "image/png";
        }
        if (n.endsWith(".jpg") || n.endsWith(".jpeg")) {
            return "image/jpeg";
        }
        if (n.endsWith(".svg")) {
            return "image/svg+xml";
        }
        if (n.endsWith(".ico")) {
            return "image/x-icon";
        }
        if (n.endsWith(".woff2")) {
            return "font/woff2";
        }
        return "application/octet-stream";
    }
}
