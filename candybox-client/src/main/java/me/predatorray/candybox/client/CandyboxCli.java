/*
 * Copyright (c) 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package me.predatorray.candybox.client;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import me.predatorray.candybox.common.exception.CandyboxException;
import me.predatorray.candybox.protocol.transport.TcpTransport;

/**
 * A thin command-line front end over {@link CandyboxClient}, talking directly to a single node
 * ({@code host:port}). Intended for smoke tests, operations and demos — not a streaming bulk-load
 * tool (objects are buffered in memory, per the client's current limitation).
 *
 * <pre>
 *   candybox [-s host:port] &lt;command&gt; [args]
 *
 *   list-boxes
 *   create-box  &lt;box&gt; [partitions]
 *   delete-box  &lt;box&gt; [--force]
 *   head-box    &lt;box&gt;
 *   put          &lt;box&gt; &lt;key&gt; [file]      # file or stdin; --content-type T, --meta k=v (repeatable)
 *   get          &lt;box&gt; &lt;key&gt; [outfile]   # outfile or stdout
 *   head         &lt;box&gt; &lt;key&gt;
 *   delete       &lt;box&gt; &lt;key&gt;
 *   copy         &lt;box&gt; &lt;src&gt; &lt;dst&gt;       # zero-copy, same Box
 *   rename       &lt;box&gt; &lt;src&gt; &lt;dst&gt;       # zero-copy move, same Box
 *   delete-range &lt;box&gt; [prefix] [--start K] [--end K]   # range tombstone
 *   list         &lt;box&gt; [prefix] [--max N] [--start-after K] [--start K] [--end K] [--reverse]
 * </pre>
 *
 * Defaults to {@code 127.0.0.1:9709}; override with {@code -s/--server} or {@code CANDYBOX_SERVER}.
 */
public final class CandyboxCli {

    private static final int DEFAULT_PORT = 9709;

    private CandyboxCli() {
    }

    public static void main(String[] args) {
        System.exit(run(args, System.out, System.err));
    }

    /** Runs one command; returns the process exit code. Separated from {@link #main} for testing. */
    static int run(String[] argv, PrintStream out, PrintStream err) {
        List<String> args = new ArrayList<>(List.of(argv));

        String server = firstNonBlank(System.getenv("CANDYBOX_SERVER"), "127.0.0.1:" + DEFAULT_PORT);
        // Pull out the global -s/--server option wherever it appears.
        for (int i = 0; i < args.size(); i++) {
            String a = args.get(i);
            if (a.equals("-s") || a.equals("--server")) {
                if (i + 1 >= args.size()) {
                    err.println("Missing value for " + a);
                    return 2;
                }
                server = args.remove(i + 1);
                args.remove(i);
                i--;
            }
        }

        if (args.isEmpty() || args.get(0).equals("-h") || args.get(0).equals("--help")
                || args.get(0).equals("help")) {
            printUsage(out);
            return args.isEmpty() ? 2 : 0;
        }

        String host;
        int port;
        try {
            int colon = server.lastIndexOf(':');
            if (colon < 0) {
                throw new IllegalArgumentException("server must be host:port, got: " + server);
            }
            host = server.substring(0, colon);
            port = Integer.parseInt(server.substring(colon + 1));
        } catch (RuntimeException e) {
            err.println("Invalid --server: " + e.getMessage());
            return 2;
        }

        String command = args.remove(0);
        try (TcpTransport transport = new TcpTransport();
             CandyboxClient client = new CandyboxClient(transport, host, port)) {
            return dispatch(command, args, client, out, err);
        } catch (CandyboxException e) {
            err.println("error: " + e.getMessage());
            return 1;
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            return 2;
        }
    }

    private static int dispatch(String command, List<String> args, CandyboxClient client,
                                PrintStream out, PrintStream err) {
        switch (command) {
            case "list-boxes" -> {
                client.listBoxes().forEach(out::println);
                return 0;
            }
            case "create-box" -> {
                String box = requireArg(args, 0, "box");
                int partitions = args.size() > 1 ? Integer.parseInt(args.get(1)) : 0;
                client.createBox(box, partitions);
                return 0;
            }
            case "delete-box" -> {
                boolean force = args.remove("--force");
                client.deleteBox(requireArg(args, 0, "box"), force);
                return 0;
            }
            case "head-box" -> {
                boolean exists = client.headBox(requireArg(args, 0, "box"));
                out.println(exists ? "exists" : "absent");
                return exists ? 0 : 1;
            }
            case "put" -> {
                return put(args, client, err);
            }
            case "get" -> {
                return get(args, client, out, err);
            }
            case "head" -> {
                CandyboxClient.CandyInfo info =
                        client.headCandy(requireArg(args, 0, "box"), requireArg(args, 1, "key"));
                out.println("contentLength: " + info.contentLength());
                out.println("contentType:   " + info.contentType());
                out.println("crc32c:        " + Integer.toHexString(info.crc32c()));
                out.println("createdAt:     " + info.createdAtMillis());
                info.userMetadata().forEach((k, v) -> out.println("meta." + k + ": " + v));
                return 0;
            }
            case "delete" -> {
                client.deleteCandy(requireArg(args, 0, "box"), requireArg(args, 1, "key"));
                return 0;
            }
            case "copy" -> {
                client.copyCandy(requireArg(args, 0, "box"), requireArg(args, 1, "src"),
                        requireArg(args, 2, "dst"), null);
                return 0;
            }
            case "rename" -> {
                client.renameCandy(requireArg(args, 0, "box"), requireArg(args, 1, "src"),
                        requireArg(args, 2, "dst"), null);
                return 0;
            }
            case "delete-range" -> {
                return deleteRange(args, client);
            }
            case "list" -> {
                return list(args, client, out);
            }
            default -> {
                err.println("Unknown command: " + command);
                printUsage(err);
                return 2;
            }
        }
    }

    private static int put(List<String> args, CandyboxClient client, PrintStream err) {
        String contentType = takeOption(args, "--content-type", "application/octet-stream");
        Map<String, String> meta = new LinkedHashMap<>();
        String kv;
        while ((kv = takeOptionalOption(args, "--meta")) != null) {
            int eq = kv.indexOf('=');
            if (eq < 0) {
                err.println("--meta expects key=value, got: " + kv);
                return 2;
            }
            meta.put(kv.substring(0, eq), kv.substring(eq + 1));
        }
        String box = requireArg(args, 0, "box");
        String key = requireArg(args, 1, "key");
        byte[] data;
        try {
            if (args.size() > 2 && !args.get(2).equals("-")) {
                data = Files.readAllBytes(Path.of(args.get(2)));
            } else {
                data = System.in.readAllBytes(); // file omitted or "-" ⇒ read stdin
            }
        } catch (IOException e) {
            err.println("Failed to read input: " + e.getMessage());
            return 1;
        }
        client.putCandy(box, key, data, contentType, meta, null);
        return 0;
    }

    private static int get(List<String> args, CandyboxClient client, PrintStream out, PrintStream err) {
        String box = requireArg(args, 0, "box");
        String key = requireArg(args, 1, "key");
        byte[] data = client.getCandy(box, key);
        if (args.size() > 2 && !args.get(2).equals("-")) {
            try {
                Files.write(Path.of(args.get(2)), data);
            } catch (IOException e) {
                err.println("Failed to write output: " + e.getMessage());
                return 1;
            }
        } else {
            try {
                OutputStream raw = out;
                raw.write(data);
                raw.flush();
            } catch (IOException e) {
                err.println("Failed to write output: " + e.getMessage());
                return 1;
            }
        }
        return 0;
    }

    private static int list(List<String> args, CandyboxClient client, PrintStream out) {
        int max = Integer.parseInt(takeOption(args, "--max", "1000"));
        String startAfter = takeOptionalOption(args, "--start-after");
        String startKey = takeOptionalOption(args, "--start");
        String endKey = takeOptionalOption(args, "--end");
        boolean reverse = args.remove("--reverse");
        String box = requireArg(args, 0, "box");
        String prefix = args.size() > 1 ? args.get(1) : "";
        CandyboxClient.Listing listing = client.listCandies(box, prefix, startKey, endKey, startAfter,
                reverse, max);
        for (CandyboxClient.Listing.Entry e : listing.entries()) {
            out.println(e.key() + "\t" + e.contentLength() + "\t" + e.createdAtMillis());
        }
        if (listing.isTruncated()) {
            out.println("# truncated; next --start-after " + listing.nextStartAfter());
        }
        return 0;
    }

    private static int deleteRange(List<String> args, CandyboxClient client) {
        String startKey = takeOptionalOption(args, "--start");
        String endKey = takeOptionalOption(args, "--end");
        String box = requireArg(args, 0, "box");
        if (args.size() > 1) {
            client.deleteRangeByPrefix(box, args.get(1)); // positional prefix form
        } else {
            client.deleteRange(box, startKey, endKey); // [--start, --end) window form
        }
        return 0;
    }

    // ---- arg helpers -----------------------------------------------------------------------

    private static String requireArg(List<String> args, int index, String name) {
        if (index >= args.size()) {
            throw new IllegalArgumentException("Missing required argument: <" + name + ">");
        }
        return args.get(index);
    }

    /** Removes {@code --opt value} from args and returns the value, or {@code def} if absent. */
    private static String takeOption(List<String> args, String opt, String def) {
        String v = takeOptionalOption(args, opt);
        return v != null ? v : def;
    }

    private static String takeOptionalOption(List<String> args, String opt) {
        int i = args.indexOf(opt);
        if (i < 0) {
            return null;
        }
        if (i + 1 >= args.size()) {
            throw new IllegalArgumentException("Missing value for " + opt);
        }
        String value = args.get(i + 1);
        args.remove(i + 1);
        args.remove(i);
        return value;
    }

    private static String firstNonBlank(String a, String b) {
        return (a != null && !a.isBlank()) ? a : b;
    }

    private static void printUsage(PrintStream out) {
        out.println("""
                Usage: candybox [-s host:port] <command> [args]

                Commands:
                  list-boxes
                  create-box   <box> [partitions]
                  delete-box   <box> [--force]
                  head-box     <box>
                  put          <box> <key> [file]    file or stdin; --content-type T, --meta k=v
                  get          <box> <key> [outfile]  outfile or stdout
                  head         <box> <key>
                  delete       <box> <key>
                  copy         <box> <src> <dst>     zero-copy, same Box
                  rename       <box> <src> <dst>     zero-copy move, same Box
                  delete-range <box> [prefix] [--start K] [--end K]   single range tombstone
                  list         <box> [prefix] [--max N] [--start-after K] [--start K] [--end K] [--reverse]

                Server defaults to 127.0.0.1:9709 (override with -s/--server or CANDYBOX_SERVER).""");
    }
}
