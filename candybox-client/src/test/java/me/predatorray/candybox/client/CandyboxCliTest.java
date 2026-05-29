package me.predatorray.candybox.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * Covers the argument-handling paths of {@link CandyboxCli} that resolve before any network call —
 * usage/help, missing arguments and a malformed {@code --server}. Command execution against a live
 * node is exercised end-to-end in the integration tests.
 */
class CandyboxCliTest {

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();

    private int run(String... args) {
        return CandyboxCli.run(args, new PrintStream(out, true, StandardCharsets.UTF_8),
                new PrintStream(err, true, StandardCharsets.UTF_8));
    }

    private String stdout() {
        return out.toString(StandardCharsets.UTF_8);
    }

    private String stderr() {
        return err.toString(StandardCharsets.UTF_8);
    }

    @Test
    void noArgumentsPrintsUsageAndFails() {
        assertThat(run()).isEqualTo(2);
        assertThat(stdout()).contains("Usage: candybox");
    }

    @Test
    void helpPrintsUsageAndSucceeds() {
        assertThat(run("help")).isEqualTo(0);
        assertThat(stdout()).contains("create-box").contains("list-boxes");
    }

    @Test
    void missingServerValueIsRejected() {
        assertThat(run("-s")).isEqualTo(2);
        assertThat(stderr()).contains("Missing value for -s");
    }

    @Test
    void malformedServerIsRejected() {
        assertThat(run("-s", "no-port", "list-boxes")).isEqualTo(2);
        assertThat(stderr()).contains("Invalid --server");
    }
}
