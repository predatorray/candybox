package me.predatorray.candybox.common;

import java.util.regex.Pattern;
import me.predatorray.candybox.common.exception.ValidationException;

/**
 * A validated Box name (the S3 "bucket" name). S3-like rules: 3–63 characters, lowercase letters,
 * digits and hyphens only, not starting or ending with a hyphen. The constraints keep names safely
 * encodable in ZooKeeper paths and ledger metadata.
 *
 * @param value the validated name
 */
public record BoxName(String value) implements Comparable<BoxName> {

    public static final int MIN_LENGTH = 3;
    public static final int MAX_LENGTH = 63;
    private static final Pattern PATTERN = Pattern.compile("[a-z0-9]([a-z0-9-]*[a-z0-9])?");

    public BoxName {
        if (value == null) {
            throw new ValidationException("Box name must not be null");
        }
        int len = value.length();
        if (len < MIN_LENGTH || len > MAX_LENGTH) {
            throw new ValidationException("Box name length must be " + MIN_LENGTH + ".." + MAX_LENGTH
                    + " chars: '" + value + "'");
        }
        if (!PATTERN.matcher(value).matches()) {
            throw new ValidationException("Box name must match [a-z0-9-], not start/end with '-': '"
                    + value + "'");
        }
    }

    public static BoxName of(String value) {
        return new BoxName(value);
    }

    @Override
    public int compareTo(BoxName o) {
        return value.compareTo(o.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
