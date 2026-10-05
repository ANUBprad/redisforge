package Components.Persistence;

import java.io.IOException;

/**
 * Thrown when the append only file holds bytes that cannot be replayed, for
 * example a complete frame that is not valid RESP in the middle of the file.
 * Startup fails instead of silently restoring an arbitrary subset of the data.
 */
public class AppendOnlyCorruptedException extends IOException {
    public AppendOnlyCorruptedException(String message) {
        super(message);
    }
}