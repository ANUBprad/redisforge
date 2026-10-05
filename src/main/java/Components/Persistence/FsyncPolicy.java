package Components.Persistence;

public enum FsyncPolicy {
    /** Every append is forced to disk before the mutation is reported as done. */
    ALWAYS,
    /** Appends are written immediately and forced about once a second. */
    EVERYSEC,
    /** Appends are written but never explicitly forced. */
    NO;

    public static FsyncPolicy parse(String value) {
        if (value == null) {
            throw new IllegalArgumentException("appendfsync must be one of always, everysec or no");
        }
        switch (value.trim().toLowerCase()) {
            case "always":
                return ALWAYS;
            case "everysec":
                return EVERYSEC;
            case "no":
                return NO;
            default:
                throw new IllegalArgumentException(
                        "unknown appendfsync policy '" + value + "', expected always, everysec or no");
        }
    }
}