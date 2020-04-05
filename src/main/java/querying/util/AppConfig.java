package querying.util;

import java.util.Arrays;
import java.util.List;

public class AppConfig {
    public static List<String> SUPPORTED_AGGR = Arrays.asList("avg", "sum", "count");
    public static List<String> SUPPORTED_INTERVALS = Arrays.asList("1day", "1week", "1month", "all");
}
