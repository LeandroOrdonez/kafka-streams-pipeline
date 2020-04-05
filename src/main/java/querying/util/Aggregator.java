package querying.util;

import model.Aggregate;

import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

public class Aggregator<T> implements Consumer<Map<T, Aggregate>> {
    TreeMap<T, Aggregate> aggregateMap = new TreeMap<>();

    public TreeMap<T, Aggregate> getAggregateMap() {
        return aggregateMap;
    }

    @Override
    public void accept(Map<T, Aggregate> aggMap) {
        for(Map.Entry<T, Aggregate> entry : aggMap.entrySet()) {
            aggregateMap.merge(entry.getKey(), entry.getValue(),
                    (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum) / (a1.count + a2.count))
            );
        }
    }

    public void combine(Aggregator<T> other) {
        other.aggregateMap.forEach(
                (ts, agg) -> aggregateMap.merge(ts, agg,
                        (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum)/(a1.count + a2.count)))
        );
    }
}
