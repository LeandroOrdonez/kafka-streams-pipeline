package model;

public class AggregateTuple extends Aggregate {
    public String key;
    public String geohash;
    public Long timestamp;

    public AggregateTuple() {
        super();
    }

    public AggregateTuple(String key, String gh, Long ts, Long count, Double sum, Double avg) {
        super(count, sum, avg);
        this.key = key;
        this.geohash = gh;
        this.timestamp = ts;
    }

    @Override
    public String toString() {
        return "AggregateTuple{" +
                "key='" + key + '\'' +
                ", geohash='" + geohash + '\'' +
                ", timestamp=" + timestamp +
                ", count=" + count +
                ", sum=" + sum +
                ", avg=" + avg +
                '}';
    }
}
