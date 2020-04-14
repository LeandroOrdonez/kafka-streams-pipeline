package querying;

import com.github.davidmoten.geo.Base32;
import com.github.davidmoten.geo.GeoHash;
import model.Aggregate;
import model.ErrorMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.*;
import org.glassfish.jersey.jackson.JacksonFeature;
import querying.util.Aggregator;
import querying.util.HostStoreInfo;
import querying.util.MetadataService;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

public class QueryingController {

    private final KafkaStreams streams;
    private final MetadataService metadataService;
    private final HostInfo hostInfo;
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    public static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd:HHmmss:SSS");

    public QueryingController(KafkaStreams streams, HostInfo hostInfo) {
        this.streams = streams;
        this.metadataService = new MetadataService(streams);
        this.hostInfo = hostInfo;
    }

    public TreeMap<Long, Aggregate> solveSpatialQuery(String aggregate, List<String> geohashes, Long fromDate, Long toDate, int geohashPrecision, Boolean local) {
        final String viewStoreName = "view-gh" + geohashPrecision + "-hour";
        if (local) { // Answering request for LOCAL state
            Aggregator<Long> aggCollect = geohashes.stream()
                    .map(gh -> getLocalAggregates4Range(viewStoreName, gh, fromDate <= 0 ? null : fromDate, toDate <= 0 ? null : toDate))
                    .collect(Aggregator<Long>::new, Aggregator<Long>::accept, Aggregator<Long>::combine);
            return aggCollect.getAggregateMap();
        } else { // Answering request for GLOBAL state
            final List<HostStoreInfo> hosts = metadataService.streamsMetadataForStore(viewStoreName);
            Aggregator<Long> aggCollect = hosts.stream()
//                    .peek(host -> System.out.println(String.format("[solveSpatialQuery] Current host: %s:%s", host.getHost(), host.getPort())))
                    .map(host -> getAggregates4GeohashList(host, viewStoreName, aggregate, geohashes,"", fromDate, toDate, geohashPrecision))
                    .collect(Aggregator<Long>::new, Aggregator<Long>::accept, Aggregator<Long>::combine);
            return aggCollect.getAggregateMap();
        }
    }

    public TreeMap<Long, Aggregate> solveSpatioTemporalQuery(String aggregate, List<String> geohashes, String interval, Long fromDate, int geohashPrecision, Boolean local) {
        final String viewStoreName = "view-gh" + geohashPrecision + "-hour";
        if (local) { // Answering request for LOCAL state
            Long to = fromDate <= 0 ? System.currentTimeMillis() : fromDate;
            Long from = getFromDate(to, interval);
            Aggregator<Long> aggCollect = geohashes.stream()
                    .map(gh -> getLocalAggregates4Range(viewStoreName, gh, from, to))
                    .collect(Aggregator<Long>::new, Aggregator<Long>::accept, Aggregator<Long>::combine);
            return aggCollect.getAggregateMap();
        } else { // Answering request for GLOBAL state
            final List<HostStoreInfo> hosts = metadataService.streamsMetadataForStore(viewStoreName);
            Aggregator<Long> aggCollect = hosts.stream()
//                    .peek(host -> System.out.println(String.format("[solveSpatioTemporalQuery] Current host: %s:%s", host.getHost(), host.getPort())))
                    .map(host -> getAggregates4GeohashList(host, viewStoreName, aggregate, geohashes, interval, fromDate, -1L, geohashPrecision))
                    .collect(Aggregator<Long>::new, Aggregator<Long>::accept, Aggregator<Long>::combine);
            return aggCollect.getAggregateMap();
        }
    }

    public TreeMap<String, Aggregate> solveTimeQuery(String aggregate, Long timestamp, int geohashPrecision, List<Double> bbox, Boolean local) {
        Long ts = truncateTS(timestamp, ChronoUnit.HOURS); // truncate the provided timestamp to the exact hour
        final String viewStoreName = "view-gh" + geohashPrecision + "-hour";
        if (local) { // Answering request for LOCAL state
            TreeMap<String, Aggregate> aggregateReadings = new TreeMap<>();
            aggregateReadings.putAll(getLocalAggregates4Timestamp(viewStoreName, geohashPrecision, timestamp, bbox));
            return aggregateReadings;
        } else { // Answering request for GLOBAL state
            final List<HostStoreInfo> hosts = metadataService.streamsMetadataForStore(viewStoreName);
            Aggregator<String> aggCollect = hosts.stream()
//                    .peek(host -> System.out.println(String.format("[solveTimeQuery] Current host: %s:%s", host.getHost(), host.getPort())))
                    .map(host -> getAggregates4Timestamp(host, viewStoreName, aggregate, ts, geohashPrecision, bbox))
                    .collect(Aggregator<String>::new, Aggregator<String>::accept, Aggregator<String>::combine);
            return aggCollect.getAggregateMap();
        }
    }

    public Map<Long, Aggregate> getAggregates4GeohashList(HostStoreInfo host, String viewStoreName, String aggregate, List<String> geohashes, String interval, Long fromDate, Long toDate, int geohashPrecision) {
        if (!thisHost(host)) {
            try {
                return client.target(String.format("http://%s:%d/api/temperature/aggregate/%s/history",
                        host.getHost(),
                        host.getPort(),
                        aggregate))
                        .queryParam("geohashes", String.join(",", geohashes))
                        .queryParam("interval", interval)
                        .queryParam("gh_precision", geohashPrecision)
                        .queryParam("from", fromDate)
                        .queryParam("to", toDate)
                        .queryParam("local", true)
                        .request(MediaType.APPLICATION_JSON_TYPE)
                        .get(new GenericType<TreeMap<Long, Aggregate>>() {});
            } catch (Exception e) {
                e.printStackTrace();
                Throwable rootCause = ExceptionUtils.getRootCause(e);
                rootCause.printStackTrace();
                Response errorResp = Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new ErrorMessage(rootCause.getMessage(), 500))
                        .build();
                throw new WebApplicationException(errorResp);
            }
        } else {
            // look in the local store
            Long to, from;
            if (!interval.isEmpty()) {
                to = fromDate <= 0 ? System.currentTimeMillis() : fromDate;
                from = getFromDate(to, interval);
            } else {
                to = toDate <= 0 ? null : toDate;
                from = fromDate <= 0 ? null : fromDate;
            }
            Aggregator<Long> aggCollect = geohashes.stream()
                    .map(gh -> getLocalAggregates4Range(viewStoreName, gh, from, to))
                    .collect(Aggregator<Long>::new, Aggregator<Long>::accept, Aggregator<Long>::combine);
            return aggCollect.getAggregateMap();
        }
    }

    public Map<String, Aggregate> getAggregates4Timestamp(HostStoreInfo host, String viewStoreName, String aggregate, Long timestamp, int geohashPrecision, List<Double> bbox) {
        if (!thisHost(host)) {
            try { //Forwarding request to host.getHost():host.getPort()
                return client.target(String.format("http://%s:%d/api/temperature/aggregate/%s/snapshot",
                        host.getHost(),
                        host.getPort(),
                        aggregate))
                        .queryParam("gh_precision", geohashPrecision)
                        .queryParam("ts", timestamp)
                        .queryParam("bbox", String.join(",", bbox.stream().map(c -> c.toString()).collect(Collectors.toList())))
                        .queryParam("local", true)
                        .request(MediaType.APPLICATION_JSON_TYPE)
                        .get(new GenericType<TreeMap<String, Aggregate>>() {});
            } catch (Exception e) {
                e.printStackTrace();
                Throwable rootCause = ExceptionUtils.getRootCause(e);
                rootCause.printStackTrace();
                Response errorResp = Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                        .entity(new ErrorMessage(rootCause.getMessage(), 500))
                        .build();
                throw new WebApplicationException(errorResp);
            }
        } else {
            // look in the local store
            TreeMap<String, Aggregate> aggregateReadings = new TreeMap<>();
            aggregateReadings.putAll(getLocalAggregates4Timestamp(viewStoreName, geohashPrecision, timestamp, bbox));
            return aggregateReadings;
        }
    }

    public Map<Long, Aggregate> getLocalAggregates4Range(String storeName, String geohashPrefix, Long from, Long to) {
        final ReadOnlyWindowStore<String, Aggregate> viewStore = streams.store(storeName,
                QueryableStoreTypes.windowStore());
        // final String fromK = geohashPrefix + "#" + (from != null ? toFormattedTimestamp(from, ZoneId.systemDefault()) : "");
        // final String toK = geohashPrefix + "#" + (to != null ? toFormattedTimestamp(to, ZoneId.systemDefault()) : toFormattedTimestamp(System.currentTimeMillis(), ZoneId.systemDefault()));
        long fromk = from != null ? from : 0L;
        long tok = to != null ? to : System.currentTimeMillis();
        Map<Long, Aggregate> aggregateReadings = new TreeMap<>();
        WindowStoreIterator<Aggregate> iterator =  viewStore.fetch(geohashPrefix, Instant.ofEpochMilli(fromk), Instant.ofEpochMilli(tok)); //.range(fromK, toK);
        while (iterator.hasNext()) {
            KeyValue<Long, Aggregate> aggFromStore = iterator.next();
            long windowTimestamp = aggFromStore.key;
            Aggregate windowValue = aggFromStore.value;
//            System.out.println("Aggregate of '" + geohashPrefix + "' @ time " + windowTimestamp + " is " + windowValue);
            Aggregate agg = new Aggregate(aggFromStore.value.count, aggFromStore.value.sum, aggFromStore.value.avg);
            aggregateReadings.merge(windowTimestamp, agg,
                    (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum)/(a1.count + a2.count)));
        }
        iterator.close();
        return aggregateReadings;
    }

    public Map<String, Aggregate> getLocalAggregates4Timestamp(String storeName, int geohashPrecision, Long ts, List<Double> bbox) {
        List<String> bboxGeohashes = new ArrayList<>(GeoHash.coverBoundingBox(bbox.get(0), bbox.get(1), bbox.get(2), bbox.get(3)).getHashes());
        Aggregator<String> aggCollect = bboxGeohashes.stream()
                .map(gh -> getLocalAggregates4TimestampAndGHPrefix(storeName, geohashPrecision, ts, gh))
                .collect(Aggregator<String>::new, Aggregator<String>::accept, Aggregator<String>::combine);
        return aggCollect.getAggregateMap();
    }

    public Map<String, Aggregate> getLocalAggregates4TimestampAndGHPrefix(String storeName, int geohashPrecision, Long ts, String geohashPrefix) {
        final ReadOnlyWindowStore<String, Aggregate> viewStore = streams.store(storeName,
                QueryableStoreTypes.windowStore());
        String truncateGHPrefix = StringUtils.truncate(geohashPrefix, geohashPrecision);
        Map<String, Aggregate> aggregateReadings = new TreeMap<>();
        for (long i = 0L; i < Math.pow(32, geohashPrecision - truncateGHPrefix.length()); i++) {
            String ghPart = geohashPrecision == truncateGHPrefix.length() ? truncateGHPrefix : truncateGHPrefix + Base32.encodeBase32(i, geohashPrecision - truncateGHPrefix.length());
            String searchKey = ghPart; // + "#" + toFormattedTimestamp(ts, ZoneId.systemDefault());
//            System.out.println("[getLocalAggregates4TimestampAndGHPrefix] searchKey=" + searchKey);
//            System.out.println("[getLocalAggregates4TimestampAndGHPrefix] ts=" + ts);
            Aggregate aggregateT = viewStore.fetch(searchKey, ts);
            if (aggregateT != null) {
                // System.out.println("Aggregate for " + ghPart + ": " + aggregateT);
                Aggregate agg = new Aggregate(aggregateT.count, aggregateT.sum, aggregateT.avg);
                aggregateReadings.merge(searchKey, agg,
                        (a1, a2) -> new Aggregate(a1.count + a2.count, a1.sum + a2.sum, (a1.sum + a2.sum)/(a1.count + a2.count)));
            }
        }
        return aggregateReadings;
    }

    private Long getFromDate(long toDate, String interval) {
        //"1day", "1week", "1month", "all"
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(toDate);
        switch (interval) {
            case "1week":
                c.add(Calendar.DATE, -7);
                return c.getTime().getTime();
            case "1month":
                c.add(Calendar.MONTH, -1);
                return c.getTime().getTime();
            case "all":
                c.add(Calendar.YEAR, -30);
                return c.getTime().getTime();
            default:
                c.add(Calendar.DATE, -1);
                return c.getTime().getTime();
        }
    }

    private Long truncateTS(Long timestamp, ChronoUnit unit) {
        try{
            ZonedDateTime tsDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
            return tsDate.truncatedTo(unit).toInstant().toEpochMilli();
        } catch (Exception e) {
            e.printStackTrace();
            return timestamp;
        }

    }

    private static String toFormattedTimestamp(Long timestamp, ZoneId zoneId) {
        return ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), zoneId).toLocalDateTime().format(DATE_TIME_FORMATTER);
    }

    public boolean thisHost(final HostStoreInfo host) {
        return host.getHost().equals(hostInfo.host()) &&
                host.getPort() == hostInfo.port();
    }
}
