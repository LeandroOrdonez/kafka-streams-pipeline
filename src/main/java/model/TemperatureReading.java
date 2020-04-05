package model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TemperatureReading {
    private Long timestamp;
    private String sensorId;
    private String geohash;
    private Double tempVal;
    private String tempUnit;

    public TemperatureReading() {
    }

    public TemperatureReading(Long timestamp, String sensorId, String geohash,
                              Double tempVal, String tempUnit) {
        this.timestamp = timestamp;
        this.sensorId = sensorId;
        this.geohash = geohash;
        this.tempVal = tempVal;
        this.tempUnit = tempUnit;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public String getGeohash() {
        return geohash;
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }

    public Double getTempVal() {
        return tempVal;
    }

    public void setTempVal(Double tempVal) {
        this.tempVal = tempVal;
    }

    public String getTempUnit() {
        return tempUnit;
    }

    public void setTempUnit(String tempUnit) {
        this.tempUnit = tempUnit;
    }

    @Override
    public String toString() {
        return "TemperatureReading{" +
                "timestamp=" + timestamp +
                ", sensorId='" + sensorId + '\'' +
                ", geohash='" + geohash + '\'' +
                ", tempVal=" + tempVal +
                ", tempUnit='" + tempUnit + '\'' +
                '}';
    }
}
