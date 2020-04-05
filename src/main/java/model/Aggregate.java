package model;

public class Aggregate extends Object{
    public Long count;
    public Double sum;
    public Double avg;

    public Aggregate() {
    }

    public Aggregate(Long count, Double sum, Double avg) {
        this.count = count;
        this.sum = sum;
        this.avg = avg;
    }
    
    @Override
    public String toString() {
        return "Aggregate{" +
                "count=" + count +
                ", sum=" + sum +
                ", avg=" + avg +
                '}';
    }
}
