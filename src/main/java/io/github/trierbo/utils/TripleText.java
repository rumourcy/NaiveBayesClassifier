package io.github.trierbo.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TripleText implements WritableComparable<TripleText> {
    private Text file;
    private Text country;
    private Text pred;

    public TripleText() {
        file = new Text();
        country = new Text();
        pred = new Text();
    }

    public TripleText(Text file, Text country, Text pred) {
        this.file = file;
        this.country = country;
        this.pred = pred;
    }

    public TripleText(String file, String country, String pred) {
        this.file = new Text(file);
        this.country = new Text(country);
        this.pred = new Text(pred);
    }

    public void write(DataOutput dataOutput) throws IOException {
        file.write(dataOutput);
        country.write(dataOutput);
        pred.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        file.readFields(dataInput);
        country.readFields(dataInput);
        pred.readFields(dataInput);
    }

    public boolean equals(Object o) {
        if (o instanceof TripleText) {
            TripleText temp = (TripleText)o;
            return file.equals(temp.file) && country.equals(temp.country) && pred.equals(temp.pred);
        }
        return false;
    }

    public int compareTo(TripleText tripleText) {
        int comp = file.compareTo(tripleText.file);
        if (comp != 0) {
            return comp;
        }
        comp = country.compareTo(tripleText.country);
        if (comp != 0) {
            return comp;
        }
        return pred.compareTo(tripleText.pred);
    }

    public int hashCode() {
        return Objects.hash(file, country, pred);
    }

    public String toString() {
        return file + "\t" + country + "\t" + pred;
    }
}
