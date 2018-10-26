package io.github.trierbo.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class TextPair implements WritableComparable<TextPair> {
    private Text word;
    private Text country;

    public TextPair() {
        this.word = new Text();
        this.country = new Text();
    }

    public TextPair(Text word, Text country) {
        this.word = word;
        this.country = country;
    }

    public TextPair(String word, String country) {
        this.word = new Text(word);
        this.country = new Text(country);
    }

    public Text getWord() {
        return word;
    }

    public void setWord(Text word) {
        this.word = word;
    }

    public Text getCountry() {
        return country;
    }

    public void setCountry(Text country) {
        this.country = country;
    }

    public void write(DataOutput dataOutput) throws IOException {
        word.write(dataOutput);
        country.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        word.readFields(dataInput);
        country.readFields(dataInput);
    }

    public boolean equals(Object o) {
        if (o instanceof TextPair) {
            TextPair temp = (TextPair)o;
            return word.equals(temp.word) && country.equals(temp.country);
        }
        return false;
    }

    public int hashCode() {
        return Objects.hash(word, country);
    }

    public int compareTo( TextPair textPair) {
        int comp = word.compareTo(textPair.word);
        if (comp != 0) {
            return comp;
        }
        return country.compareTo(textPair.country);
    }

    public String toString() {
        return word + "\t" + country;
    }
}
