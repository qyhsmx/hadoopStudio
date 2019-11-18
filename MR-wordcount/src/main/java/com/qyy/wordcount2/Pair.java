package com.qyy.wordcount2;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author qyhsmx@outlook.com
 * @date
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Pair implements WritableComparable<Pair> {
    private String value;
    private int num;

    public int compareTo(Pair o) {
        //先比较value
        int i = this.getValue().compareTo(o.getValue());
        //在比较num
        if(i==0) {
            return this.getNum() > o.getNum() ? 1 : (this.getNum() == o.getNum() ? 0 : -1);
        }
        return i;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(value);
        dataOutput.writeInt(num);
    }

    public void readFields(DataInput dataInput) throws IOException {
        value = dataInput.readUTF();
        num = dataInput.readInt();
    }
}
