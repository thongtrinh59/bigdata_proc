package comp9313.proj1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

//class used for value
public class StringInt implements Writable{
	private Text first;
	private IntWritable second;

	public StringInt() {
		this.first = new Text();
        this.second = new IntWritable(0);
	}

	public StringInt(Text first, IntWritable second) {
		set(first, second);
	}

	public void set(Text left, IntWritable right) {
		this.first = left;
		this.second = right;
	}

	public Text getFirst() {
		return this.first;
	}

	public IntWritable getSecond() {
		return this.second;
	}
	
	public void setFirst(Text first) {
        this.first = first;
    }

    public void setSecond(IntWritable second) {
        this.second = second;
    }

	public void readFields(DataInput in) throws IOException {
		this.first.readFields(in);
		this.second.readFields(in);
	}

	public void write(DataOutput out) throws IOException {	
		this.first.write(out);
        this.second.write(out);
	}
	
	@Override
    public String toString() {
        return this.first.toString() + "\t" + this.second.toString();
    }
}
