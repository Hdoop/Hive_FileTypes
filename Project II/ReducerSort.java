package advancemr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class ReducerSort implements Writable ,WritableComparable<ReducerSort>
{

	@Override
	public int compareTo(ReducerSort o) {
		// TODO Auto-generated method stub
		int x =this.compareTo(o);
		if(x ==0)
		{
			//x=this.
		}
		return 0;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
