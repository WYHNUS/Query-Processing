package qp.operators;

import java.io.*;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

public class HashingClass
{
	int batchsize;
	int mod; /// mod no.
	boolean eos; ///end of input stream
	Schema schema;
	String prefix;
	Operator table;
	String[]fileNames;

	public HashingClass(Operator table , int num, String pre) 
	{
		this.schema = table.getSchema();
		this.mod = num;
		this.table = table;
		prefix = pre;
		fileNames = new String[num];
		eos = false;
	}

	///hash table into partitions
	public String[] hashTable(int attrIndex) 
	{
		int parIndex = 0; ///index for partition
		int tuplesize = schema.getTupleSize();
		batchsize = Batch.getPageSize() / tuplesize;
		Batch tmpBuffer = new Batch(batchsize);
		Batch[] parBuffer = new Batch[mod];
		for (int k = 0; k < parBuffer.length; k++) 
		{
			parBuffer[k] = new Batch(batchsize);
		}

		ObjectOutputStream[] out = new ObjectOutputStream[mod];

		///one output file for each partition
		for( parIndex = 0 ; parIndex < mod ; parIndex++ )
		{
			fileNames[parIndex] = prefix + String.valueOf(parIndex);
			try 
			{
				out[parIndex] = new ObjectOutputStream(new FileOutputStream(fileNames[parIndex]+ ".partition"));
			} 
			catch (Exception e) 
			{
				System.err.println(e.toString());
				System.exit(1);
			}
		}

		/// read a page of table and write into diff partitions, process page by page
		while (!eos) 
		{

			tmpBuffer = table.next(); /// read a page

			/// if reach the end of table, output all buffers
			if (tmpBuffer == null) 
			{
				eos = true;
				/// write out each buffer of partition if not empty
				for (parIndex = 0; parIndex < mod; parIndex++) 
				{
					try 
					{
						if (!parBuffer[parIndex].isEmpty()) 
						{
								out[parIndex].writeObject(parBuffer[parIndex]);
						}
						out[parIndex].close();
					} 
					catch (Exception e) 
					{
						System.err.println(e.toString());
						System.exit(1);
					}
				}
				return fileNames;
			}
			
			///put tuples of the page to corresponding partition buffers
			for (int i = 0; i < tmpBuffer.size(); i++) 
			{
				Tuple rec = tmpBuffer.elementAt(i);
				parIndex = Integer.valueOf(String.valueOf(rec.dataAt(attrIndex))) % mod;
				parBuffer[parIndex].add(rec);

				///output partition buffer if full
				if (parBuffer[parIndex].isFull())
				{
					try 
					{
						out[parIndex].writeObject(parBuffer[parIndex]);
					} 
					catch (Exception e) 
					{
						System.err.println(e.toString());
						System.exit(1);
					}
					/// empty the partition buffer after output 
					parBuffer[parIndex] = new Batch(batchsize);
				}
			}

		}
		return fileNames;
	}
}
