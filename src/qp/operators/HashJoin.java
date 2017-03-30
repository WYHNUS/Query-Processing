
package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;

public class HashJoin extends Join 
{
	int batchsize; // number of tuples per out batch
	int leftindex; // index of the join attribute in left table
	int rightindex; // index of the join attribute in right table
	boolean eopl; // reach end of left partition
	boolean eopr; // reach end of right partition
	boolean eobr; // reach the end of right batch 
	int lcurs = 0; // Cursor for left buffer page
	int rcurs = 0; // Cursor for right buffer page
	int bcurs = 0; // partition index
	int linkedlistindex = 0;
	Batch leftbatch = null;
	Batch rightbatch = null;
	Batch outbatch = null;

	String[] leftPartitions; // file name of left table partitions
	String[] rightPartitions; // file name of right table partitions
	ObjectInputStream leftInput;
	ObjectInputStream rightInput;
	String prefix;

	Hashtable<Integer, LinkedList<Tuple>> innerHashTable = null; /// hash table for left partition
	LinkedList<Tuple> linkedlist = null;
	
	public LinkedList<Tuple> put(Hashtable<Integer, LinkedList<Tuple>> innerHashTable, Integer key, LinkedList<Tuple> list) {
		if (innerHashTable.containsKey(key)) {
			LinkedList<Tuple> v = innerHashTable.get(key);
			v.add(list.getFirst());
			return v;
		} else {
			innerHashTable.put(key, list);
			return null;
		}
	}

	public HashJoin(Join jn) 
	{
		super(jn.getLeft(), jn.getRight(), jn.getCondition(), jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
		prefix = "tmphashfile"; 
	}

	public boolean open() 
	{
		int tuplesize = schema.getTupleSize();
		batchsize = Batch.getPageSize() / tuplesize;

		Attribute leftattr = con.getLhs();
		Attribute rightattr = (Attribute) con.getRhs();
		leftindex = left.getSchema().indexOf(leftattr);
		rightindex = right.getSchema().indexOf(rightattr);

		eopl = false;
		eopr = true;
		eobr = true;
		bcurs = 0;
		rcurs = 0;

		if (!left.open())
			return false;
		if (!right.open())
			return false;

		/// partition left table on the join attribute
		HashingClass leftHasher = new HashingClass(left , numBuff - 1, prefix + "_left_");
		leftPartitions = leftHasher.hashTable(leftindex);
		/// partition right table on the join attribute
		HashingClass rightHasher = new HashingClass(right , numBuff - 1, prefix + "_right_");
		rightPartitions = rightHasher.hashTable(rightindex);

		return true;
	}


	public Batch next() 
	{
		///join over all partitions finished
		if( eopl && eopr && bcurs == 0 && rcurs == 0 )
		{
			return null;
		}

		outbatch = new Batch(batchsize);
		
		///continue join 
		while (!outbatch.isFull()) 
		{
			///last batch to return
			if ( eopl && eopr && bcurs == 0 && rcurs == 0 ) 
			{
				if (!outbatch.isEmpty()) 
				{
					return outbatch;
				} 
				else 
				{
					return null;
				}
			}
			///join each partition
			for (int parIndex = bcurs; parIndex < numBuff - 1; parIndex ++) 
			{
				
				///eopr == true means we need to read a new left partition and build hash table for it
				if (eopr) 
				{
					try 
					{
						leftInput = new ObjectInputStream(new FileInputStream(leftPartitions[parIndex] + ".partition"));
						eopl = false;
					} 
					catch (IOException io) 
					{
						System.err.println("HashJoin: read in left partition error");
						System.exit(1);
					}

					///build hash table for left partition
					innerHashTable = new Hashtable<Integer, LinkedList<Tuple>>((numBuff - 2) * batchsize);
					while (!eopl) 
					{
						try 
						{
							leftbatch = (Batch)leftInput.readObject();  ///read a page of left partition
							for (int i = 0; i < leftbatch.size(); i++) 
							{
								Tuple rec = leftbatch.elementAt(i);
								Integer key = hashFunc(rec.dataAt(leftindex));
								LinkedList<Tuple> list = new LinkedList<Tuple>();
								list.add(rec);
								put(innerHashTable, key, list); ///assume the entire partition can be held
							}
						} 
						catch (EOFException e) 
						{
							eopl = true; ///whole left partition has been hashed
							try 
							{
								leftInput.close();
							} 
							catch (Exception ee) 
							{
								System.err.println(ee.toString());
								System.exit(1);
							}
						} 
						catch (Exception other)
						{
							System.err.println(other.toString());
							System.exit(1);
						}
					}

					///after build hash table for left partition, be ready to read right partition
					try
					{
						rightInput = new ObjectInputStream(new FileInputStream(rightPartitions[parIndex] + ".partition"));
						eopr = false;
					} 
					catch (Exception e) 
					{
						System.err.println(e.toString());
						System.exit(1);
					}
				}
				
				///begin to join right partition to hash table of left partition
				while (!eopr) 
				{
					///eobr == true means we need to read another page of right partition
					if (eobr) 
					{
						try 
						{
							rightbatch = (Batch)rightInput.readObject();
							eobr = false;
						} 
						catch (EOFException e)
						{
							eopr = true;
							try 
							{
								rightInput.close();
							}
							catch (Exception ee) 
							{
								System.err.println(ee.toString());
								System.exit(1);
							}
						} 
						catch (Exception e)
						{
							System.err.println(e.toString());
							System.exit(1);
						}
					}

					if (eopr) ///be ready to move to next partition
					{
						break;
					}

					///for each tuple in right partition, probe hash table of left partition
					for (int j = rcurs ; j <  rightbatch.size(); j++ ) 
					{
						Tuple righttuple = rightbatch.elementAt(j);
						if (linkedlistindex == 0) 
						{
							Integer key = hashFunc(righttuple.dataAt(rightindex));
							if (innerHashTable.containsKey(key)) 
							{
								linkedlist = innerHashTable.get(key);
							} 
							else /// move to next tuple of right page
							{
								continue;
							}
						}
						/// if hashtable.containsKey(key)==true, then join
						for(int m = linkedlistindex ; m < linkedlist.size(); m++) 
						{
							Tuple lefttuple = linkedlist.get(m);
							if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) 
							{
								Tuple outtuple = null;
								outtuple = lefttuple.joinWith(righttuple);
								outbatch.add(outtuple);
								/// need to check whether outbatch is full after each successful join
								if (outbatch.isFull()) 
								{
									///not finish scanning the list
									if(m != linkedlist.size() - 1 )
									{
										linkedlistindex = m + 1;
										bcurs = parIndex;
										rcurs = j;
									}
									///finish scanning the list but not reach the end of right page
									else if (j != rightbatch.size() - 1) 
									{
										linkedlistindex = 0;
										bcurs = parIndex;
										rcurs = j + 1;
									}
									///finish scanning the list and reach the end of right page, but not reach the end of right partition
									else if (!eopr) 
									{
										linkedlistindex = 0;
										bcurs = parIndex;
										rcurs = 0;
										eobr = true;
									}
									///reach the end of right partition, then be ready to read next left partition
									else if (parIndex != numBuff - 2) 
									{/// 2: one for right page, one for outbatch
										linkedlistindex = 0;
										bcurs = parIndex + 1;
										rcurs = 0;
										eobr = true;
									}
									///finish join over all partitions
									else 
									{
										linkedlistindex = 0;
										bcurs = 0;
										rcurs = 0;
										eobr = true;
									}
									return outbatch;
								}

							}
						}
						linkedlistindex = 0;
					}
					rcurs = 0;
					eobr = true;
				}
			}
			bcurs = 0;
		}
		return outbatch;
	}
	
	
	protected int hashFunc(Object o) 
	{
		int h = Integer.valueOf(String.valueOf(o));
		return h * 101 + 50051;
	}
	
	//delete temp files
	public boolean close() 
	{
		if (left.close() && right.close()) 
		{
			
			File dirr = new File("."); 
			File[] files = dirr.listFiles();
			for(int i = 0; i<files.length; i++)
			{
				if(files[i].getName().startsWith(prefix))
					files[i].delete();
			}
			return true;
		} 
		else 
			return false;
	}
}