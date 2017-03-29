/** page nested join algorithm **/

package qp.operators;

import qp.sorter.*;
import qp.sorter.std.*;
import qp.sorter.util.*;
import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class SortMergeJoin extends Join{

	Sorter<Tuple> sorter;
	int batchsize;  //Number of tuples per out batch
	ArrayList<Tuple> leftBuffer;
	ArrayList<Tuple> rightBuffer;

	/** The following fields are useful during execution of
	 ** the SortMergeJoin operation
	 **/
	int leftindex;     // Index of the join attribute in left table
	int rightindex;    // Index of the join attribute in right table

	String leftTablePath;
	String rightTablePath;    // The file name where the right table is materialize

	String outFileName;

	Batch outbatch;   // Output buffer
	Batch leftbatch;  // Buffer for left input stream
	Batch rightbatch;  // Buffer for right input stream
	ObjectInputStream lin; // File pointer to the left hand materialized file
	ObjectInputStream rin; // File pointer to the right hand materialized file
	ObjectOutputStream lout;
	ObjectOutputStream rout;

	static int leftfilenum=0;
	static int rightfilenum=0;

	int lcurs;    // Cursor for left side buffer
	int rcurs;    // Cursor for right side buffer
	boolean eosl;  // Whether end of stream (left table) is reached
	boolean eosr;  // End of stream (right table)

	public SortMergeJoin(Join jn){
		super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
		schema = jn.getSchema();
		jointype = jn.getJoinType();
		numBuff = jn.getNumBuff();
		leftBuffer = new ArrayList<Tuple> ();
		rightBuffer = new ArrayList<Tuple> ();
		sorter = new Sorter<Tuple>();
	}


	/** During open finds the index of the join attributes
	 **  Materializes the right hand side into a file
	 **  Opens the connections
	 **/



	public boolean open(){

		/** select number of tuples per batch **/
		int tuplesize=schema.getTupleSize();
		batchsize=Batch.getPageSize()/tuplesize;

		Attribute leftattr = con.getLhs();
		Attribute rightattr =(Attribute) con.getRhs();
		leftindex = left.getSchema().indexOf(leftattr);
		rightindex = right.getSchema().indexOf(rightattr);
		Batch leftpage;
		Batch rightpage;

		/** initialize the cursors of input buffers **/
		lcurs = 0; rcurs =0;
		eosl=false;
		eosr=false;
		if(!left.open() || !right.open()) {
			return false;
		} else {
			try {
				leftfilenum++;
				leftTablePath = "lefttemp-" + String.valueOf(leftfilenum);
				File output = new File(leftTablePath);
				lout = new ObjectOutputStream(new FileOutputStream(output));
				while( (leftpage = left.next()) != null){
					lout.writeObject(leftpage);
				}
				lout.close();
				lin = new ObjectInputStream(new FileInputStream(output));
				leftTablePath = "sorted " + leftTablePath;
				File sortedOutput = new File(leftTablePath);
				lout = new ObjectOutputStream(new FileOutputStream(sortedOutput));
				sorter = new Sorter<Tuple>(new SortConfig(numBuff - 1).withMaxMemoryUsage((long)numBuff * Batch.getPageSize()));
				sorter.setComparator(new TupleComparator());
				sorter.sort(lin, lout);
				lin.close();
				lout.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				rightfilenum++;
				rightTablePath = "righttemp-" + String.valueOf(leftfilenum);
				File output = new File(rightTablePath);
				rout = new ObjectOutputStream(new FileOutputStream(output));
				while( (rightpage = right.next()) != null){
					rout.writeObject(rightpage);
				}
				rout.close();
				rin = new ObjectInputStream(new FileInputStream(output));
				rightTablePath = "sorted " + rightTablePath;
				File sortedOutput = new File(rightTablePath);
				rout = new ObjectOutputStream(new FileOutputStream(sortedOutput));
				sorter = new Sorter<Tuple>(new SortConfig(numBuff - 1).withMaxMemoryUsage((long)numBuff * Batch.getPageSize()));
				sorter.setComparator(new TupleComparator());
				sorter.sort(rin, rout);
				rin.close();
				rout.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}


			return true;
		}



	}



	/** from input buffers selects the tuples satisfying join condition
	 ** And returns a page of output tuples
	 **/


	public Batch next(){
		//System.out.print("NestedJoin:--------------------------in next----------------");
		//Debug.PPrint(con);
		//System.out.println();
		int i,j;
		if(eosl || eosr){
			close();
			return null;
		}
		outbatch = new Batch(batchsize);


		while(!outbatch.isFull()){

			if(lcurs==0){
				/** new left page is to be fetched**/
				leftbatch =(Batch) left.next();
				if(leftbatch==null){
					eosl=true;
					return outbatch;
				}
			}

			if(rcurs==0) {
				rightbatch = (Batch) right.next();
				if(rightbatch==null) {
					eosr=true;
					return outbatch;
				}
			}

			try {
				lin = new ObjectInputStream(new FileInputStream(leftTablePath));
				rin = new ObjectInputStream(new FileInputStream(rightTablePath));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			while(eosl==false && eosr==false){

				try{
					if(rcurs==0 && lcurs==0){
						leftbatch = (Batch) lin.readObject();
						rightbatch = (Batch) rin.readObject();
					}
					i=lcurs;
					j=rcurs;
					while(i<leftbatch.size()){
						while(j<rightbatch.size()){
							while(Tuple.compareTuples(leftbatch.elementAt(i), 
									rightbatch.elementAt(j), leftindex, rightindex) != 0) {
								if(Tuple.compareTuples(leftbatch.elementAt(i), 
										rightbatch.elementAt(j), leftindex, rightindex) > 0) {
									j++;
								} else {
									i++;
								}
							}
							int Istart = i;
							int Jstart = j;
							while(i+1<leftbatch.size() && 
									Tuple.compareTuples(leftbatch.elementAt(i), leftbatch.elementAt(i+1), leftindex) == 0){
								i++;
							}
							while(j+1<rightbatch.size() && 
									Tuple.compareTuples(rightbatch.elementAt(i), rightbatch.elementAt(i+1), rightindex) == 0){
								j++;
							}
							int Iend = i;
							int Jend = j;
							for (int a = Istart; a<= Iend; a++) {
								for (int b = Jstart; b<=Jend; b++) {
									Tuple outtuple = leftbatch.elementAt(a).joinWith(rightbatch.elementAt(b));
									outbatch.add(outtuple);
									if(outbatch.isFull()){
										if(i==leftbatch.size()-1 && j==rightbatch.size()-1){//case 1
											lcurs=0;
											rcurs=0;
										}else if(i!=leftbatch.size()-1 && j==rightbatch.size()-1){//case 2
											lcurs = i+1;
											rcurs = 0;
										}else if(i==leftbatch.size()-1 && j!=rightbatch.size()-1){//case 3
											lcurs = 0;
											rcurs = j+1;
										}else{
											lcurs = i+1;
											rcurs =j+1;
										}
										return outbatch;
									}
								}
							}
						}
						rcurs =0;
					}
					lcurs=0;
				}catch(EOFException e){
					try{
						rin.close();
					}catch (IOException io){
						System.out.println("NestedJoin:Error in temporary file reading");
					}
					eosr=true;
				}catch(ClassNotFoundException c){
					System.out.println("NestedJoin:Some error in deserialization ");
					System.exit(1);
				}catch(IOException io){
					System.out.println("NestedJoin:temporary file reading error");
					System.exit(1);
				}
			}
		}




		return outbatch;
	}



	/** Close the operator */
	public boolean close(){

		File left = new File(leftTablePath);
		File right = new File(rightTablePath);
		left.delete();
		right.delete();
		return true;

	}


}












































