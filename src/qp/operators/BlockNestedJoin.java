/**
 * Block Nested Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    int outerBlockSize;
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ArrayList<Batch> outerBlock;    // Outer block for tuples
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    int bcurs;                      // Cursor for block
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    public BlockNestedJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        outerBlockSize = numBuff - 2;

        outerBlock = new ArrayList<>(outerBlockSize);

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

        /** Right hand side table is to be materialized
         ** for the Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
            try {

                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        System.out.println("bump next 1");

        int i, j, k;
        if (eosl) {
            return null;
        }
        outbatch = new Batch(batchsize);   
        while (!outbatch.isFull()) {
            System.out.println("bump next 1.2");

            if (lcurs == 0 && eosr == true && bcurs == 0) {
                /** new left block is to be fetched**/
                outerBlock = new ArrayList<>(outerBlockSize);
                while(outerBlock.size() < outerBlockSize) {
                    System.out.println("bump next 2");
                    Batch currBatch = (Batch) left.next();
                    if (currBatch == null) {
                        System.out.println("bump next 2.1");
                        eosl = true;
                        System.out.println("bump next 2.4");
                        break;
                    }
                    System.out.println("bump next 2.5");
                    outerBlock.add(currBatch);
                }

                /** Whenever a new left block come, we have to start the
                 ** scanning of right table
                 **/
                

                try {
                    System.out.println("bump next 2.2");
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                    System.out.println(eosr);
                } catch (IOException io) {
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }

            }
            while (eosr == false) {
                System.out.println("bump next 1.3");

                try {
                    if (rcurs == 0 && lcurs == 0 && bcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }

                    for(k = bcurs; k < outerBlockSize; ++k ) {
                        Batch leftbatch = outerBlock.get(k);
                        for (i = lcurs; i < leftbatch.size(); ++i) {
                            for (j = rcurs; j < rightbatch.size(); ++j) {
                                Tuple lefttuple = leftbatch.get(i);
                                Tuple righttuple = rightbatch.get(j);
                                // Debug.PPrint(lefttuple);
                                // Debug.PPrint(righttuple);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);
                                    Debug.PPrint(outtuple);
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        if (i == leftbatch.size() - 1 && 
                                            j == rightbatch.size() - 1 && 
                                                k == outerBlock.size() - 1) {  //case 1 when all completed
                                            lcurs = 0;
                                            rcurs = 0;
                                            bcurs = 0;
                                        } else if (i == leftbatch.size() - 1 && 
                                        j == rightbatch.size() - 1 && 
                                            k != outerBlock.size() - 1) {  //case 2 when left and right completed
                                            lcurs = 0;
                                            rcurs = 0;
                                            bcurs = k + 1;
                                        } else if (i != leftbatch.size() - 1 && 
                                            j == rightbatch.size() - 1) {  //case 3 when right side completed 
                                            lcurs = i + 1;
                                            rcurs = 0;
                                        } else if (i == leftbatch.size() - 1 && j != rightbatch.size() - 1) {  //case 4 when right side not complete
                                            lcurs = i;
                                            rcurs = j + 1;
                                        } else {
                                            lcurs = i;
                                            rcurs = j + 1;
                                        }
                                        return outbatch;
                                    }
                                }
                            }
                            rcurs = 0;
                        }
                        lcurs = 0;
                    }
                    bcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        System.out.println("bump close");

        File f = new File(rfname);
        f.delete();
        return true;
    }

}
