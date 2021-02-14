package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;
import qp.utils.TupleComparator;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;

public class Orderby extends Operator {

    Operator base;
    ArrayList<Attribute> attrset;  // Set of attributes to order by
    int batchsize;                 // Number of tuples per outbatch
    boolean isDesc;
    int numBuffer;                 // Number of buffers used for sorting
    int numRuns = 0;
    int passNum = 0;
    int maxTuples;                 // Max number of tuples the main memory can store
    ArrayList<Tuple> mainMemory;   // Simulate memory
    ArrayList<String> runNames = new ArrayList<>();

    /**
     * The following fields are requied during execution
     * * of the Orderby Operator
     **/
    Batch inbatch;
    Batch outbatch;

    /**
     * index of the attributes in the base operator
     * * that are to be ordered by
     **/
    int[] attrIndex;

    public Orderby(Operator base, ArrayList<Attribute> as, int type, boolean isDesc) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.isDesc = isDesc;
    }

    public Operator getBase() {
        return base;
    }

    public boolean isDesc() { return isDesc; }

    public void setBase(Operator base) {
        this.base = base;
    }

    public void setNumBuffer(int numBuffer) { this.numBuffer = numBuffer; }

    public ArrayList<Attribute> getOrderbyAttr() {
        return attrset;
    }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * ordered by from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;
        StoreIndexToOrderBy();
        GenerateSortedRuns();
        return true;
    }

    /**
     * Stores the indices of the attributes to order by.
     */
    private void StoreIndexToOrderBy() {
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);
            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;
        }
    }

    /**
     * Generates sorted runs
     **/
    private void GenerateSortedRuns() {
        maxTuples = numBuffer * batchsize;
        mainMemory = new ArrayList<>(maxTuples);
        inbatch = base.next();

        while (inbatch != null) {
            if (mainMemory.size() != maxTuples) {
                ReadTuplesIntoMemory();
            }
            else {
                SortTuplesInMemory();
                WriteTuplesToFile(passNum, numRuns);
                mainMemory.clear();
                numRuns++;
            }
            inbatch = base.next();
        }

        // Store remaining tuples (if any)
        if (!mainMemory.isEmpty()) {
            SortTuplesInMemory();
            WriteTuplesToFile(passNum, numRuns);
            mainMemory.clear();
            numRuns++;
        }
    }

    /**
     * ReadTuplesIntoMemory() reads tuples from the file into main memory.
     */
    private void ReadTuplesIntoMemory() {
        mainMemory.addAll(inbatch.getAllTuplesCopy());
    }

    private void SortTuplesInMemory() {
        if (this.isDesc()) {
            Collections.sort(mainMemory, new TupleComparator(attrIndex).reversed());
        } else {
            Collections.sort(mainMemory, new TupleComparator(attrIndex));
        }
    }

    private void WriteTuplesToFile(int passNum, int runNum) {
        String filename = String.format("Orderby_Pass-%d_Run-%d", passNum, runNum);
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));
            out.writeObject(mainMemory);
            runNames.add(filename);
        } catch (IOException io) {
            System.out.println("Error writing to file");
        }
    }

    /**
     * Merges the sorted runs
     */
    private void MergeSortedRuns() {
        /**
         *  While numRuns > numBuffers:
         *      While there are still runs to merge:
         *          For each run in current set of runs:
         *              Open file and prepare to read
         *
         *          While all open runs are still not empty:
         *              Read into (numBuffers-1) array of size batchsize
         *              MergeKArrays
         */
    }

    private void MergeKArrays(ArrayList<ArrayList<Tuple>> arr, int numArrays) {
        // https://www.geeksforgeeks.org/merge-k-sorted-arrays/
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Orderby newOrderby = new Orderby(newbase, newattr, optype, this.isDesc);
        newOrderby.setSchema(newbase.getSchema());
        return newOrderby;
    }
}
