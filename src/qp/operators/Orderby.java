package qp.operators;

import qp.utils.*;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Math.min;

public class Orderby extends Operator {

    Operator base;
    ArrayList<Attribute> attrset;  // Set of attributes to order by
    int batchsize;                 // Number of tuples per outbatch
    boolean isDesc;
    int numBuffer = 3;             // Number of buffers used for sorting. Minimally need 3.
    int numRuns = 0;
    int passNum = 0;
    int maxTuples;                 // Max number of tuples the main memory can store
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
        MergeSortedRuns();
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
        ArrayList<Tuple> mainMemory = new ArrayList<>(maxTuples);   // Simulate main memory
        inbatch = base.next();

        while (inbatch != null) {
            if (mainMemory.size() != maxTuples) {
                ReadTuplesIntoMemory(mainMemory);
            }
            else {
                SortTuplesInMemory(mainMemory);
                WriteTuplesToFile(mainMemory, passNum, numRuns);
                mainMemory.clear();
                numRuns++;
                continue;
            }
            inbatch = base.next();
        }

        // Store remaining tuples (if any)
        if (!mainMemory.isEmpty()) {
            SortTuplesInMemory(mainMemory);
            WriteTuplesToFile(mainMemory, passNum, numRuns);
            mainMemory.clear();
            numRuns++;
        }
    }

    /**
     * ReadTuplesIntoMemory() reads tuples from the file into main memory.
     */
    private void ReadTuplesIntoMemory(ArrayList<Tuple> mainMemory) {
        mainMemory.addAll(inbatch.getAllTuplesCopy());
    }

    private void SortTuplesInMemory(ArrayList<Tuple> mainMemory) {
        if (this.isDesc()) {
            Collections.sort(mainMemory, new TupleComparator(attrIndex).reversed());
        } else {
            Collections.sort(mainMemory, new TupleComparator(attrIndex));
        }
    }

    private void WriteTuplesToFile(ArrayList<Tuple> mainMemory, int passNum, int runNum) {
        String filename = GenerateFileName(passNum, runNum);
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));
            out.writeObject(mainMemory);
            runNames.add(filename);
        } catch (IOException io) {
            System.out.println("Error writing to file");
        }
    }

    private String GenerateFileName(int passNum, int runNum) {
        return String.format("Orderby_Pass-%d_Run-%d", passNum, runNum);
    }

    /**
     * Merges the sorted runs until the last step.
     */
    private void MergeSortedRuns() {
        int numBuffersForMerge = numBuffer - 1;

        assert numBuffersForMerge > 1;

        // Last merge step to be done in next()
        if (numRuns <= numBuffersForMerge) {
            return;
        }

        ArrayList<String> mergedRuns = new ArrayList<>();

        // While condition ensures that last step is reserved for next()
        while (numRuns > numBuffersForMerge) {
            int counter = 0;

            // Number of runs to merge is constrained by number of buffers available
            // i and j are used to retrieve the set of runs to merge
            for (int i = 0, j = numBuffersForMerge; i < j; i = j, j = min(j + numBuffersForMerge, runNames.size())) {
                ArrayList<String> runsToMerge = new ArrayList<>(runNames.subList(i, j));
                String mergedFileName = GenerateFileName(passNum+1, counter);
                MergeKArrays(runsToMerge, mergedFileName);
                mergedRuns.add(mergedFileName);

                counter++;
            }
            passNum++;

            // Deletes runs already merged in current pass.
            for (String filename : runNames) {
                File f = new File(filename);
                f.delete();
            }
            runNames.addAll(mergedRuns);
            mergedRuns.clear();
        }
    }

    private void MergeKArrays(ArrayList<String> runNames, String mergedFileName) {
        ObjectOutputStream outputStream = null;
        try {
            outputStream = new ObjectOutputStream(new FileOutputStream(mergedFileName));
        } catch (IOException io) {
            System.out.println("Error writing to temporary file");
            System.exit(1);
        }

        // Init buffers for merging
        Batch outputBuffer = new Batch(batchsize);  // 1 buffer reserved for output
        Batch[] buffers = new Batch[numBuffer-1];
        for (int i = 0; i < numBuffer - 1; i++) {
            buffers[i] = new Batch(batchsize);
        }

        // Init streams for runs
        ArrayList<ObjectInputStream> runs = new ArrayList<>();
        for (String rname : runNames) {
            try {
                ObjectInputStream input = new ObjectInputStream(new FileInputStream(rname));
                runs.add(input);
            } catch (IOException io) {
                System.out.println("Error reading in file: " + rname);
                System.exit(1);
            }
        }

        boolean[] endOfStream = new boolean[numBuffer-1];
        boolean isMergeComplete = false;

        while (!isMergeComplete) {
            // Read tuples into buffers
            for (int i = 0; i < runs.size(); i++) {
                if (endOfStream[i]) {
                    System.out.println("Object Input Stream for run " + i + " is closed");
                    continue;
                }

                if (!buffers[i].isEmpty()) {
                    System.out.println("Buffer " + i + " still contain tuples. Don't read in new batch yet");
                    continue;
                }

                ObjectInputStream run = runs.get(i);
                System.out.println("Reading batches for run " + i);
                try {
                    Batch data = (Batch) run.readObject();
                    buffers[i] = data;
                } catch (ClassNotFoundException e) {
                    System.out.println("Class not found for reading batch");
                    System.exit(1);
                } catch (EOFException eof) {
                    System.out.println("EOF reached for this run");
                    endOfStream[i] = true;
                } catch (IOException io) {
                    System.out.println("Error reading in batch.");
                    System.exit(1);
                }
            }

            // Merge buffers into output buffer until full or until all buffers are empty
            while (!outputBuffer.isFull()) {
                Tuple minSoFar = null;
                Batch chosen = null;

                for (Batch b : buffers) {
                    if (!b.isEmpty()) {
                        minSoFar = b.get(0);
                        chosen = b;
                        break;
                    }
                }

                if (minSoFar == null) {
                    // Input buffers are all empty.
                    break;
                }

                // Find minimum
                for (int i = 0; i < buffers.length; i++) {
                    // Ignore empty buffer and empty stream.
                    if (endOfStream[i] && buffers[i].isEmpty()) {
                        continue;
                    }

                    // Read Tuples into this empty buffer.
                    if (!endOfStream[i] && buffers[i].isEmpty()) {
                        try {
                            Batch data = (Batch) runs.get(i).readObject();
                            buffers[i] = data;
                        } catch (ClassNotFoundException e) {
                            System.out.println("Class not found for reading batch");
                            System.exit(1);
                        } catch (EOFException eof) {
                            System.out.println("EOF reached for this run");
                            endOfStream[i] = true;
                        } catch (IOException io) {
                            System.out.println("Error reading in batch.");
                            System.exit(1);
                        }
                    }

                    Tuple current = buffers[i].get(0);
                    // minSoFar > current
                    if (Tuple.compareTuples(minSoFar, current, new ArrayList<>(Arrays.stream(attrIndex).boxed().collect(Collectors.toList()))) == 1) {
                        minSoFar = current;
                        chosen = buffers[i];
                    }
                }

                // Add minimum to output buffer
                chosen.remove(0);
                outputBuffer.add(minSoFar);
            }

            if (outputBuffer.isEmpty()) {
                System.out.println("Output buffer is empty. Don't write");
                continue;
            }

            // Write out to file
            try {
                outputStream.writeObject(outputBuffer);
                outputBuffer.clear();
            } catch (IOException io) {
                System.out.println("Error writing to new run");
                System.exit(1);
            }

            isMergeComplete = CheckIfMergeComplete(endOfStream, buffers);
        }

        try {
            outputStream.close();
        } catch (IOException e) {
            System.out.println("Error closing stream");
            System.exit(1);
        }
    }

    private boolean CheckIfMergeComplete(boolean[] eos, Batch[] buffers) {
        for (int i = 0; i < eos.length; i++) {
            if (!eos[i]) {
                return false;
            }
        }
        for (int i = 0; i < buffers.length; i++) {
            if (buffers[i].isEmpty()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        // Determine next output item
        // Read new item from the correct run file
        return null;
    }

    private void MergeLast() {
    }

    /**
     * Close the operator
     */
    public boolean close() {
        // Destroy remaining run files
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
