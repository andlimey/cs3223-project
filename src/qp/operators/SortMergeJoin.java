package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import static java.lang.Math.min;

public class SortMergeJoin extends Join {
    int batchsize;                  // Number of tuples per out batch
    int[] leftAttrIndex;   // Indices of the join attributes in left table
    int[] rightAttrIndex;  // Indices of the join attributes in right table
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream

    static int filenum = 0;

    int leftBatchSize;
    int rightBatchSize;

    String leftMergedRunName;
    String rightMergedRunName;

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached
    boolean[] hasLeftBeenConsidered;        // Boolean array to check if tuples on the left table has been used for join.

    ArrayList<Tuple> rightPartition = new ArrayList<>();
    ObjectInputStream leftIn = null;
    ObjectInputStream rightIn = null;   

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
        leftBatchSize = Batch.getPageSize() / left.getSchema().getTupleSize();
        rightBatchSize = Batch.getPageSize() / right.getSchema().getTupleSize();
        leftbatch = new Batch(batchsize);
        rightbatch = new Batch(batchsize);
    }

    /**
     * From the input of a base operator, generate runs sorted by the input tuples' attribute set.
     * The number of tuples in each run is determined by the number of buffers. To simulate storage into the disk,
     * each file is stored as a temporary file and named according to their run and pass number.
     *
     * @param base is the operator whose input will be used in generating the runs.
     * @param attindex contains the indices of attributes on which the tuples will be sorted on.
     * @param basename is the leading portion of each run's filename.
     * @return the filenames of the generated sorted runs.
     */
    public ArrayList<String> generateSortedRuns(Operator base, int[] attindex, String basename, int bsize) {
        int runnum = 0;
        Batch inbatch = base.next();
        int runCapacity = numBuff*inbatch.capacity(); // Max number of tuples in a sorted run
        ArrayList<Tuple> runToBeStored = new ArrayList<>(runCapacity);
        ArrayList<String> runNames = new ArrayList<>(); // Tracks files of all sorted runs
        while (inbatch != null) {
            // Buffers are full, sort the pages in a single run and write out
             if (runToBeStored.size() == runCapacity) {
                 Collections.sort(runToBeStored, new TupleComparator(attindex));
                 String rfname = generateRunFileName(basename, 0, runnum);
                 WriteRunToFile(runToBeStored, runNames, rfname, inbatch.capacity());

                 runToBeStored.clear();
                 runnum++;
             }

             runToBeStored.addAll(inbatch.getAllTuplesCopy());
             inbatch = base.next();
        }

        // Store leftover tuples from inbatch
        if (runToBeStored.size() != 0) {
            Collections.sort(runToBeStored, new TupleComparator(attindex));
            String rfname = generateRunFileName(basename, 0, runnum);
            WriteRunToFile(runToBeStored, runNames, rfname, bsize);
        }
        return runNames;
    }

    /**
     * Write the tuples of the run into a temporary file. The run tuples are serialised as a Batch object.
     */
    private void WriteRunToFile(ArrayList<Tuple> runToBeStored, ArrayList<String> runNames, String rfname, int bsize) {
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));

            Batch newbatch = new Batch(bsize);
            for (Tuple t : runToBeStored) {
                newbatch.add(t);
            }
            out.writeObject(newbatch);
            runNames.add(rfname);
        } catch (IOException io) {
            System.out.println("SortMergeJoin: Error writing to temporary file");
            System.exit(1);
        }
    }

    /**
     * @param base base name used in temp file naming
     * @param passnum corresponds to the nth pass
     * @param runnum corresponds to the nth run
     * @return a unique filename that is representative of the tempfile
     */
    private String generateRunFileName(String base, int passnum, int runnum) {
        return String.format("%s-pass%d-run%d", base, passnum, runnum);
    }

    @Override
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        assert Batch.getPageSize() > tuplesize : "Page size must be larger than tuple size";
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        leftAttrIndex = new int[conditionList.size()];
        rightAttrIndex = new int[conditionList.size()];
        for (int i = 0; i < conditionList.size(); i++) {
            Attribute leftattr = conditionList.get(i).getLhs();
            Attribute rightattr = (Attribute) conditionList.get(i).getRhs();
            leftAttrIndex[i] = left.getSchema().indexOf(leftattr);
            rightAttrIndex[i] = right.getSchema().indexOf(rightattr);
        }

        lcurs = 0;
        rcurs = 0;
        eosl = false;
        eosr = false;

        if (!left.open() || !right.open()) return false;

        // External Sort
        ArrayList<String> leftSortedRunNames = generateSortedRuns(left, leftAttrIndex,"SMJtemp-left-" + filenum, leftBatchSize);
        if (leftSortedRunNames.isEmpty()) return false;
        this.leftMergedRunName = mergeSortedRuns(leftSortedRunNames, "SMJtemp-left-" + filenum, 1, leftBatchSize, leftAttrIndex).get(0);

        ArrayList<String> rightSortedRunNames = generateSortedRuns(right, rightAttrIndex,"SMJtemp-right-" + filenum, rightBatchSize);
        if (rightSortedRunNames.isEmpty()) return false;
        this.rightMergedRunName = mergeSortedRuns(rightSortedRunNames, "SMJtemp-right-" + filenum, 1, rightBatchSize, rightAttrIndex).get(0);

        filenum++;

        try {
            leftIn = new ObjectInputStream(new FileInputStream(leftMergedRunName));
            rightIn = new ObjectInputStream(new FileInputStream(rightMergedRunName));
        } catch (IOException io) {
            System.err.println("SortMergeJoin:error in reading a mergedRun file");
            System.exit(1);
        }

        return true;
    }

    private ArrayList<String> mergeSortedRuns(ArrayList<String> runFileNames, String basename, int passnum, int bsize, int[] attrIndex) {
        if (runFileNames.size() == 1) return runFileNames; // merge to a single run
        ArrayList<String> mergedRuns = new ArrayList<>();
        String rfname;
        assert numBuff > 0;

        // Merge runs in batches of numBuff-1
        for (int i = 0, j = min(numBuff-1, runFileNames.size()); i < j; i = j, j = min(j+numBuff-1, runFileNames.size())) {
            rfname = generateRunFileName(basename, passnum, mergedRuns.size());
            merge(new ArrayList<>(runFileNames.subList(i, j)), rfname, bsize, attrIndex);
            mergedRuns.add(rfname);
        }

        // Delete merged disk files
        for (String fname : runFileNames) {
            File f = new File(fname);
            f.delete();
        }
        return mergeSortedRuns(mergedRuns, basename, passnum+1, bsize, attrIndex);
    }

    private void merge(ArrayList<String> runNames, String mergedRunFileName, int bsize, int[] attrIndex) {
        // Init OOS for mergedRunFileName
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(new FileOutputStream(mergedRunFileName));
        } catch (IOException io) {
            System.out.println("SortMergeJoin: Error writing to temporary file");
            System.exit(1);
        }

        // Init buffers
        Batch out = new Batch(bsize);
        Batch[] buffers = new Batch[numBuff-1];
        for (int i = 0; i < numBuff-1; i++) {
            buffers[i] = new Batch(bsize);
        }

        // Init streams for runs
        ArrayList<ObjectInputStream> runs = new ArrayList<>(); // represents the merged sorted run
        for (String rname : runNames) {
            try {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(rname));
                runs.add(ois);
            } catch (IOException io) {
                System.out.println("SortMergeJoin|merge: Error reading in temporary file: " + rname);
                System.exit(1);
            }
        }

        // Track closed input streams
        boolean[] eos = new boolean[runs.size()];
        boolean isMergeComplete = false;

        assert runs.size() <= buffers.length;

        while(!isMergeComplete) { // exists an unfinished input stream
            isMergeComplete = true;
            for(int i = 0; i < runs.size(); i++) { // only check eos for all runs
                boolean isOISClosed = eos[i];
                isMergeComplete &= isOISClosed;
            }

            for(int i = 0; i < buffers.length; i++) {
                isMergeComplete &= buffers[i].isEmpty();
            }

            if (isMergeComplete) {
                for (int k = 0; k < buffers.length; k++) {
                    assert buffers[k].isEmpty();
                }
                assert out.isEmpty();
                continue;
            }

            // Merge all with existing buffers to fill up output batch
            while (!out.isFull()) {
                // Fill up buffers with inputs
                for (int i = 0; i < buffers.length; i++) {
                    if (!buffers[i].isEmpty()) {
                        continue; // buffer is still loaded
                    }

                    // buffers[i] is non-empty
                    if (i < eos.length) {
                        if (!eos[i]) {
                            try {
                                Batch data = (Batch) runs.get(i).readObject();
                                buffers[i] = data;
                            } catch (ClassNotFoundException cnf) {
                                System.err.println("merge: Class not found for reading batch");
                                System.exit(1);
                            } catch (EOFException EOF) {
                                eos[i] = true; // Must remove the stream outside the loop
                            } catch (IOException io) {
                                System.err.println("merge: Error reading in batch");
                                System.exit(1);
                            }
                            continue;
                        } else if (eos[i]) {
                            // find a batch from an open stream
                            int availableRun = -1;
                            for (int j = 0; j < eos.length; j++) {
                                if (!eos[j]) {
                                    availableRun = j;
                                    break;
                                }
                            }

                            // Does not exists an open ObjectInputStream for a run, stopping attempting to fill up buffers
                            if (availableRun == -1) break;

                            try {
                                Batch data = (Batch) runs.get(availableRun).readObject();
                                buffers[i] = data;
                            } catch (ClassNotFoundException cnf) {
                                System.err.println("merge: Class not found for reading batch");
                                System.exit(1);
                            } catch (EOFException EOF) {
                                eos[availableRun] = true; // Must remove the stream outside the loop
                            } catch (IOException io) {
                                System.err.println("merge: Error reading in batch");
                                System.exit(1);
                            }
                        }
                    } else {
                        int availableRun = -1;
                        for (int j = 0; j < eos.length; j++) {
                            if (!eos[j]) {
                                availableRun = j;
                                break;
                            }
                        }

                        // Does not exists an open ObjectInputStream for a run, stopping attempting to fill up buffers
                        if (availableRun == -1) break;

                        try {
                            Batch data = (Batch) runs.get(availableRun).readObject();
                            buffers[i] = data;
                        } catch (ClassNotFoundException cnf) {
                            System.err.println("merge: Class not found for reading batch");
                            System.exit(1);
                        } catch (EOFException EOF) {
                            eos[availableRun] = true; // Must remove the stream outside the loop
                        } catch (IOException io) {
                            System.err.println("merge: Error reading in batch");
                            System.exit(1);
                        }
                    }

                }

                // Initialise minSoFar
                Tuple minSoFar = null;
                Batch chosen = null; // choose arbitrary batch

                for (Batch b : buffers) {
                    if (!b.isEmpty()) {
                        minSoFar = b.get(0);
                        chosen = b;
                        break;
                    }
                }
                if (minSoFar == null) {
                    // All buffers are empty and output buffer is not full. Write out remaining tuples in output
                    break;
                }

                // Iterate through batches and find min tuple of all
                for (int i = 0; i < buffers.length; i++) {
                    if (buffers[i].isEmpty()) continue; // skip empty buffers
                    Tuple batchMin = buffers[i].get(0);
                    if (Tuple.compareTuples(minSoFar, batchMin, attrIndex, attrIndex) == 1) { // minSoFar > batchMin
                        minSoFar = batchMin;
                        chosen = buffers[i];
                    }
                }

                // chosen refers to the batch with the smallest 'minSoFar'
                out.add(minSoFar);
                chosen.remove(0); // head's index is 0
            }

            if (out.isEmpty()) {
                continue;
            }
            try {
                oos.writeObject(out.copyOf(out));
                out.clear(); // empty output buffer
            } catch (IOException io) {
                System.err.println("Trouble writing out object " + io);
                System.exit(1);
            }
        }
        try {
            oos.close();
        } catch (IOException io) {
            System.err.println("Trouble closing output stream");
        }
    }


    @Override
    public Batch next() {
        if (eosl) {
            return null;
        }
        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            if (eosl) return outbatch;

            // Load r or s from their batches if empty
            try {
                if (lcurs >= leftbatch.size()) {
                    leftbatch = (Batch) leftIn.readObject();
                    hasLeftBeenConsidered = new boolean[leftbatch.size()];
                    lcurs = 0;
                }
            } catch (EOFException e) {
                try {
                    leftIn.close();
                } catch (IOException io) {
                    System.out.println("SortMergeJoin: Error in reading left merged run file");
                }
                eosl = true;
                // once leftIn is empty no other join tuples expected
                return outbatch;
            } catch (ClassNotFoundException c) {
                System.out.println("SortMergeJoin: Error in deserialising left merged run file ");
                System.exit(1);
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error in reading left merged run file");
                System.exit(1);
            }

            try {
                if (!eosr && rcurs >= rightbatch.size()) {
                    rightbatch = (Batch) rightIn.readObject();
                    rcurs = 0;
                }
            } catch (EOFException e) {
                try {
                    rightIn.close();
                } catch (IOException io) {
                    System.out.println("SortMergeJoin: Error in reading right merged run file");
                }
                eosr = true;
            } catch (ClassNotFoundException c) {
                System.out.println("SortMergeJoin: Error in deserialising right merged run file ");
                System.exit(1);
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error in reading right merged run file");
                System.exit(1);
            }

            while (lcurs < leftbatch.size() &&
                    ((eosr) || // for the case where left batch could still have tuples that can join with rightPartition
                    (Tuple.compareTuples(leftbatch.get(lcurs), rightbatch.get(rcurs), leftAttrIndex, rightAttrIndex) == -1))
            ) {
                if (hasLeftBeenConsidered[lcurs]) lcurs++;
                if (lcurs >= leftbatch.size()) break;
                if (!rightPartition.isEmpty() && Tuple.compareTuples(leftbatch.get(lcurs), rightPartition.get(0), leftAttrIndex, rightAttrIndex) == 0) {
                    for (Tuple r : rightPartition) {
                        assert leftbatch.get(lcurs).checkJoin(rightPartition.get(0), leftAttrIndex, rightAttrIndex);
                        outbatch.add(leftbatch.get(lcurs).joinWith(r)); // Incorrect implementation
                    }
                } else {
                    rightPartition.clear();
                }
                hasLeftBeenConsidered[lcurs] = true;
            }
            if (lcurs >= leftbatch.size()) continue;

            while (rcurs < rightbatch.size() && Tuple.compareTuples(rightbatch.get(rcurs), leftbatch.get(lcurs), rightAttrIndex, leftAttrIndex) == -1) {
                rcurs++;
                rightPartition.clear();
            }
            if (rcurs >= rightbatch.size()) continue;

            if (Tuple.compareTuples(leftbatch.get(lcurs), rightbatch.get(rcurs), leftAttrIndex, rightAttrIndex) == 0) {
                if ( rightPartition.isEmpty() || (!rightPartition.isEmpty() && Tuple.compareTuples(rightbatch.get(rcurs), rightPartition.get(0), rightAttrIndex, rightAttrIndex) == 0)) {
                    rightPartition.add(rightbatch.get(rcurs));
                } else {
                    rightPartition.clear();
                    rightPartition.add(rightbatch.get(rcurs));
                }

                assert leftbatch.get(lcurs).checkJoin(rightbatch.get(rcurs), leftAttrIndex, rightAttrIndex);
                Tuple outtuple = leftbatch.get(lcurs).joinWith(rightbatch.get(rcurs));
                outbatch.add(outtuple);
                hasLeftBeenConsidered[lcurs] = true;
                rcurs++;
            }
        }
        return outbatch;
    }

    @Override
    public boolean close() {
        File lf = new File(leftMergedRunName);
        File rf = new File(rightMergedRunName);
        lf.delete();
        rf.delete();
        left.close();
        right.close();
        return true;
    }
}
