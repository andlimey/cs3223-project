package qp.operators;

import qp.utils.*;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;

import static java.lang.Math.min;

public class SortMergeJoin extends Join {
    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;                // Buffer page for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    ArrayList<Batch> allLeftBatches = new ArrayList<>();
    ArrayList<Batch> allRightBatches = new ArrayList<>();

    int leftBatchSize;
    int rightBatchSize;

    String leftMergedRunName;
    String rightMergedRunName;

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

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
     * @param id contains the indices of attributes on which the tuples will be sorted on.
     * @param basename is the leading portion of each run's filename.
     * @return the filenames of the generated sorted runs.
     */
    public ArrayList<String> generateSortedRuns(Operator base, ArrayList<Integer> id, String basename, int bsize) {
        int runnum = 0;
        Batch inbatch = base.next();
        int runCapacity = numBuff*inbatch.capacity(); // Max number of tuples in a sorted run
        ArrayList<Tuple> runToBeStored = new ArrayList<>(runCapacity);
        ArrayList<String> runNames = new ArrayList<>(); // Tracks files of all sorted runs
        while (inbatch != null) {
            // Buffers are full, sort the pages in a single run and write out
             if (runToBeStored.size() == runCapacity) {
                 Collections.sort(runToBeStored, new TupleComparator(id));
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
            Collections.sort(runToBeStored, new TupleComparator(id));
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
            System.out.println("Writing to " + rfname + ": ");

            Batch newbatch = new Batch(bsize);
            for (Tuple t : runToBeStored) {
                newbatch.add(t);
                Debug.PPrint(t);
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
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }

        lcurs = 0;
        rcurs = 0;
        eosl = false;
        eosr = false;

        if (!left.open() || !right.open()) return false;

        // External Sort
        ArrayList<String> leftSortedRunNames = generateSortedRuns(left, leftindex,"temp-left", leftBatchSize);
        System.out.println("leftSortedRunNames: " + leftSortedRunNames);
        this.leftMergedRunName = mergeSortedRuns(leftSortedRunNames, "temp-left", 1, leftBatchSize, leftindex).get(0);

        ArrayList<String> rightSortedRunNames = generateSortedRuns(right, rightindex,"temp-right", rightBatchSize);
        System.out.println("rightSortedRunNames: " + rightSortedRunNames);
        this.rightMergedRunName = mergeSortedRuns(rightSortedRunNames, "temp-right", 1, rightBatchSize, rightindex).get(0);

        System.out.println("leftMergedRunName: " + this.leftMergedRunName);
        System.out.println("rightMergedRunName: " + this.rightMergedRunName);


        try {
            leftIn = new ObjectInputStream(new FileInputStream(leftMergedRunName));
            rightIn = new ObjectInputStream(new FileInputStream(rightMergedRunName));
        } catch (IOException io) {
            System.err.println("SortMergeJoin:error in reading a mergedRun file");
            System.exit(1);
        }

        return true;
    }

    private ArrayList<String> mergeSortedRuns(ArrayList<String> runFileNames, String basename, int passnum, int bsize, ArrayList<Integer> id) {
        if (runFileNames.size() == 1) return runFileNames; // merge to a single run
        ArrayList<String> mergedRuns = new ArrayList<>();
        String rfname;
        assert numBuff > 0;

        // Merge runs in batches of numBuff-1
        for (int i = 0, j = min(numBuff-1, runFileNames.size()); i < j; i = j, j = min(j+numBuff-1, runFileNames.size())) {
            rfname = generateRunFileName(basename, passnum, mergedRuns.size());
            merge(new ArrayList<>(runFileNames.subList(i, j)), rfname, bsize, id);
            System.out.println("Writing runfile: " + rfname);
            Debug.PPrint(rfname);
            mergedRuns.add(rfname);
        }

        // Delete merged disk files
        for (String fname : runFileNames) {
            File f = new File(fname);
            f.delete();
        }
        return mergeSortedRuns(mergedRuns, basename, passnum+1, bsize, id);
    }

    private void merge(ArrayList<String> runNames, String mergedRunFileName, int bsize, ArrayList<Integer> id) {
        // TODO: if runNames.size() < numBuff-1, then all numBuffs should be used
        System.out.println("====== merge(): " + mergedRunFileName + "======");
        System.out.println("Merging runs: " + runNames);

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
                System.out.println("isOISClosed: " + isOISClosed);
                isMergeComplete &= isOISClosed;
            }

            for(int i = 0; i < buffers.length; i++) {
                System.out.println("buffers[" + i + "]: is empty");
                isMergeComplete &= buffers[i].isEmpty();
            }

            System.out.println("isMergeComplete: " + isMergeComplete);
            if (isMergeComplete) {
                for (int k = 0; k < buffers.length; k++) {
                    assert buffers[k].isEmpty();
                }
                assert out.isEmpty();
                System.out.print("Merge completed: " + mergedRunFileName + "\n\n");
                continue;
            }

            // Merge all with existing buffers to fill up output batch
            while (!out.isFull()) {
                // Fill up buffers with inputs
                for (int i = 0; i < buffers.length; i++) {
                    if (!buffers[i].isEmpty()) {
                        System.out.println("Buffer[" + i + "] is not empty yet, do not read in new batch from OIS");
                        continue; // buffer is still loaded
                    }

                    // buffers[i] is non-empty
                    if (i < eos.length) {
                        if (!eos[i]) {
                            // TODO: refactor out to reduce duplicate code
                            try {
                                Batch data = (Batch) runs.get(i).readObject();
                                buffers[i] = data;
                            } catch (ClassNotFoundException cnf) {
                                System.err.println("merge: Class not found for reading batch");
                                System.exit(1);
                            } catch (EOFException EOF) {
                                eos[i] = true; // Must remove the stream outside the loop
                                System.out.println("OIS[" + i + "] has closed");
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
                                System.out.println("OIS[" + availableRun + "] has closed");
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
                            System.out.println("OIS[" + availableRun + "] has closed");
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
                    System.out.println("\nbuffers[" + i + "]");
                    Debug.PPrint(buffers[i]);
                    System.out.println("minSoFar: ");
                    Debug.PPrint(minSoFar);
                    System.out.println("batchMin: ");
                    Debug.PPrint(batchMin);

                    if (Tuple.compareTuples(minSoFar, batchMin, id, id) == 1) { // minSoFar > batchMin
                        minSoFar = batchMin;
                        chosen = buffers[i];
                    }
                }

                // chosen refers to the batch with the smallest 'minSoFar'
                out.add(minSoFar);
                System.out.println("Selected minSoFar: ");
                Debug.PPrint(minSoFar);
                chosen.remove(0); // head's index is 0
            }

            if (out.isEmpty()) {
                System.out.println("Empty batch discovered, don't write out.");
                continue;
            }
            try {
                System.out.println("Writing out merged batch for " + mergedRunFileName);
                Debug.PPrint(out);
                oos.writeObject(out.copyOf(out)); // TODO: refactor out

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
        if (eosl || eosr) {
            return null;
        }
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull()) {
            if (eosl || eosr) return outbatch;

            // Load r or s from their batches if empty
            try {
                if (lcurs >= leftbatch.size()) {
                    leftbatch = (Batch) leftIn.readObject();
                    lcurs = 0;
                }
            } catch (EOFException e) {
                try {
                    leftIn.close();
                } catch (IOException io) {
                    System.out.println("SortMergeJoin: Error in reading left merged run file");
                }
                eosl = true;
                return outbatch;
            } catch (ClassNotFoundException c) {
                System.out.println("SortMergeJoin: Error in deserialising left merged run file ");
                System.exit(1);
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error in reading left merged run file");
                System.exit(1);
            }

            try {
                if (rcurs >= rightbatch.size()) {
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
                return outbatch;
            } catch (ClassNotFoundException c) {
                System.out.println("SortMergeJoin: Error in deserialising right merged run file ");
                System.exit(1);
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error in reading right merged run file");
                System.exit(1);
            }

            while (lcurs < leftbatch.size() && Tuple.compareTuples(leftbatch.get(lcurs), rightbatch.get(rcurs), leftindex, rightindex) == -1) {
                System.out.println("here2");
                lcurs++;
                if (lcurs >= leftbatch.size()) break;
                if (!rightPartition.isEmpty() && Tuple.compareTuples(leftbatch.get(lcurs), rightPartition.get(0), leftindex, rightindex) == 0) {
                    for(Tuple r : rightPartition) { // TODO: need to just add one at a time, or outbatch will overflow
                        assert leftbatch.get(lcurs).checkJoin(rightPartition.get(0), leftindex, rightindex);
                        System.out.println("Joining these tuples");
                        Debug.PPrint(leftbatch.get(lcurs));
                        Debug.PPrint(rightbatch.get(0));
                        outbatch.add(leftbatch.get(lcurs).joinWith(r)); 
                    }
                } else {
                    // No match on LHS, clear rightPartition
                    System.out.println("No match for LHS and rightPartition[0]");
                    Debug.PPrint(leftbatch.get(lcurs));
                    Debug.PPrint(rightbatch.get(0));
                    System.out.println("Clearing rightPartition");
                    rightPartition.clear();
                }
            }
            if (lcurs >= leftbatch.size()) continue;

            while (rcurs < rightbatch.size() && Tuple.compareTuples(rightbatch.get(rcurs), leftbatch.get(lcurs), rightindex, leftindex) == -1) {
                System.out.println("rcurs: " + rcurs);
                System.out.println("rightbatch.size(): " + rightbatch.size());
                System.out.println("here3");
                rcurs++;
            }
            if (rcurs >= rightbatch.size()) continue;

            if (Tuple.compareTuples(leftbatch.get(lcurs), rightbatch.get(rcurs), leftindex, rightindex) == 0) {
                System.out.println("Tuples match");
                Debug.PPrint(leftbatch.get(lcurs));
                Debug.PPrint(rightbatch.get(rcurs));
                if (!rightPartition.isEmpty() && Tuple.compareTuples(rightbatch.get(rcurs), rightPartition.get(0), rightindex, rightindex) == 0) {
                    System.out.println("Added right tuple to rightPartition");
                    rightPartition.add(rightbatch.get(rcurs));
                }

                assert leftbatch.get(lcurs).checkJoin(rightbatch.get(rcurs), leftindex, rightindex);
                Tuple outtuple = leftbatch.get(lcurs).joinWith(rightbatch.get(rcurs));
                outbatch.add(outtuple); 
                rcurs++;
            }
        }
        return outbatch;
    }

    @Override
    public boolean close() {
        File lf = new File(leftMergedRunName);
        File rf = new File(rightMergedRunName);

//        System.out.println("allLeftBatches");
//        for(Batch b : allLeftBatches) {
//            Debug.PPrint(b);
//        }
//
//        System.out.println("allRightBatches");
//        for(Batch b : allRightBatches) {
//            Debug.PPrint(b);
//        }


        lf.delete();
        rf.delete();
        return true;
    }
}
