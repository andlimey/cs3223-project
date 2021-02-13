package qp.operators;

import qp.utils.*;

import java.io.*;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;

import static java.lang.Math.max;
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

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    public ArrayList<String> generateSortedRuns(Operator base, ArrayList<Integer> id, String basename) {
        int runnum = 0;
        Batch inbatch = base.next();
        int MAX_SIZE = numBuff*inbatch.size();
        ArrayList<Tuple> runToBeStored = new ArrayList<>(MAX_SIZE);
        ArrayList<String> runNames = new ArrayList<>();
        while (inbatch != null) {
             assert inbatch.size() <= inbatch.capacity();
             if (runToBeStored.size() < MAX_SIZE) {
                 runToBeStored.addAll(inbatch.getAllTuplesCopy());
                 // Possible for addition of
                 // MAX_SIZE is a multiple of batchsize
                 assert runToBeStored.size() <= MAX_SIZE;
             } else {
                 Collections.sort(runToBeStored, new TupleComparator(id));
                 String rfname = getRfname(basename, 0, runnum);
                 try {
                     ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                     System.out.println("Printing to " + rfname + ": ");
                     Debug.PPrint(runToBeStored);
                     out.writeObject(runToBeStored);
                     runNames.add(rfname);
                 } catch (IOException io) {
                     System.out.println("SortMergeJoin: Error writing to temporary file");
                 }

                 // Clear buffer and add current inbatch to buffer
                 runToBeStored = new ArrayList<>(MAX_SIZE);
                 runToBeStored.addAll(inbatch.getAllTuplesCopy());
                 assert runToBeStored.size() <= MAX_SIZE;
                 runnum++;
             }
             inbatch = base.next();
        }

        // Store remaining tuples
        if (runToBeStored.size() != 0) {
            Collections.sort(runToBeStored, new TupleComparator(id));
            String rfname = getRfname(basename, 0, runnum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                System.out.println("Printing to " + rfname + ": ");
                Debug.PPrint(runToBeStored);
                out.writeObject(runToBeStored);
                runNames.add(rfname);
            } catch (IOException io) {
                System.out.println("SortMergeJoin: Error writing to temporary file");
            }
        }
        return runNames;
    }

    /**
     * @param base base name used in temp file naming
     * @param passnum corresponds to the nth pass
     * @param runnum corresponds to the nth run
     * @return a unique filename that is representative of the tempfile
     */
    private String getRfname(String base, int passnum, int runnum) {
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
        ArrayList<String> leftRunNames = generateSortedRuns(left, leftindex,"temp-left");
        ArrayList<String> leftMerged = mergeSortedRuns(leftRunNames, "temp-left", 1);
//        ArrayList<String> rightRunNames = generateSortedRuns(right, rightindex,"temp-right");
//        mergeSortedRuns(rightRunNames, "temp-right", 1);
//        System.out.println("leftRunNames: " + leftRunNames);
        System.out.println("leftMerged: " + leftMerged);

//        System.out.println("rightRunNames: " + rightRunNames);
        return true;
    }

    private ArrayList<String> mergeSortedRuns(ArrayList<String> runs, String basename, int passnum) {
        if (runs.size() < numBuff-1) return runs;
        ArrayList<String> newRuns = new ArrayList<>();
        String rfname;
        int runnum = 0;
        assert numBuff > 0;
        for (int i = 0, j = min(numBuff-1, runs.size()); i < j; i = j, j = min(j+numBuff-1, runs.size())) {
            rfname = getRfname(basename, passnum, runnum);
            merge(new ArrayList<>(runs.subList(i, j)), rfname);
            for (String fname : runs.subList(i,j)) {
                File f = new File(fname);
                f.delete();
            }
            newRuns.add(rfname);
        }
        return mergeSortedRuns(newRuns, basename, passnum+1);
    }

    private void merge(ArrayList<String> runs, String newfname) {
        // TODO: if runs.size() < numBuff-1, then all numBuffs should be used
        // TODO: Use ArrayList<Tuple>s first, later change to batch-by-batch reading

        ArrayList<ArrayList<Tuple>> r = new ArrayList<>(); // represents the merged sorted run

        for (String rname : runs) {
            try {
                ObjectInputStream ois = new ObjectInputStream(new FileInputStream(rname));
                r.add((ArrayList<Tuple>) ois.readObject());
            } catch (EOFException e) {
                try {
                    in.close();
                } catch (IOException io) {
                    System.out.println("SortMergeJoin|merge: Error in reading temporary file");
                }
                eosr = true;
            } catch (ClassNotFoundException c) {
                System.out.println("SortMergeJoin|merge: Error in deserialising temporary file ");
                System.exit(1);
            } catch (IOException io) {
                System.out.println("SortMergeJoin|merge: Error reading in temporary file: " + rname);
            }
        }

        // Init index for tuple
        ArrayList<Integer> id = new ArrayList<>();
        for (int i = 0; i < r.get(0).get(0).data().size(); i++) {
            id.add(i);
        }
        ArrayList<Tuple> merged = new ArrayList<>(); // Merged run
        while (!r.isEmpty()) {
            Tuple min = r.get(0).get(0);
            int minI = 0;
            for (int i = 0; i < r.size(); i++) {
                ArrayList<Tuple> run = r.get(i);
                if (Tuple.compareTuples(min, run.get(0), id, id) == -1) {
                    min = run.get(0);
                    minI = i;
                }
            }
            // chosen refers to the run with the smallest 'head'
            ArrayList<Tuple> chosen = r.get(minI);
            chosen.remove(min);
            if (chosen.isEmpty()) r.remove(chosen); // if a run is empty, remove it from r.
            merged.add(min);
        }
        System.out.println("LeftMergedRun: ");
        Debug.PPrint(merged);
        //TODO: Should be written out on batch-by-batch basis, to simulate the single output buffer
        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(newfname));
            for (Tuple t : merged) {
                out.writeObject(t);
            }
            out.close();
        } catch (IOException io) {
            System.out.println("SortMergeJoin: Error writing to temporary file");
        }
    }

    @Override
    public boolean close() {
//        for (String fname : leftRunNames) {
//            File f = new File(fname);
//            f.delete();
//        }
//
//        for (String fname : rightRunNames) {
//            File f = new File(fname);
//            f.delete();
//        }
        return true;
    }
}
