package qp.operators;

import qp.utils.*;

import java.io.*;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Math.min;

public class Groupby  extends Operator {

    Operator base;
    ArrayList<Attribute> attrset;  // Set of attributes to group by
    int batchsize;                 // Number of tuples per outbatch
    int numBuffer = 3;             // Number of buffers used for sorting. Minimally need 3.
    int numRuns = 0;
    int passNum = 0;
    int maxTuples;                 // Max number of tuples the main memory can store
    ArrayList<String> runNames = new ArrayList<>();
    ArrayList<ObjectInputStream> runs = new ArrayList<>();
    Batch[] buffers = new Batch[numBuffer-1];
    boolean[] endOfStream;

    /**
     * The following fields are requied during execution
     * * of the Groupby Operator
     **/
    Batch inbatch;
    Batch outbatch;

    /**
     * index of the attributes in the base operator
     * * that are to be grouped by
     **/
    int[] attrIndex;

    public Groupby(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
    }

    public Operator getBase() {
        return base;
    }

    public int getNumBuffer() {
        return numBuffer;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public void setNumBuffer(int numBuffer) { this.numBuffer = numBuffer; }

    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * grouped by from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;
        StoreIndexToGroupBy();
        GenerateSortedRuns();
        MergeSortedRuns();
        PrepareLastRun();
        return true;
    }

    /**
     * Stores the indices of the attributes to group by.
     */
    private void StoreIndexToGroupBy() {
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
        numRuns = 0;
        ArrayList<Tuple> mainMemory = new ArrayList<>(maxTuples);   // Simulate main memory
        inbatch = base.next();

        while (inbatch != null) {
            if (mainMemory.size() == maxTuples) {
                SortTuplesInMemory(mainMemory);
                WriteTuplesToFile(mainMemory, passNum, numRuns);
                mainMemory.clear();
                numRuns++;
            }
            ReadTuplesIntoMemory(mainMemory);
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
        Collections.sort(mainMemory, new TupleComparator(attrIndex));
    }

    private void WriteTuplesToFile(ArrayList<Tuple> mainMemory, int passNum, int runNum) {
        String filename = GenerateFileName(passNum, runNum);

        try {
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));

            Batch batch = new Batch(batchsize);
            for (Tuple t: mainMemory) {
                batch.add(t);
            }

            out.writeObject(batch);
            runNames.add(filename);
        } catch (IOException io) {
            System.out.println("Error writing to file");
        }
    }

    private String GenerateFileName(int passNum, int runNum) {
        return String.format("Groupby_Pass-%d_Run-%d", passNum, runNum);
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

        // Constrains the number of runs until the last step.
        int runCounter = (int) Math.ceil(Math.log(numRuns) / Math.log(numBuffersForMerge)) - 1;

        for (int i = 0; i < runCounter; i++) {
            numRuns = 0;    // Reset to 0 for the next pass.
            passNum++;

            // Number of runs to merge is constrained by number of buffers available
            // j and k are used to retrieve the set of runs to merge
            for (int j = 0, k = min(numBuffersForMerge, runNames.size()); j < k; j = k, k = min(k + numBuffersForMerge, runNames.size())) {
                ArrayList<String> runsToMerge = new ArrayList<>(runNames.subList(j, k));
                String mergedFileName = GenerateFileName(passNum, numRuns);
                MergeKArrays(runsToMerge, mergedFileName);
                mergedRuns.add(mergedFileName);

                numRuns++;
            }

            // Deletes runs already merged in current pass.
            DeleteFiles(runNames);

            runNames.clear();
            runNames.addAll(mergedRuns);
            mergedRuns.clear();
        }
    }

    private void PrepareLastRun() {
        PrepareSortedRuns(runNames);
    }

    private void MergeKArrays(ArrayList<String> runsToMerge, String mergedFileName) {
        ObjectOutputStream outputStream = null;
        try {
            outputStream = new ObjectOutputStream(new FileOutputStream(mergedFileName));
        } catch (IOException io) {
            System.out.println("Error writing to temporary file");
            System.exit(1);
        }

        PrepareSortedRuns(runsToMerge);

        boolean isMergeComplete = false;

        assert runs.size() <= buffers.length;

        while (!isMergeComplete) {
            // Check if merging is completed
            isMergeComplete = CheckIfMergeComplete(endOfStream, buffers);
            if (isMergeComplete) {
                continue;
            }

            // Merge buffers into output buffer until full or until all buffers are empty
            while (!outbatch.isFull()) {
                // Read tuples into buffers
                ReadTuplesIntoBuffer(runs, buffers, endOfStream);

                // Perform K-way merge
                Tuple chosenTuple = null;
                Batch chosenBatch = null;

                for (Batch b : buffers) {
                    if (!b.isEmpty()) {
                        chosenTuple = b.get(0);
                        chosenBatch = b;
                        break;
                    }
                }

                if (chosenTuple == null) {
                    // Input buffers are all empty and output buffer is not full. Break and write out.
                    break;
                }

                // Find minimum/maximum
                for (int i = 0; i < buffers.length; i++) {
                    if (buffers[i].isEmpty()) continue;

                    Tuple current = buffers[i].get(0);
                    boolean shouldCurrentBeChosen = ShouldCurrentTupleBeChosen(chosenTuple, current);

                    if (shouldCurrentBeChosen) {
                        chosenTuple = current;
                        chosenBatch = buffers[i];
                    }
                }

                // Add chosen to output buffer
                outbatch.add(chosenTuple);
                chosenBatch.remove(0);
            }

            if (outbatch.isEmpty()) {
                continue;
            }

            // Write out to file
            try {
                outputStream.writeObject(outbatch.copyOf(outbatch));
                outbatch.clear();
            } catch (IOException io) {
                System.out.println("Error writing to new run");
                System.exit(1);
            }
        }

        try {
            outputStream.close();
        } catch (IOException e) {
            System.out.println("Error closing stream");
            System.exit(1);
        }
    }

    private void PrepareSortedRuns(ArrayList<String> runsToMerge) {
        // Init buffers for merging
        outbatch = new Batch(batchsize);  // 1 buffer reserved for output
        buffers = new Batch[numBuffer-1];
        for (int i = 0; i < numBuffer - 1; i++) {
            buffers[i] = new Batch(batchsize);
        }

        // Init streams for runs
        runs = new ArrayList<>();
        for (String rname : runsToMerge) {
            try {
                ObjectInputStream input = new ObjectInputStream(new FileInputStream(rname));
                runs.add(input);
            } catch (IOException io) {
                System.out.println("Error reading in file: " + rname);
                System.exit(1);
            }
        }
        endOfStream = new boolean[runs.size()];
    }

    private void ReadTuplesIntoBuffer(ArrayList<ObjectInputStream> runs, Batch[] buffers, boolean[] eos) {
        for (int i = 0; i < buffers.length; i++) {
            if (!buffers[i].isEmpty()) {
                System.out.println("Buffer " + i + " still contain tuples. Don't read in new batch yet");
                continue;
            }

            // buffers[i] is not empty
            if (i < eos.length) {
                if (!eos[i]) {
                    ReadRecordsIntoChosenBuffer(runs, buffers, eos, i, i);
                } else {
                    int availableRun = FindAvailableRunNum(eos);

                    // All runs are closed. Stop filling up buffers
                    if (availableRun == -1) break;

                    ReadRecordsIntoChosenBuffer(runs, buffers, eos, i, availableRun);
                }
            } else {
                int availableRun = FindAvailableRunNum(eos);
                if (availableRun == -1) break;

                ReadRecordsIntoChosenBuffer(runs, buffers, eos, i, availableRun);

            }
        }
    }

    private void ReadRecordsIntoChosenBuffer(ArrayList<ObjectInputStream> runs, Batch[] buffers, boolean[] eos, int bufferNum, int runNum) {
        assert(buffers[bufferNum].isEmpty());
        try {
            Batch data = (Batch) runs.get(runNum).readObject();
            buffers[bufferNum] = data;
        } catch (ClassNotFoundException e) {
            System.out.println("Class not found for reading batch");
            System.exit(1);
        } catch (EOFException eof) {
            System.out.println("EOF reached for this run: " + runNum);
            eos[runNum] = true;
        } catch (IOException io) {
            System.out.println("Error reading in batch.");
            System.exit(1);
        }
    }

    private int FindAvailableRunNum(boolean[] eos) {
        for (int i = 0; i < eos.length; i++) {
            if (!eos[i]) {
                return i;
            }
        }
        return -1;
    }

    private boolean ShouldCurrentTupleBeChosen(Tuple chosen, Tuple current) {
        ArrayList<Integer> attrToCompare = new ArrayList<>(Arrays.stream(attrIndex).boxed().collect(Collectors.toList()));
        // result = true if chosen > current
        boolean result = Tuple.compareTuples(chosen, current, attrToCompare) == 1;
        return result;
    }

    private boolean CheckIfMergeComplete(boolean[] eos, Batch[] buffers) {
        for (int i = 0; i < eos.length; i++) {
            if (!eos[i]) {
                return false;
            }
        }
        for (int i = 0; i < buffers.length; i++) {
            if (!buffers[i].isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private void DeleteFiles(ArrayList<String> fileNames) {
        for (String filename : fileNames) {
            File f = new File(filename);
            try {
                Files.deleteIfExists(Paths.get(f.getAbsolutePath()));
            }
            catch(NoSuchFileException e) {
                System.out.println("No such file/directory exists");
            }
            catch(DirectoryNotEmptyException e) {
                System.out.println("Directory is not empty.");
            }
            catch(IOException e) {
                System.out.println("Invalid permissions.");
            }
        }
    }

    /**
     *  Determine next output item
     *  Read new item from the correct run file
     */
    public Batch next() {
        if (CheckIfMergeComplete(endOfStream, buffers)) {
            return null;
        }
        outbatch = new Batch(batchsize);  // 1 buffer reserved for output
        while (!outbatch.isFull()) {
            // Read tuples into buffers
            ReadTuplesIntoBuffer(runs, buffers, endOfStream);

            // Perform K-way merge
            Tuple chosenTuple = null;
            Batch chosenBatch = null;

            for (Batch b : buffers) {
                if (!b.isEmpty()) {
                    chosenTuple = b.get(0);
                    chosenBatch = b;
                    break;
                }
            }

            if (chosenTuple == null) {
                // Input buffers are all empty and output buffer is not full. Break and write out.
                break;
            }

            // Find minimum/maximum
            for (int i = 0; i < buffers.length; i++) {
                if (buffers[i].isEmpty()) continue;

                Tuple current = buffers[i].get(0);
                boolean shouldCurrentBeChosen = ShouldCurrentTupleBeChosen(chosenTuple, current);

                if (shouldCurrentBeChosen) {
                    chosenTuple = current;
                    chosenBatch = buffers[i];
                }
            }

            // Add minimum to output buffer
            chosenBatch.remove(0);
            outbatch.add(chosenTuple);
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        // Destroy remaining run files
        DeleteFiles(runNames);
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Groupby newGroupby = new Groupby(newbase, newattr, optype);
        newGroupby.setSchema(newbase.getSchema());
        return newGroupby;
    }
}
