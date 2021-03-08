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

public class Distinct extends Operator{
    Operator base;
    int batchsize;

    /**
     * The following fields are required during
     * * execution of the select operator
     **/
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer

    int numBuffer = 3; // Minimally 3 for sorting TODO: complain if insufficient buffers
    int numRuns = 0;
    int passNum = 0;
    int maxTuples;                 // Max number of tuples the main memory can store
    ArrayList<String> runNames = new ArrayList<>();
    ArrayList<ObjectInputStream> runs = new ArrayList<>();
    Batch[] buffers = new Batch[numBuffer-1];
    boolean[] endOfStream;

    int[] attrIndex;

    TupleWriter writer; // only use writer for next(); current implementation uses primitive readers

    public Distinct(Operator base, int type) {
        super(type);
        this.base = base;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public int getNumBuffer() {
        return numBuffer;
    }

    public void setNumBuffer(int numBuffer) {
        System.out.println("HERE: " + numBuffer);
        this.numBuffer = numBuffer;
    }

    @Override
    public boolean open() {
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        System.out.println("Size of tuple is: " + tuplesize);
        System.out.println("Batch size is: " + batchsize);

        if (!base.open()) return false;
        StoreIndexToDistinct();
        GenerateSortedRuns();
        MergeSortedRuns(); // merge all into 1

        System.out.println(runNames);
        Debug.PPrint(runNames.get(0));
        return true;
    }

    /**
     * Stores the indices of the attributes to order by.
     */
    private void StoreIndexToDistinct() {
        Schema baseSchema = base.getSchema();
        ArrayList<Attribute> attrList = baseSchema.getAttList();;
        attrIndex = new int[attrList.size()];
        // In distinct, all attributes are used
        for (int i = 0; i < attrList.size(); ++i) {
            attrIndex[i] = i;
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
//            Debug.PPrint(inbatch);
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
        return String.format("Distinct_Pass-%d_Run-%d", passNum, runNum);
    }

    /**
     * Merges the sorted runs until the last step.
     */
    private void MergeSortedRuns() {
        assert numBuffer > 2;
        int numBuffersForMerge = numBuffer - 1;
        assert numBuffersForMerge > 1;

        ArrayList<String> mergedRuns = new ArrayList<>();
        // Execute the number of passes to combine to a single run
        int passCounter = (int) Math.ceil(Math.log(numRuns) / Math.log(numBuffersForMerge));
        System.out.println("numRuns: " + numRuns);
        System.out.println("numBuffersForMerge: " + numBuffersForMerge);
        System.out.println("passCounter: " + passCounter);
        for (int i = 0; i < passCounter; i++) {
            numRuns = 0;    // Reset to 0 for the next pass.
            passNum++;
            System.out.println("Run names being merged are: " + runNames);

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

            System.out.println("Merged Runs are: " + mergedRuns);
            runNames.clear();
            runNames.addAll(mergedRuns);
            mergedRuns.clear();
        }
    }

    private void MergeKArrays(ArrayList<String> runsToMerge, String mergedFileName) {
        System.out.println("====== merge(): " + mergedFileName + "======");
        System.out.println("Merging runs " + runsToMerge);

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
                System.out.println("Merge completed: " + mergedFileName + "\n");
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
                } else {
                    System.out.println("Current chosen tuple is: ");
                    Debug.PPrint(chosenTuple);
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
                System.out.print("Chosen tuple is: ");
                Debug.PPrint(chosenTuple);

                outbatch.add(chosenTuple);
                System.out.println("Current outbatch is: ");
                Debug.PPrint(outbatch);

                chosenBatch.remove(0);
            }

            if (outbatch.isEmpty()) {
                System.out.println("Output buffer is empty. Don't write");
                continue;
            }

            // Write out to file
            try {
                System.out.println("Outbatch to write out is: ");
                Debug.PPrint(outbatch);
                outputStream.writeObject(outbatch.copyOf(outbatch));
                outbatch.clear();
            } catch (IOException io) {
                System.out.println("Error writing to new run");
                System.exit(1);
            }
        }

        System.out.println("Printing file name");
        Debug.PPrint(mergedFileName);

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
        System.out.println("==========Before==========");
        for (int i = 0; i < buffers.length; i++) {
            System.out.println("Buffer " + i + ":");
            Debug.PPrint(buffers[i]);
        }

        for (int i = 0; i < buffers.length; i++) {
            if (!buffers[i].isEmpty()) {
                System.out.println("Buffer " + i + " still contain tuples. Don't read in new batch yet");
                continue;
            }

            // buffers[i] is not empty
            if (i < eos.length) {
                if (!eos[i]) {
                    System.out.println("eos[i] is false");
                    System.out.println("Before reading");
                    System.out.println("Buffer " + i + ":");
                    Debug.PPrint(buffers[i]);

                    ReadRecordsIntoChosenBuffer(runs, buffers, eos, i, i);

                    System.out.println("After reading");
                    System.out.println("Buffer " + i + ":");
                    Debug.PPrint(buffers[i]);
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

        System.out.println("==========After==========");
        for (int i = 0; i < buffers.length; i++) {
            System.out.println("Buffer " + i + ":");
            Debug.PPrint(buffers[i]);
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
        boolean result;
        // result = true if chosen > current
        result = Tuple.compareTuples(chosen, current, attrToCompare) == 1;
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


    // @Override
    // public Batch next() {
    //     int i;
    //     if (eos) {
    //         close();
    //         return null;
    //     }

    //     /** An output buffer is initiated **/
    //     outbatch = new Batch(batchsize);

    //     /** keep on checking the incoming pages until
    //      ** the output buffer is full
    //      **/
    //     while (!outbatch.isFull()) {

    //     }
    //     System.out.println("Outbatch is full.");
    //     Debug.PPrint(outbatch);
    //     return outbatch;
    // }

    @Override
    public boolean close() {
        DeleteFiles(runNames);
        base.close();
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

    @Override
    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Distinct newdis = new Distinct(newbase, optype);
        newdis.setSchema((Schema) newbase.getSchema().clone());
        return newdis;
    }
}
