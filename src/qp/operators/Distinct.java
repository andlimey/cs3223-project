package qp.operators;

import qp.utils.*;

import java.util.HashSet;
import java.util.Set;

public class Distinct extends Operator{
    Operator base;
    int batchsize;

    /**
     * The following fields are required during
     * * execution of the select operator
     **/
    boolean eos;     // Indicate whether end of stream is reached or not
    Batch inbatch;   // This is the current input buffer
    Batch outbatch;  // This is the current output buffer
    int start;       // Cursor position in the input buffer

    private static HashSet<Tuple> set;

    static {
        set = new HashSet<>();
    }

    public Distinct(Operator base, int type) {
        super(type);
        this.base = base;
    }

    @Override
    public boolean open() {
        eos = false;
        start = 0;

        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (base.open())
            return true;
        else
            return false;
    }

    @Override
    public Batch next() {
        int i;
        if (eos) {
            close();
            return null;
        }

        /** An output buffer is initiated **/
        outbatch = new Batch(batchsize);

        /** keep on checking the incoming pages until
         ** the output buffer is full
         **/
        while (!outbatch.isFull()) {
            if (start == 0) {
                inbatch = base.next();
                /** There is no more incoming pages from base operator **/
                if (inbatch == null) {
//                    System.out.println("No more input to Distinct, return outbatch.");
                    eos = true;
                    return outbatch;
                }
            }
//            Debug.PPrint(this);
//            Debug.PPrint(inbatch);
            /** Continue this for loop until this page is fully observed
             ** or the output buffer is full
             **/
            for (i = start; i < inbatch.size() && (!outbatch.isFull()); ++i) {
                Tuple present = inbatch.get(i);
                /** If the tuple is distinct by the overridden Tuple.hashCode(),
                 ** this tuple is added to the output batch
                 **/
//                System.out.print("Checking tuple: ");
                Debug.PPrint(present);
                boolean containsTuple = set.contains(present);
//                System.out.println("Does set contain tuple: " + containsTuple);
                if (!containsTuple) {
                    outbatch.add(present);
                    set.add(present);
                }
//                System.out.println("Set: " + set.toString());
                System.out.println("\n");

            }

            /** Modify the cursor to the position required
             ** when the base operator is called next time;
             **/
            if (i == inbatch.size())
                start = 0;
            else
                start = i;
        }
//        System.out.println("Outbatch is full.");
//        Debug.PPrint(outbatch);
        return outbatch;
    }

    @Override
    public boolean close() {
        base.close();
        return true;
    }

    @Override
    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Distinct newdis = new Distinct(newbase, optype);
        newdis.setSchema((Schema) newbase.getSchema().clone());
        return newdis;
    }
}
