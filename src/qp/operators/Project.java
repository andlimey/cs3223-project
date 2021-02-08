/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    Batch outbatch;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;

    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }


    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        attrIndex = new int[attrset.size()];
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            if (true) {
                System.err.println("attr tab name " + attr.getTabName());
                System.err.println("attr col name " + attr.getColName());
                System.err.println("attr agg type " + attr.getAggType());
                System.err.println("attr attr size " + attr.getAttrSize());
                System.err.println("attr base attr " + attr.getBaseAttribute());
                System.err.println("attr key type " + attr.getKeyType());
                System.err.println("attr projected type " + attr.getProjectedType());
                System.err.println("attr type " + attr.getType());
            }

            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggragation is not implemented.");
//                System.exit(1);
            }

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            Debug.PPrint(baseSchema);
            System.err.println("attr type " + attr.getType());
            attrIndex[i] = index;
        }
        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        /** all the tuples in the inbuffer goes to the output buffer **/
        inbatch = base.next();

        if (inbatch == null) {
            return null;
        }

        int tempMax = -1;
        Object temp = null;

        // Loop each row
        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            System.err.println("************");
            Debug.PPrint(basetuple);
            //System.out.println();
            ArrayList<Object> present = new ArrayList<>();
            //Loop each column per row

            for (int j = 0; j < attrset.size(); j++) {
                //if MAX attr

                if(attrset.get(j).getAggType() == 1) {
                    Object data = basetuple.dataAt(attrIndex[j]);
                    if( j > tempMax) {
                        tempMax = j;
                        temp = data;
                    }
                }

                if(j == attrset.size() - 1)
                    present.add(temp);
                System.err.println("=======");
                Object data = basetuple.dataAt(attrIndex[j]);
//                present.add(data);
                System.err.println("=======");
            }
            Tuple outtuple = new Tuple(present);
            System.err.println("******00*****");
            Debug.PPrint(outtuple);
            outbatch.add(outtuple);
            System.err.println("************");
        }



        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Project newproj = new Project(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

}
