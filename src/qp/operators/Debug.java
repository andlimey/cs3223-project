/**
 * This class to print various information
 **/

package qp.operators;

import qp.utils.*;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;

public class Debug {
    /**
     * print the attribute
     **/
    public static void PPrint(Attribute attr) {
        String[] aggregates = new String[]{"", "MAX", "MIN", "SUM", "COUNT", "AVG"};

        int aggregate = attr.getAggType();
        String tabname = attr.getTabName();
        String colname = attr.getColName();
        if (aggregate == 0) {
            System.out.print(tabname + "." + colname);
        } else {
            System.out.print(aggregates[aggregate] + "(" + tabname + "." + colname + ")  ");
        }
    }

    /**
     * print the condition
     **/
    public static void PPrint(Condition con) {
        Attribute lhs = con.getLhs();
        Object rhs = con.getRhs();
        int exprtype = con.getExprType();
        PPrint(lhs);
        switch (exprtype) {
            case Condition.LESSTHAN:
                System.out.print("<");
                break;
            case Condition.GREATERTHAN:
                System.out.print(">");
                break;
            case Condition.LTOE:
                System.out.print("<=");
                break;
            case Condition.GTOE:
                System.out.print(">=");
                break;
            case Condition.EQUAL:
                System.out.print("==");
                break;
            case Condition.NOTEQUAL:
                System.out.print("!=");
                break;
        }

        if (con.getOpType() == Condition.JOIN) {
            PPrint((Attribute) rhs);
        } else if (con.getOpType() == Condition.SELECT) {
            System.out.print((String) rhs);
        }
    }


    /**
     * print schema
     **/
    public static void PPrint(Schema schema) {
        System.out.println();
        for (int i = 0; i < schema.getNumCols(); i++) {
            Attribute attr = schema.getAttribute(i);
            PPrint(attr);
        }
        System.out.println();
    }


    /**
     * print a node in plan tree
     **/
    public static void PPrint(Operator node) {
        int optype = node.getOpType();

        if (optype == OpType.JOIN) {
            int exprtype = ((Join) node).getJoinType();
            switch (exprtype) {
                case JoinType.NESTEDJOIN:
                    System.out.print("NestedJoin(");
                    break;
                case JoinType.BLOCKNESTED:
                    System.out.print("BlockNested(");
                    break;
                case JoinType.SORTMERGE:
                    System.out.print("SortMerge(");
                    break;
                case JoinType.HASHJOIN:
                    System.out.print("HashJoin(");
                    break;
            }
            PPrint(((Join) node).getLeft());
            System.out.print("  [");
            PPrint(((Join) node).getCondition());
            System.out.print("]  ");
            PPrint(((Join) node).getRight());
            System.out.print(")");

        } else if (optype == OpType.SELECT) {
            System.out.print("Select(");
            PPrint(((Select) node).getBase());
            System.out.print("  '");
            PPrint(((Select) node).getCondition());
            System.out.print(")");

        } else if (optype == OpType.PROJECT) {
            System.out.print("Project(");
            PPrint(((Project) node).getBase());
            System.out.print(")");

        } else if (optype == OpType.SCAN) {
            System.out.print(((Scan) node).getTabName());

        } else if (optype == OpType.DISTINCT) {
            System.out.println("Distinct(");
            PPrint(((Distinct) node).getBase());
            System.out.print(")");
        }
    }

    /**
     * print a tuple
     **/
    public static void PPrint(Tuple t) {
        for (int i = 0; i < t.data().size(); i++) {
            Object data = t.dataAt(i);
            if (data instanceof Integer) {
                System.out.print((Integer) data + "\t");
            } else if (data instanceof Float) {
                System.out.print((Float) data + "\t");
            } else {
                System.out.print(((String) data) + "\t");
            }
        }
        System.out.println();
    }

    /**
     * print a page of tuples
     **/
    public static void PPrint(Batch b) {
        for (int i = 0; i < b.size(); i++) {
            PPrint(b.get(i));
            System.out.println();
        }
    }

    /**
     * print an arraylist of tuples
     **/
    public static void PPrint(ArrayList<Tuple> l) {
        for (Tuple t : l) {
            PPrint(t);
        }
    }

    /**
     * Prints out the tuples in each of the Batches from a serialised file of Batches
     * @param fn file name of serialised Batch objects
     */
    public static void PPrint(String fn) {
        ObjectInputStream in = null;
        System.out.println("Printing out serialised file of Batch objects: " + fn);
        try {
            in = new ObjectInputStream(new FileInputStream(fn));
        } catch (Exception e) {
            System.err.println(" Error reading " + fn);
            System.exit(1);
        }

        while (true) {
            try {
                Batch data = (Batch) in.readObject();
                Debug.PPrint(data);
            } catch (ClassNotFoundException cnf) {
                System.err.println("Debug:Class not found for reading file  " + fn);
                System.exit(1);
            } catch (EOFException EOF) {
                /** At this point incomplete page is sent and at next call it considered
                 ** as end of file
                 **/
                System.out.println("EOF reached.");
                break;
            } catch (IOException e) {
                System.err.println("Debug:Error reading " + fn);
                System.exit(1);
            }
        }
    }
}













