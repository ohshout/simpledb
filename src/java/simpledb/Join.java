package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        this.p = p;
        this.child1 = child1;
        this.child2 = child2;
    }

    private JoinPredicate p;
    private OpIterator child1;
    private OpIterator child2;
    private Tuple t1 = null;

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return this.p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        TupleDesc td = child1.getTupleDesc();
        int fieldNo = this.p.getField1();
        return td.getFieldName(fieldNo);
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        TupleDesc td = child2.getTupleDesc();
        int fieldNo = this.p.getField2();
        return td.getFieldName(fieldNo);
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        System.out.println("new test start marker");
        child1.open();
        t1 = child1.next();
        child2.open();
        super.open();
    }

    public void close() {
        // some code goes here
        child1.close();
        child2.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        Tuple t2 = null;

        while (true) {
            while (child2.hasNext()) {
                t2 = child2.next();

                if (p.filter(t1, t2)) {
                    Tuple t = new Tuple(this.getTupleDesc());
                    int idx = 0;
                    for (int i = 0; i < t1.getTupleDesc().numFields(); i++) {
                        t.setField(idx, t1.getField(i));
                        idx++;
                    }
                    for (int i = 0; i < t2.getTupleDesc().numFields(); i++) {
                        t.setField(idx, t2.getField(i));
                        idx++;
                    }

                    return t;
                }
            }
            
            // use next t1
            if (!child1.hasNext()) {
                return null;
            }
            t1 = child1.next();
            child2.rewind();
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] { this.child1, this.child2 };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child1 = children[0];
        this.child2 = children[1];
    }

}
