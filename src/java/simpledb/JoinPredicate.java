package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;
    private int jpField1;
    private int jpField2;
    private Predicate.Op jpOp;
    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        jpField1 = field1;
        jpField2 = field2;
        jpOp = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        //return t.getField(preField).compare(preOp,preOperand);
        //return new Predicate(jpField1,jpOp,t1.getField(jpField1)).filter(t1)&&new Predicate(jpField2,jpOp,t2.getField(jpField2)).filter(t2);
        return t1.getField(jpField1).compare(jpOp,t2.getField(jpField2));//难点：理解->重点看两个fields是否满足约束条件（等于，大于....)
    }
    
    public int getField1()
    {
        // some code goes here
        return jpField1;
    }
    
    public int getField2()
    {
        // some code goes here
        return jpField2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return jpOp;
    }
}
