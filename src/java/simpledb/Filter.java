package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter<bool> extends Operator {

    private static final long serialVersionUID = 1L;
    private List<OpIterator> opIteratorList = new ArrayList<>();
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    Predicate filterPre;
    OpIterator filterOp;
    boolean isOpened = false;
    public Filter(Predicate p, OpIterator child) {
        
        filterPre = p;
        filterOp =  child;
        opIteratorList.add(child);
    }

    public Predicate getPredicate() {
       return filterPre;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return filterOp.getTupleDesc();
        //这个OpIterator就是SeqScan实现过的
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        filterOp.open();
        super.open();
        isOpened = true;
    }

    public void close() {
        // some code goes here
        if(!isOpened){
            return ;
        }
        super.close();
        filterOp.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if(!isOpened){
           open();
        }
        filterOp.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        while(filterOp.hasNext()){
            Tuple t = filterOp.next();
            if(t!=null){
                if(filterPre.filter(t)){
                    //通过约束测试
                    return t;
                }
            }
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        //return (OpIterator[]) opIteratorList.toArray();
        return new OpIterator[] {filterOp};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        opIteratorList.clear();
        for(int i=0;i<children.length;i++){
            opIteratorList.add(children[i]);
        }
    }

}
