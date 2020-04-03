package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    //!!!与insert不同，delete要符合一定跳进
    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private OpIterator opIterator;
    private TupleDesc tupleDesc = null;
    private int count = 0;
    private boolean isOpened = false;
    private boolean isCalled = false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here

        transactionId = t;
        opIterator = child;
        tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{null});


    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        opIterator.open();
        super.open();
        isOpened = true;
        while(opIterator.hasNext()){
            try{
                Database.getBufferPool().deleteTuple(transactionId,opIterator.next());
                //把所有可以的插入进去,count表示目前插入了多少
                count++;
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }

    public void close() {
        // some code goes here
        if(isOpened){
            super.close();
            opIterator.close();
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if(isOpened){
            opIterator.rewind();
        }
        else{
            opIterator.open();
        }
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(!isCalled){
            BufferPool bufferPool = Database.getBufferPool();
            Tuple tuple = new Tuple(tupleDesc);
            tuple.setField(0,new IntField(count));
            isCalled =true;
            return tuple;
        }
        else{
            return null;
        }
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{opIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        opIterator = children[0];
    }

}
