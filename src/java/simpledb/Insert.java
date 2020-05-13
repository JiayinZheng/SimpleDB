package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private OpIterator opIterator;
    private int tId;
    private TupleDesc tupleDesc = null;
    private HeapPage heapPage;
    private int count = 0;
    private boolean isOpened = false;
    private boolean isCalled = false;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException, TransactionAbortedException {
        // some code goes here
        BufferPool bufferPool = Database.getBufferPool();
        for(Page page : bufferPool.pageMap.values()){
            if(page.getId().getTableId()==tableId){
                if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))){
                    // 之前留下的隐患
                        throw new DbException("The tuple fails to match with the existed tupleDesc!");
                }
                else{
                    heapPage = (HeapPage) page;
                }
            }
        }
        tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{null});
        //要返回的数组是一个整数count，没有描述
        transactionId = t;
        opIterator = child;
        tId = tableId;
//        while (opIterator.hasNext()){
//            heapPage.insertTuple(opIterator.next());
//        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        opIterator.open();
        super.open();
        isOpened = true;
        while(opIterator.hasNext()){
           try{
               Database.getBufferPool().insertTuple(transactionId,tId,opIterator.next());
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
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.不用去重！
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
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
