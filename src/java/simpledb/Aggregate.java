package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator  {
    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    private OpIterator agIterator;
    private int aggreField;
    private int grField;
    private Aggregator.Op aggreOp;
    private Aggregator aggregator;
    //根据类型判断是int还是string来具体实现aggregator
    private OpIterator getAgIterator;
    TupleDesc tupleDesc;
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        agIterator = child;
        aggreField = afield;
        grField = gfield;
        aggreOp = aop;
        if(agIterator.getTupleDesc().getFieldType(afield)==Type.INT_TYPE&&grField!=-1){
            aggregator = new IntegerAggregator(grField,agIterator.getTupleDesc().getFieldType(gfield),afield,aop);
        }
        else{
            if(agIterator.getTupleDesc().getFieldType(afield)==Type.STRING_TYPE&&grField!=-1){
                aggregator = new StringAggregator(grField,agIterator.getTupleDesc().getFieldType(gfield),afield,aop);
            }
            else{
                if(agIterator.getTupleDesc().getFieldType(afield)==Type.INT_TYPE&&grField==-1){
                    aggregator = new IntegerAggregator(Aggregator.NO_GROUPING,null,afield,aop);
                }
                else{
                    aggregator = new StringAggregator(Aggregator.NO_GROUPING,null,afield,aop);
                }
            }
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	  if(grField!=-1){
	      return grField;
      }
	  else{
	      return Aggregator.NO_GROUPING;
      }
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        if(grField!=-1){
            return aggregator.iterator().getTupleDesc().getFieldName(0);
        }
        else{
            return null;
        }
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return aggreField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        if(grField!=-1){
            //参数[0]是groupValue
            return aggregator.iterator().getTupleDesc().getFieldName(1);
        }
        else{
            return aggregator.iterator().getTupleDesc().getFieldName(0);
        }
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	   return aggreOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    agIterator.open();
	    super.open();
	    while(agIterator.hasNext()){
	        aggregator.mergeTupleIntoGroup(agIterator.next());
	        //当还有未进入的成员时，进组进行聚合操作
        }
	    //全部进入后打开
        getAgIterator = aggregator.iterator();
	    getAgIterator.open();
	    super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    if(getAgIterator.hasNext()){
	        return getAgIterator.next();
        }
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    getAgIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
        if(tupleDesc!=null){
            return tupleDesc;
        }
        if(groupField()==Aggregator.NO_GROUPING){
            Type[] types = new Type[1];
            types[0] = Type.INT_TYPE;
            String[] strings = new String[1];
            strings[0] = agIterator.getTupleDesc().getFieldName(aggreField);
            tupleDesc = new TupleDesc(types,strings);
        }
        else{
            Type[] types = new Type[2];
            types[0] = Type.INT_TYPE;
            types[1] = Type.INT_TYPE;
            String[] strings = new String[2];
            strings[0] =  agIterator.getTupleDesc().getFieldName(grField);
            strings[1] = agIterator.getTupleDesc().getFieldName(aggreField);
            tupleDesc = new TupleDesc(types,strings);
        }
	return tupleDesc;
    }

    public void close() {
	// some code goes here
        super.close();
        getAgIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
	return new OpIterator[]{agIterator};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	    agIterator = children[0];
    }
    
}
