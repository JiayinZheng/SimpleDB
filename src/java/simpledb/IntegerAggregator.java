package simpledb;

import javafx.util.Pair;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    private int groupField;
    private Type groupFieldType;
    private int aggreField;
    private Op aggreOp;
    private List<Tuple> tupleList = new LinkedList<>();
    private TupleDesc iagTupleDesc;
    private Field groupValue = null;

    private Map<Field,List<Tuple>> groupMap= new HashMap<>();
    private Map<Field,Integer> valueMap = new HashMap<>();
    private Map<Field,Integer> sumMap = new HashMap<>();
    private Map<Field,Integer> countMap = new HashMap<>();
    private int groupCount = 0;
    private int groupSum = 0;
    private boolean notGroup;
    private Tuple tup;
    //按照groupValue值装进不同的List,分别进行iterator操作
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        groupField = gbfield;
        groupFieldType = gbfieldtype;
        aggreField = afield;
        aggreOp = what;
        if(groupField==NO_GROUPING){
            //没有group操作，则返回的tuple只包括aggre字段值
            notGroup = true;
            Type[] types = new Type[1];
            types[0] = Type.INT_TYPE;
            iagTupleDesc = new TupleDesc(types);
        }
        else{
            //有group操作，则返回（groupVal,aggreVal)
            notGroup = false;
            Type[] types = new Type[2];
            types[0] = groupFieldType;
            types[1] = Type.INT_TYPE;
            iagTupleDesc = new TupleDesc(types);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Tuple iagTuple = new Tuple(iagTupleDesc);
//        if(!tup.getTupleDesc().equals(iagTupleDesc)){
//            return;
//        }
        if(notGroup){
            iagTuple.setField(0,tup.getField(aggreField));
            tupleList.add(iagTuple);
        }
        else{
            if(valueMap.containsKey(tup.getField(groupField))){
                //对Map进行更新
                Field f = tup.getField(groupField);
                int oldValue = valueMap.get(f);
                int thisValue = ((IntField)tup.getField(aggreField)).getValue();
                //新进入元组的值
                countMap.replace(f,countMap.get(f)+1);
                switch (aggreOp){
                    case MIN:
                        valueMap.replace(f,Math.min(oldValue,thisValue));
                        break;
                    case MAX:
                        valueMap.replace(f,Math.max(oldValue,thisValue));
                        break;
                    case COUNT:
                        valueMap.replace(f,oldValue+1);
                        break;
                    case SUM:
                        valueMap.replace(f,oldValue+thisValue);
                        sumMap.replace(f,sumMap.get(f)+thisValue);
                        //不是oldValue，是更新自增值！
                        break;
                    case SUM_COUNT:
                        groupSum = 0;
                        for(Field field:sumMap.keySet()){
                            groupSum+=sumMap.get(field);
                        }//算出当前的groupSum
                        break;
                    case AVG:
                        sumMap.replace(f,sumMap.get(f)+thisValue);
                        valueMap.replace(f, (sumMap.get(f)/countMap.get(f)));
                        break;
                    case SC_AVG:
                        int sum= 0;
                        for(Field field:sumMap.keySet()){
                            sum+=sumMap.get(field);
                        }//算出当前的group平均值
                        valueMap.replace(f,sum/countMap.size());
                        //将平均值再放进去
                    default:
                        break;

                }


//                iagTuple.setField(0,tup.getField(groupField));
//                iagTuple.setField(1,tup.getField(aggreField));
//                List<Tuple> tuples = groupMap.get(tup.getField(groupField));
//                tuples.add(iagTuple);
            }
            else{
                //不存在的话，直接put放进去
                Field f = tup.getField(groupField);
                int thisValue = ((IntField)tup.getField(aggreField)).getValue();
                groupCount +=1;
                countMap.put(f,1);
                sumMap.put(f,thisValue);
                switch (aggreOp){
                    case MIN:
                    case MAX:
                        valueMap.put(f,thisValue);
                        break;
                    case COUNT:
                        valueMap.put(f,1);
                        break;
                    case SUM:
                        valueMap.put(f,thisValue);
                        break;
                    case SUM_COUNT:
                        valueMap.put(f,null);//只是为了循环时可以循环到的占位符
                        break;
                    case AVG:
                        valueMap.put(f,thisValue);//只是为了循环时可以循环到的占位符
                        break;
                    case SC_AVG:
                        groupSum = thisValue;
                        valueMap.put(f,thisValue);//只是为了循环时可以循环到的占位符
                    default:
                        break;

                }
            }

        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        if(notGroup){
            return new TupleIterator(iagTupleDesc,tupleList);
        }
        else{
            //tupleList.clear();
            for(Field f:valueMap.keySet()){
                Tuple t = new Tuple(iagTupleDesc);
                t.setField(0,f);
                t.setField(1,new IntField(valueMap.get(f)));
                tupleList.add(t);
            }
            return new TupleIterator(iagTupleDesc,tupleList);
        }
//        class iagOpIterator implements OpIterator{
//
//            private int index;
//            private boolean isOpened;
//            @Override
//            public void open() throws DbException, TransactionAbortedException {
//                index = -1;
//                isOpened = true;
//            }
//
//            @Override
//            public boolean hasNext() throws DbException, TransactionAbortedException {
//                if(!isOpened)
//                    return false;
//                return index+1<tupleList.size();
//            }
//
//            @Override
//            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
//                if(hasNext()){
//                    return tupleList.get(++index);
//                }
//                return null;
//            }
//
//            @Override
//            public void rewind() throws DbException, TransactionAbortedException {
//                open();
//
//            }
//
//            @Override
//            public TupleDesc getTupleDesc() {
//                return iagTupleDesc;
//            }
//
//            @Override
//            public void close() {
//                isOpened = false;
//            }
//        }
//        return new iagOpIterator();
    }

}
