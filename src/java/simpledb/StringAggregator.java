package simpledb;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
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
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Tuple iagTuple = new Tuple(iagTupleDesc);

        if (notGroup) {
            iagTuple.setField(0, tup.getField(aggreField));
            tupleList.add(iagTuple);
        } else {
            if (valueMap.containsKey(tup.getField(groupField))) {
                //对Map进行更新
                Field f = tup.getField(groupField);
                int oldValue = valueMap.get(f);
                //新进入元组的值
                countMap.replace(f, countMap.get(f) + 1);
                switch (aggreOp) {
                    case COUNT:
                        valueMap.replace(f, valueMap.get(f)+ 1);
                        break;
                    default:
                        break;

                }

            } else {
                //不存在的话，设个表再放进去
                Field f = tup.getField(groupField);

                groupCount += 1;
                countMap.put(f, 1);
                switch (aggreOp) {
                    case COUNT:
                        valueMap.put(f, 1);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
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
    }

}
