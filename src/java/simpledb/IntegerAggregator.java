package simpledb;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc atd;
    private HashMap<Field, Integer> minMaxGroup;
    private HashMap<Field, Integer> sumGroup;
    private HashMap<Field, Integer> countGroup;
    private HashMap<Field, Integer> avgGroup;

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

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        switch (this.what){
            case AVG:
                sumGroup = new HashMap<>();
                countGroup = new HashMap<>();
                avgGroup = new HashMap<>();
                break;
            case MIN:
            case MAX:
                minMaxGroup = new HashMap<>();
                break;
            case SUM:
                sumGroup = new HashMap<>();
                break;
            case COUNT:
                countGroup = new HashMap<>();
                break;
            default:
                throw new IllegalStateException("Shouldn't reach");
        }

    }

    private void catchAggregateTupleDesc(Tuple t){
        if (atd == null){
            if (gbfield == NO_GROUPING) {
                Type[] atype = new Type[1];
                String[] aName = new String[1];
                atype[0] = t.getTupleDesc().getFieldType(afield);
                aName[0] = what.toString()+"("+t.getTupleDesc().getFieldName(afield)+")";
                atd = new TupleDesc(atype, aName);
            }else {
                Type[] atypes = new Type[2];
                String[] aNames = new String[2];
                atypes[0] = gbfieldtype;
                aNames[0] = t.getTupleDesc().getFieldName(gbfield);
                atypes[1] = t.getTupleDesc().getFieldType(afield);
                aNames[1] = what.toString()+"("+t.getTupleDesc().getFieldName(afield)+")";
                atd = new TupleDesc(atypes, aNames);
            }
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
        catchAggregateTupleDesc(tup);
        Field groupField;
        if (gbfield == NO_GROUPING){
            groupField = null;
        }else {
            groupField = tup.getField(gbfield);
        }
        IntField tarField = (IntField)tup.getField(afield);

        switch (what){
            case AVG:
                sumGroup.put(groupField, sumGroup.getOrDefault(groupField, 0) + tarField.getValue());
                countGroup.put(groupField, countGroup.getOrDefault(groupField, 0) + 1);
                avgGroup.put(groupField, sumGroup.get(groupField)/countGroup.get(groupField));
                break;
            case MIN:
                int previous = minMaxGroup.getOrDefault(groupField, tarField.getValue());
                int smaller = tarField.getValue() < previous ? tarField.getValue():previous;
                minMaxGroup.put(groupField, smaller);
                break;
            case MAX:
                int previouss = minMaxGroup.getOrDefault(groupField, tarField.getValue());
                int bigger = tarField.getValue() > previouss ? tarField.getValue():previouss;
                minMaxGroup.put(groupField, bigger);
                break;
            case SUM:
                sumGroup.put(groupField, sumGroup.getOrDefault(groupField, 0) + tarField.getValue());
                break;
            case COUNT:
                countGroup.put(groupField, countGroup.getOrDefault(groupField, 0) + 1);
                break;
            default:
                throw new IllegalStateException("Shouldn't reach");
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
        LinkedList<Tuple> groupTuples;
        if (atd == null) throw new IllegalStateException("Shouldn't reach");

        switch (what){
            case AVG:
                groupTuples = getTuples(avgGroup);
                break;
            case MAX:
            case MIN:
                groupTuples = getTuples(minMaxGroup);
                break;
            case SUM:
                groupTuples = getTuples(sumGroup);
                break;
            case COUNT:
                groupTuples = getTuples(countGroup);
                break;
            default:
                throw new IllegalStateException("Shouldn't reach");

        }
        return new TupleIterator(atd, groupTuples);
    }

    private LinkedList<Tuple> getTuples(HashMap<Field, Integer> hm){
        LinkedList<Tuple> groupTuples = new LinkedList<>();
        for(Entry<Field, Integer> entry : hm.entrySet()){
            Tuple groupTuple = new Tuple(atd);
            int index = 0;
            if(gbfield != NO_GROUPING){
                groupTuple.setField(index++, entry.getKey());
            }
            groupTuple.setField(index, new IntField(entry.getValue()));
            groupTuples.add(groupTuple);
        }
        return groupTuples;

    }

}
