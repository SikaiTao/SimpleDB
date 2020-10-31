package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator a;
    private TupleDesc atd;

    private String afieldName;
    private String gfieldName;

    private OpIterator result;

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
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;

        Type gfieldType = null;
        Type afieldType = child.getTupleDesc().getFieldType(afield);

        afieldName = aop.toString() + "(" + child.getTupleDesc().getFieldName(afield) + ")";

        Type[] aTypes;
        String[] aNames;

        if(this.gfield == Aggregator.NO_GROUPING){
            aTypes = new Type[1];
            aNames = new String[1];
            aTypes[0] = afieldType;
            aNames[0] = afieldName;

        }else {
            gfieldType = child.getTupleDesc().getFieldType(gfield);
            gfieldName = child.getTupleDesc().getFieldName(gfield);
            aTypes = new Type[2];
            aNames = new String[2];
            aTypes[0] = gfieldType;
            aNames[0] = gfieldName;
            aTypes[1] = afieldType;
            aNames[1] = afieldName;

        }
        this.atd = new TupleDesc(aTypes, aNames);

        if(afieldType == Type.INT_TYPE){
            this.a = new IntegerAggregator(gfield, gfieldType, afield, this.aop);
        }else if (afieldType == Type.STRING_TYPE){
            this.a = new StringAggregator(gfield, gfieldType, afield, this.aop);
        }else {
            throw new IllegalStateException("Only support type Integer or String");
        }


    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	    return gfieldName;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        return afieldName;
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    super.open();
	    child.open();
	    while (child.hasNext()){
	        a.mergeTupleIntoGroup(child.next());
        }
        child.close();
	    result = a.iterator();
	    result.open();

    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    if(!result.hasNext()) return null;
	    return result.next();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        result.rewind();
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
        return atd;
    }

    public void close() {
        result.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{ child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
	    child = children[0];
    }
    
}
