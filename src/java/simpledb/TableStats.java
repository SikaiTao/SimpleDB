package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    private DbFile f;
    private int iocostperpage;
    private int tupleAmounts;
    private HashMap<Integer, IntHistogram> intHistMap;
    private HashMap<Integer, StringHistogram> stringHistMap;


    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        f = Database.getCatalog().getDatabaseFile(tableid);
        iocostperpage = ioCostPerPage;
        intHistMap = new HashMap<>();
        stringHistMap = new HashMap<>();
        HashMap<Integer, Integer> maxRecord = new HashMap<>();
        HashMap<Integer, Integer> minRecord = new HashMap<>();

        tupleAmounts = 0;
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
        TransactionId tid = new TransactionId();
        DbFileIterator tableIter = f.iterator(tid);


        try {
            //记录各个字段的最大值最小值
            tableIter.open();
            while (tableIter.hasNext()){
                Tuple t = tableIter.next();
                tupleAmounts ++;

                for (int i = 0; i < td.numFields(); i++){
                    Type type = td.getFieldType(i);
                    if (type == Type.INT_TYPE){
                        int thisValue = ((IntField) t.getField(i)).getValue();
                        int oldMax = maxRecord.getOrDefault(i, Integer.MIN_VALUE);
                        int oldMin = minRecord.getOrDefault(i, Integer.MAX_VALUE);

                        int newMax = thisValue > oldMax ? thisValue : oldMax;
                        int newMin = thisValue < oldMin ? thisValue : oldMin;

                        maxRecord.put(i, newMax);
                        minRecord.put(i, newMin);
                    }
                }

            }

            //初始化直方图
            for (int i = 0; i < td.numFields(); i++)
            {
                Type type = td.getFieldType(i);
                if (type == Type.INT_TYPE){
                    intHistMap.put(i, new IntHistogram(NUM_HIST_BINS, minRecord.get(i), maxRecord.get(i)));
                }else {
                    stringHistMap.put(i, new StringHistogram(NUM_HIST_BINS));
                }
            }

            //绘制直方图
            tableIter.rewind();
            while (tableIter.hasNext()){
                Tuple t = tableIter.next();
                for (int i = 0; i < td.numFields(); i++){
                    Type type = td.getFieldType(i);
                    if (type == Type.INT_TYPE){
                        intHistMap.get(i).addValue(((IntField) t.getField(i)).getValue());
                    }else {
                        stringHistMap.get(i).addValue(((StringField) t.getField(i)).getValue());
                    }
                }
            }


        }catch (Exception e){
            e.printStackTrace();
        }finally {
            tableIter.close();
        }


    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return iocostperpage * ((HeapFile)f).numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)(selectivityFactor * totalTuples());
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (constant.getType() == Type.INT_TYPE){
            return intHistMap.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        }else {
            return stringHistMap.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return tupleAmounts;
    }

}
