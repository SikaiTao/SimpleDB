package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int max;
    private int min;
    private int numT;
    private double w;
    private int[] bs;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.min = min;
    	this.max = max;
    	bs = new int[buckets];
    	this.numT = 0;
    	this.w = ((double) max + 1 - min) / buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	if (v <= max && v>=min){
            numT ++;
    	    bs[(int) ((v - min) / w)] ++;
        }

    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch (op){
            case LESS_THAN: {
                if (v > max)
                    return 1.0;
                if (v <= min)
                    return 0.0;

                int v_i = (int) ((v - min) / w);
                double tupleNumbeforeV = 0;

                for (int i = 0; i < v_i; i++) tupleNumbeforeV += bs[i];
                double percent = (v - min - v_i * w) / w;
                tupleNumbeforeV += bs[v_i] * percent;
                return tupleNumbeforeV / numT;
            }
            case GREATER_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);

            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);

            case GREATER_THAN:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);

            case EQUALS:
                return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.LESS_THAN, v);

            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);

            default:
                return -1.0;

        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder content = new StringBuilder();
        for (int b : bs) {
            content.append(b);
            content.append("|");
        }

        return String.format("Integer Hist : buckets num = %d, max = %d, min = %d, width = %.2f, content = %s",
                bs.length, max, min, w, content.toString());
    }
}
