package simpledb.multibuffer;

import simpledb.query.*;
import simpledb.record.Layout;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

import java.sql.SQLOutput;

/**
 * The Scan class for the multi-buffer version of the
 * <i>product</i> operator.
 * @author Edward Sciore
 */
public class NestedBlockJoinScan implements Scan {
   private Transaction tx;

   private Scan lhsscan, rhsscan;

   private Predicate pred;

    /**
     * Creates the scan class for the product of the LHS scan and a table.
     * @param tx the current transaction
     */
    public NestedBlockJoinScan(Transaction tx, Scan lhs, Scan rhs, Predicate pred) {
        this.tx = tx;
        this.lhsscan = lhs;
        this.rhsscan = rhs;
        this.pred = pred;
        beforeFirst();
    }

   /**
    * Positions the scan before the first record.
    * That is, the LHS scan is positioned at its first record,
    * and the RHS scan is positioned before the first record of the first chunk.
    * @see Scan#beforeFirst()
    */
   public void beforeFirst() {
       lhsscan.beforeFirst();
       rhsscan.beforeFirst();
       lhsscan.next();
   }

   /**
    * Moves to the next record in the current scan.
    * If there are no more records in the current chunk,
    * then move to the next LHS record and the beginning of that chunk.
    * If there are no more LHS records, then move to the next chunk
    * and begin again.
    * @see Scan#next()
    */
   public boolean next() {
       if (!rhsscan.next()){ // maybe a while loop
           if (!lhsscan.next()){
               return false;
           }
           rhsscan.beforeFirst();
           return next();
       }
       return pred.isSatisfied(this) || next();
   }

   /**
    * Closes the current scans.
    * @see Scan#close()
    */
   public void close() {
      lhsscan.close();
      rhsscan.close();
   }

   /**
    * Returns the value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see Scan#getVal(String)
    */
   public Constant getVal(String fldname) {
       if (lhsscan.hasField(fldname)){
           return lhsscan.getVal(fldname);}
       else {
           return rhsscan.getVal(fldname);
       }
   }

   /**
    * Returns the integer value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see Scan#getInt(String)
    */
   public int getInt(String fldname) {
       if (lhsscan.hasField(fldname))
           return lhsscan.getInt(fldname);
       else
           return rhsscan.getInt(fldname);
   }

   /**
    * Returns the string value of the specified field.
    * The value is obtained from whichever scan
    * contains the field.
    * @see Scan#getString(String)
    */
   public String getString(String fldname) {
       if (lhsscan.hasField(fldname))
           return lhsscan.getString(fldname);
       else
           return rhsscan.getString(fldname);
   }

   /**
    * Returns true if the specified field is in
    * either of the underlying scans.
    * @see Scan#hasField(String)
    */
   public boolean hasField(String fldname) {
      return lhsscan.hasField(fldname) && rhsscan.hasField(fldname);
   }
}

