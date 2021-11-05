package simpledb.multibuffer.nestedblock;

import simpledb.query.Constant;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.record.TableScan;
import simpledb.tx.BufferList;
import simpledb.tx.Transaction;

/**
 * The Scan class for the a nested block loop
 * join operation
 * @author Aryan Wadhwani
 */
public class NestedBlockJoinScan implements Scan {
   private Transaction tx;

   private TableScan lhsscan, rhsscan;

   private Predicate pred;

    /**
     * Creates the scan class for the product of the LHS scan and a table.
     * @param tx the current transaction
     */
    public NestedBlockJoinScan(Transaction tx, TableScan lhs, TableScan rhs, Predicate pred) {
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
       if (rhsscan.atEndOfBlock()){ // Finished the right block
           if (lhsscan.atEndOfBlock()){ // Finished the left block too
               if (!rhsscan.next()){ // No more right blocks: We need to move to next left block
                if (!lhsscan.next()){
                    return false; // No more left blocks: We're done
                } else {
                    rhsscan.beforeFirst(); // Restart reading from the beginning
                    rhsscan.next();
                }
               } else {
                   lhsscan.restartBlock(); // Restart the left block, scan through everything in the right
                   lhsscan.next();
                   // Maybe include a lhsscan.next() here?
               }
           } else { // Left block isn't done: keep going through left block records
               lhsscan.next();
               rhsscan.restartBlock();
               rhsscan.next();
               // Maybe include a rhsscan.next() here?
           }
       } else {
         rhsscan.next(); // Keep checking right blocks with the left block
       }
       return pred.isSatisfied(this) || next(); // Check if predicate works here, if not, keep going
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

