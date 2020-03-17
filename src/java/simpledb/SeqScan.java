package simpledb;

import javax.xml.crypto.Data;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId transactionId;
    private int tabId;
    //最好别叫tableId，容易跟初始化参数混淆
    private String tabAlias;
    private DbFileIterator dbFileIterator;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) throws TransactionAbortedException, DbException {
        transactionId = tid;
        tabId =tableid;
        tabAlias = tableAlias;

    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        //这个地方要填代码！！！
        return Database.getCatalog().getTableName(tabId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
       return tabAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
       tabId = tableid;
       tabAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) throws TransactionAbortedException, DbException {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        dbFileIterator =  Database.getCatalog().getDatabaseFile(tabId).iterator(transactionId);
        dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc oldTd =  Database.getCatalog().getDatabaseFile(tabId).getTupleDesc();
        //改造前
        Type[] types = new Type[oldTd.numFields()];
        String[] fieldNames = new String[oldTd.numFields()];
        for(int i=0;i<oldTd.numFields();i++){
            types[i] = oldTd.getFieldType(i);
            if(tabAlias==null&&oldTd.getFieldName(i)==null){
                fieldNames[i] = "null.null";
                continue;
            }
            if(tabAlias==null){
                fieldNames[i] = "null."+oldTd.getFieldName(i);
            }
            else{
                if(oldTd.getFieldName(i)==null){
                    fieldNames[i] = tabAlias+".null";
                }
            }
            fieldNames[i] = tabAlias+"."+oldTd.getFieldName(i);
            System.out.println(fieldNames[i]);
        }
        return new TupleDesc(types,fieldNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return dbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
       return dbFileIterator.next();
    }

    public void close() {
        dbFileIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        dbFileIterator.rewind();
    }
}
