package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file;
    private TupleDesc tupleDesc;
    private int curNum;
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
        curNum = (int) (file.length()/BufferPool.getPageSize());//file大小和page多少有关
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
       return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
      return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        byte[] data = new byte[BufferPool.getPageSize()];
        Page page = null;
        try{
            RandomAccessFile randomAccessFile = new RandomAccessFile(getFile(), "r");
            int position = pid.getPageNumber()*BufferPool.getPageSize();//在哪页
            randomAccessFile.seek(position);
            randomAccessFile.read(data,0,data.length);
            page = new HeapPage((HeapPageId)pid,data);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int offset = page.getId().getPageNumber();
        if(offset>numPages()||offset<0){
            throw new IOException();
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(getFile(), "rw");
        randomAccessFile.seek(offset*BufferPool.getPageSize());//找到写的位置
        randomAccessFile.write(page.getPageData());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        //return BufferPool.DEFAULT_PAGES;
        return curNum;
        //return ;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> heapPages = new ArrayList<>();
        boolean inserted = false;
        for(int i=0;i<curNum;++i) {
            HeapPageId heapPageId = new HeapPageId(getId(), i);
            HeapPage insertedPage = null;
            insertedPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            //获取该页
            int empty = insertedPage.getNumEmptySlots();
            if (insertedPage.getNumEmptySlots() != 0) {
                //有空间，可以插入
                insertedPage.insertTuple(t);
                heapPages.add(insertedPage);
                insertedPage.markDirty(true,tid);
                inserted = true;
                break;
            }
        }
        if(!inserted){
            //需要新开一张page
            HeapPageId heapPageId = new HeapPageId(getId(), curNum);
            HeapPage insertedPage = null;
            byte[] newByte = HeapPage.createEmptyPageData();//是静态方法
            insertedPage = new HeapPage(heapPageId,newByte);
            writePage(insertedPage);//写入磁盘
            HeapPage newPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_WRITE);
            curNum++;
            newPage.insertTuple(t);
            newPage.markDirty(true,tid);//标识脏
            heapPages.add(newPage);
        }
        return heapPages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> heapPages = new ArrayList<>();
        PageId heapPageId = t.getRecordId().getPageId();//获取该元组所在的pageId
        boolean exist = false;
        for(int i=0;i<numPages();++i){
            if(heapPageId.getPageNumber()==i){
                //是这张表
                HeapPage deletedPage = null;
                deletedPage = (HeapPage)Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_WRITE);
                //获取该页
                deletedPage.deleteTuple(t);
                //这里会检查元组的存在性
            }
        }
        return heapPages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        class dbfileIterator implements DbFileIterator{

            int position=0;
            TransactionId transactionId;
            Iterator<Tuple> tupleIterator;//用来遍历页内元组
            HeapPageId heapPageId;
            public dbfileIterator(TransactionId transactionId) throws TransactionAbortedException, DbException {
                this.transactionId = transactionId;
                tupleIterator=null;

            }

            @Override
            public void open() throws DbException, TransactionAbortedException{
                position = 0;
                heapPageId = new HeapPageId(getId(),position);
                tupleIterator = ((HeapPage)Database.getBufferPool().getPage(transactionId, heapPageId,Permissions.READ_ONLY)).iterator();
            }

            @Override
            public boolean hasNext() throws TransactionAbortedException, DbException {
                //page内读或跨page读两种情况
                //case 1
                if(tupleIterator==null)
                    return false;
                if(tupleIterator.hasNext())
                    return true;
                //case 2
                if(position<curNum-1){
                    //还可以跨页读
                    position++;
                    heapPageId = new HeapPageId(getId(),position);
                    tupleIterator = ((HeapPage)Database.getBufferPool().getPage(transactionId, heapPageId,Permissions.READ_ONLY)).iterator();
                    return tupleIterator.hasNext();
                }
                return false;
            }

            @Override
            public Tuple next() throws TransactionAbortedException, DbException {
                if(!hasNext()){
                    throw new NoSuchElementException();
                }
                return tupleIterator.next();

            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();

            }

            @Override
            public void close() {
                position =0;
                tupleIterator = null;

            }
        }
        try{
            return new dbfileIterator(tid);
        }
        catch(Exception e){
            e.printStackTrace();
        }
        return null;
    }

}

