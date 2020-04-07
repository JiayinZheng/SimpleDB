package simpledb;

import javafx.util.Pair;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int numP;
    private TransactionId transactionId;//给flush用
    private Queue<Page> usedQueue = new PriorityQueue<>(DEFAULT_PAGES, new Comparator<Page>() {

        @Override
        public int compare(Page o1, Page o2) {
            return o1.getUsedTimes()-o2.getUsedTimes();
        }
    });//存放使用次数和pageId的对应
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    Map<PageId,Page> pageMap = new HashMap<>();//存放缓冲池的所有pages
    public BufferPool(int numPages) {
        numP = numPages;

    }

    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        if(pageMap.containsKey(pid)){
            HeapPage heapPage = (HeapPage) pageMap.get(pid);
            heapPage.setUsedTimes(heapPage.getUsedTimes()+1);
            for(Page page:usedQueue){
                if(page.getId().equals(pid)){
                    //找到后更新
                    page.setUsedTimes(page.getUsedTimes()+1);
                    break;
                }
            }
            return pageMap.get(pid);
        }
        else{
            if(pageMap.size()>numP){
                evictPage();
                //需要evict+add
                //默认移走第一个

//                for(PageId pageId:pageMap.keySet()){
//                    if(pageId!=null){
//                        pageMap.remove(pageId);
//                        break;
//                    }
//                }
            }
            HeapFile heapFile = (HeapFile)Database.getCatalog().getDatabaseFile(pid.getTableId());
            //从数据库中获得DBfile
            Page page =  heapFile.readPage(pid);
            page.setUsedTimes(1);
            usedQueue.add(page);
            pageMap.put(pid,page);
            return page;
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        //catalog连接文件！！！
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> insertedPages  = heapFile.insertTuple(tid,t);
        //找到进行插入工作的所有pages
        for(Page p:insertedPages){
            p.markDirty(true,tid);
            if(!pageMap.containsValue(p)){
                //没有的话，要加入缓冲池
                int size = pageMap.keySet().size();
                if(pageMap.keySet().size()<numP){
                    //还有地方加入
                    pageMap.put(p.getId(),p);
                }
                else{
                    //需要evict+add
                    //默认移走第一个
                    evictPage();
//                    for(PageId pageId:pageMap.keySet()){
//                        if(pageId!=null){
//                            pageMap.remove(pageId);
//                            break;
//                        }
//                    }

                    pageMap.put(p.getId(),p);
                }
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        //关系 tuple有recordId->pageId->tableId
        ArrayList<Page> deletedPages = heapFile.deleteTuple(tid, t);
        for (Page p : deletedPages) {
            p.markDirty(true, tid);
            //脏：进行插入更新删除等操作
            if (!pageMap.containsValue(p)) {
                //没有的话，要加入缓冲池
                if (pageMap.size() < numP) {
                    //还有地方加入
                    pageMap.put(p.getId(), p);
                } else {
                    //需要evict+add
                    //默认移走第一个
                    evictPage();
//                    for (PageId pageId : pageMap.keySet()) {
//                        if (pageId != null) {
//                            pageMap.remove(pageId);
//                            break;
//                        }
//                    }
                    pageMap.put(p.getId(), p);
                }
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        for(Page page:pageMap.values()){
            flushPage(page.getId());
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        //这个不确定
        for(Page page: pageMap.values()){
            if(page.getId().equals(pid)){
                pageMap.remove(pid);
                Database.getLogFile().recover();
            }
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        //即变为不dirty
        HeapFile heapFile = (HeapFile) Database.getCatalog().getDatabaseFile(pid.getTableId());
        HeapPage heapPage = (HeapPage) pageMap.get(pid);
        heapPage.markDirty(true,null);//这个transactionid不知道
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //移除使用次数最少的，后续应用最小堆实现，提高效率
        pageMap.remove(usedQueue.element().getId());
        usedQueue.poll();
        //自己循环遍历的（空间少，但时间复杂度高）
//        PageId minPage = null;
//        int minUsed = 999999;
//        for (PageId pageId : pageMap.keySet()) {
//            if (pageMap.get(pageId).getUsedTimes()<minUsed) {
//                minUsed = pageMap.get(pageId).getUsedTimes();
//                minPage = pageId;
//            }
//        }
//        pageMap.remove(minPage);

    }

}
