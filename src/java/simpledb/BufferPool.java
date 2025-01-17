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
    public class PageLock{

        public Map<PageId,List<TransactionId>> sharedMap = new ConcurrentHashMap<>();
        //线程安全,用于记录共享锁，本来想string区分，但是查找是否有独占锁效率太低
        public Map<PageId,TransactionId> excluMap = new ConcurrentHashMap<>();
        //记录独占锁
        public Map<TransactionId,PageId> hasExclu = new ConcurrentHashMap<>();
        //为释放锁时效率和方便,记录哪些交易对页有独占锁
        public Map<TransactionId,PageId> hasShared = new ConcurrentHashMap<>();
        //记录哪些交易对页有共享锁
        public Map<PageId,Object> isLocked = new ConcurrentHashMap<>();
        //记录哪些页面被锁上
    }
    public class TupleLock{

        public Map<RecordId,List<TransactionId>> sharedMap = new ConcurrentHashMap<>();
        //线程安全,用于记录共享锁，本来想string区分，但是查找是否有独占锁效率太低
        public Map<RecordId,TransactionId> excluMap = new ConcurrentHashMap<>();
        //记录独占锁
        public Map<TransactionId,RecordId> hasExclu = new ConcurrentHashMap<>();
        //为释放锁时效率和方便,记录哪些交易对页有独占锁
        public Map<TransactionId,RecordId> hasShared = new ConcurrentHashMap<>();
        //记录哪些交易对页有共享锁
        public Map<PageId,Object> isLocked = new ConcurrentHashMap<>();
        //记录哪些页面被锁上
    }

    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    static int timeLimit = 10000;//tuple级锁很慢，所以由表级锁的1000改成10000
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    private int numP;
    private TransactionId transactionId;//给flush用
    public Map<TransactionId,Long> beginTimeShared = new ConcurrentHashMap<>();
    public Map<TransactionId,Long> beginTimeExcluded = new ConcurrentHashMap<>();

    public Map<TransactionId,Long> beginExcludedInsert= new ConcurrentHashMap<>();
    public Map<TransactionId,Long> beginExcludedDelete = new ConcurrentHashMap<>();
    boolean isTimed = true;//标志用时间策略或者依赖图策略
    boolean abortSelf = true;//标志抛弃自己还是抛弃别人
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
    //Map<PageId,Pair<TransactionId,String>> trackLocks = new HashMap<>();//记录page的锁
    //放弃这种，因为pageid不可相同，如果采用identitymap则无法使用contains
    //List<pageLock> pageLocks = new LinkedList<>();
    //放弃这种，因为判断起来麻烦
    PageLock pageLock = new PageLock();//标记页面锁的状态
    TupleLock tupleLock = new TupleLock();//标记元组锁的状态
    boolean isPage = true;//1 page 0 tuple
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
    private Object beLocked(PageId pageId){
        if(pageLock.isLocked.containsKey(pageId)){
            return pageLock.isLocked.get(pageId);
        }
        else{
            pageLock.isLocked.put(pageId,new Object());
            return pageLock.isLocked.get(pageId);
        }
    }
    public void getTuple(TransactionId tid, RecordId rid, Permissions perm) throws TransactionAbortedException, DbException {
        if (!isPage) {
            if (perm == Permissions.READ_WRITE) {
                boolean flag = true;
                boolean flag1 = true;
                while (flag) {
                    synchronized (tupleLock.sharedMap) {
                        if (tupleLock.sharedMap.containsKey(rid)) {
                            List<TransactionId> list = tupleLock.sharedMap.get(rid);
                            if (list.contains(tid)) {
                                //把共享锁的移走
                                list.remove(tid);
                            }
                            if (list.size() != 0) {
                                //只有共享锁被清除，才可以继续独占锁
                                flag1 = false;
                            }
                        }
                    }
                    if (flag1) {
                        //共享锁全部清除或者没有共享锁才可尝试加独占锁
                        synchronized (tupleLock.excluMap) {
                            if (tupleLock.excluMap.containsKey(rid)) {
                                {
                                    if (tupleLock.excluMap.get(rid).equals(tid)) {
                                        //已是独占锁
                                        flag1 = false;
                                        break;
                                    }
                                }
                            } else {
                                tupleLock.excluMap.put(rid, tid);//当没有占用时加入
                                flag1 = false;
                                break;
                            }
                        }
                    }

                    //检查死锁
                    if (tupleLock != null) {
                        if (!tupleLock.excluMap.containsKey(rid) || !tupleLock.excluMap.get(rid).equals(tid)) {
                            if (!beginTimeExcluded.containsKey(tid)) {
                                beginTimeExcluded.put(tid, System.currentTimeMillis());
                            } else {
                                if ((System.currentTimeMillis() - beginTimeExcluded.get(tid)) > timeLimit) {
                                    if (abortSelf) {
                                        throw new TransactionAbortedException();//抛弃自己
                                    } else {
                                        //找到现在碍事的那个
                                        {
                                            try {
                                                if (tupleLock.excluMap.containsKey(rid)) {
                                                    transactionComplete(tupleLock.excluMap.get(rid), false);
                                                }
                                                if (tupleLock.sharedMap.containsKey(rid)) {
                                                    //因为共享锁碍事
                                                    List<TransactionId> lists = tupleLock.sharedMap.get(rid);
                                                    for (TransactionId eachT : lists) {
                                                        if (!eachT.equals(tid)) {
                                                            //不是这个
                                                            transactionComplete(eachT, false);
                                                        }
                                                    }
                                                }
                                                tupleLock.excluMap.put(rid, tid);
                                                break;
                                            } catch (Exception e) {
                                                e.printStackTrace();
                                            }
                                        }
                                    }

                                }
                            }
                        }
                    }
                }
            }
            else{
                boolean flag = true;
                while(flag) {
                    //一直循环直至进入同步代码块完成上锁，注意循环在外面，否则就是抢到资源的不停循环，而其他的进不来
                    if (Database.getBufferPool().tupleLock != null) {
                        synchronized (Database.getBufferPool().tupleLock) {
                            //参数在过程中不可被改变
                            if (!Database.getBufferPool().tupleLock.excluMap.containsKey(rid) || Database.getBufferPool().tupleLock.excluMap.get(rid).equals(tid)) {
                                //没有独占锁，或本身独占
                                if (Database.getBufferPool().tupleLock.sharedMap.containsKey(rid)) {
                                    //该页上已有共享锁
                                    List<TransactionId> list = Database.getBufferPool().tupleLock.sharedMap.get(rid);
                                    if (!list.contains(tid)) {
                                        //列表中没有该tid
                                        list.add(tid);
                                        flag = false;
                                    } else {
                                        flag = false;
                                    }
                                } else {
                                    List<TransactionId> list = new LinkedList<>();
                                    list.add(tid);
                                    Database.getBufferPool().tupleLock.sharedMap.put(rid, list);
                                    flag = false;
                                }
                            }
                        }
                    }
                    //检测死锁并解决
                    if(Database.getBufferPool().tupleLock!=null&&Database.getBufferPool().tupleLock.sharedMap.containsKey(rid)) {
                        if (!Database.getBufferPool().tupleLock.sharedMap.get(rid).contains(tid)) {
                            if (!Database.getBufferPool().beginTimeShared.containsKey(tid)) {
                                Database.getBufferPool().beginTimeShared.put(tid, System.currentTimeMillis());
                            } else {
                                if ((System.currentTimeMillis() - Database.getBufferPool().beginTimeShared.get(tid)) > Database.getBufferPool().timeLimit) {
                                    throw new TransactionAbortedException();
                                }

                            }
                        }
                    }
                }
            }
        }
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
            throws TransactionAbortedException, DbException{
        Page page;

        if(isPage){
            if(perm.equals(Permissions.READ_ONLY)){
                //只需要共享锁
                boolean flag = true;
                while(flag){
                    //一直循环直至进入同步代码块完成上锁，注意循环在外面，否则就是抢到资源的不停循环，而其他的进不来
                    if(pageLock!=null){
                        synchronized (pageLock){
                            //参数在过程中不可被改变
                            if(!pageLock.excluMap.containsKey(pid)||pageLock.excluMap.get(pid).equals(tid)){
                                //没有独占锁，或本身独占
                                if(pageLock.sharedMap.containsKey(pid)){
                                    //该页上已有共享锁
                                    List<TransactionId> list = pageLock.sharedMap.get(pid);
                                    if(!list.contains(tid)){
                                        //列表中没有该tid
                                        list.add(tid);
                                        flag= false;
                                    }
                                    else{
                                        flag= false;
                                    }
                                }
                                else{
                                    List<TransactionId> list = new LinkedList<>();
                                    list.add(tid);
                                    pageLock.sharedMap.put(pid,list);
                                    flag = false;
                                }
                            }
                        }

                    }
                    if(pageLock!=null&&pageLock.sharedMap.containsKey(pid)) {
                        if (!pageLock.sharedMap.get(pid).contains(tid)) {
                            if (!beginTimeShared.containsKey(tid)) {
                                beginTimeShared.put(tid, System.currentTimeMillis());
                            } else {
                                if ((System.currentTimeMillis() - beginTimeShared.get(tid)) > timeLimit) {
                                    if(abortSelf){
                                        throw new TransactionAbortedException();
                                    }
                                    else{
                                        //找到现在碍事的那个
                                        try{
                                            if(pageLock.excluMap.containsKey(pid)){
                                                transactionComplete(pageLock.excluMap.get(pid),false);
                                            }
                                            pageLock.sharedMap.get(pid).add(tid);
                                            break;
                                        }
                                        catch (Exception e){
                                            e.printStackTrace();
                                        }
                                    }
                                }
                                else {
                                    try{
                                        Thread.sleep(100);
                                    }
                                    catch (InterruptedException e){
                                        e.printStackTrace();
                                    }

                                }
                            }
                        }
                    }

//                else{
//                    beginTimeShared.remove(tid);
//                }

                }

            }
            else{
                //需要独占锁
                boolean flag = true;
                boolean flag1 = true;
                while(flag){
                    synchronized (pageLock.sharedMap){
                        if(pageLock.sharedMap.containsKey(pid)){
                            List<TransactionId> list = pageLock.sharedMap.get(pid);
                            if(list.contains(tid)){
                                //把共享锁的移走
                                list.remove(tid);
                            }
                            if(list.size()!=0){
                                //只有共享锁被清除，才可以继续独占锁
                                flag1=false;
                            }
                        }
                    }
                    if(flag1){
                        //共享锁全部清除或者没有共享锁才可尝试加独占锁
                        synchronized (pageLock.excluMap){
                            if(pageLock.excluMap.containsKey(pid)){
                                {
                                    if(pageLock.excluMap.get(pid).equals(tid)){
                                        //已是独占锁
                                        flag1=false;
                                        break;
                                    }
                                }
                            }
                            else{
                                pageLock.excluMap.put(pid,tid);//当没有占用时加入
                                flag1=false;
                                break;
                            }
                        }
                    }


//                if(!holdsLock(tid,pid)){
//                    if(!beginTimeExcluded.containsKey(tid)){
//                        beginTimeExcluded.put(tid,System.currentTimeMillis());
//                    }
//                    else{
//                        if((System.currentTimeMillis()-beginTimeExcluded.get(tid))>timeLimit){
//                            throw new TransactionAbortedException();
//                        }
//                    }
//
//                }
//                else{
//                    beginTimeExcluded.remove(tid);
//                }
                    //检查死锁
                    if(pageLock!=null) {
                        if (!pageLock.excluMap.containsKey(pid)||!pageLock.excluMap.get(pid).equals(tid)) {
                            if (!beginTimeExcluded.containsKey(tid)) {
                                beginTimeExcluded.put(tid, System.currentTimeMillis());
                            } else {
                                if ((System.currentTimeMillis() - beginTimeExcluded.get(tid)) > timeLimit) {
                                    if(abortSelf){
                                        throw new TransactionAbortedException();//抛弃自己
                                    }
                                    else{
                                        //找到现在碍事的那个
                                        try{
                                            if(pageLock.excluMap.containsKey(pid)){
                                                transactionComplete(pageLock.excluMap.get(pid),false);
                                            }
                                            if(pageLock.sharedMap.containsKey(pid)){
                                                //因为共享锁碍事
                                                List<TransactionId> lists = pageLock.sharedMap.get(pid);
                                                for(TransactionId t:lists){
                                                    if(!t.equals(tid)){
                                                        //不是这个事务，是别人
                                                        transactionComplete(t,false);
                                                    }
                                                }
                                            }
                                            pageLock.excluMap.put(pid,tid);
                                            break;
                                        }
                                        catch (Exception e){
                                            e.printStackTrace();
                                        }
                                    }

                                }
                            }
                        }
                    }
                }
            }

        }

        synchronized (pageMap){
            if(pageMap.containsKey(pid)){
                Page heapPage = pageMap.get(pid);
                //用多态！！！
                heapPage.setUsedTimes(heapPage.getUsedTimes()+1);
                synchronized (usedQueue){
                    for(Page p:usedQueue){
                        if(p.getId().equals(pid)){
                            //找到后更新
                            p.setUsedTimes(p.getUsedTimes()+1);
                            break;
                        }
                    }
                }

                page =  pageMap.get(pid);
            }
            else{
                if(pageMap.size()+1>numP){
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
                DbFile heapFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                // HeapFile heapFile = (HeapFile)Database.getCatalog().getDatabaseFile(pid.getTableId());
                //这个地方用多态实现
                //从数据库中获得DBfile
                page =  heapFile.readPage(pid);
                if(!isPage&&perm==Permissions.READ_ONLY){
                    //把这页上所有元组
                }
                page.setUsedTimes(1);
                synchronized (usedQueue){
                    usedQueue.add(page);
                }

                pageMap.put(pid,page);

            }

        }


        return page;
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
        if(pageLock.excluMap.containsKey(pid)){
            synchronized (pid){
                //参数为要锁的对象
                if(pageLock.excluMap.get(pid).equals(tid)) {
                    pageLock.excluMap.remove(pid);
                }

            }
        }
        else{
            if(pageLock.sharedMap.containsKey(pid)){
                synchronized (pageLock.sharedMap.get(pid)){
                    pageLock.sharedMap.get(pid).remove(tid);
                    //不用判断contains，如果没有方法会失败
                }
            }
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        if((!pageLock.sharedMap.containsKey(p))&&(!pageLock.excluMap.containsKey(p)))
            return false;
        if(pageLock.sharedMap.containsKey(p)){
            synchronized (pageLock.sharedMap.get(p)){
                return pageLock.sharedMap.get(p).contains(tid);
            }
        }
        else{

            synchronized (pageLock.excluMap.get(p)){
                return pageLock.excluMap.get(p).equals(tid);
            }
        }

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

        Set<Page> dirtyPages = new LinkedHashSet<>();
        //有可能某页上有update情况，所以需要去重
        //清空锁
        //应该需要调用releasePage,但是那样得不到不好和commit是否结合
        if(isPage){
            //释放页锁
            if(pageLock.excluMap.containsValue(tid)){
                for(PageId pageId: pageLock.excluMap.keySet()){
                    if(pageLock.excluMap.get(pageId)!=null){
                        if (pageLock.excluMap.get(pageId).equals(tid)){
                            synchronized (dirtyPages){
                                if(pageMap.get(pageId)!=null){
                                    dirtyPages.add(pageMap.get(pageId));
                                }
                            }
                            synchronized (pageLock.excluMap){
                                pageLock.excluMap.remove(pageId);
                            }
                        }
                    }
                }
            }
            synchronized (pageLock.sharedMap){
                for(PageId pageId: pageLock.sharedMap.keySet()){

                    if(pageLock.sharedMap.get(pageId).contains(tid)){
                        synchronized (dirtyPages){
                            if(pageMap.get(pageId)!=null){
                                dirtyPages.add(pageMap.get(pageId));
                            }
                        }
                        {
                            pageLock.sharedMap.get(pageId).remove(tid);
                            if(pageLock.sharedMap.get(pageId).size()==0){
                                pageLock.sharedMap.remove(pageId);
                            }
                        }
                    }
                }
            }
        }
        else{
            //释放tuple锁
            if(tupleLock.excluMap.containsValue(tid)){
                for(RecordId recordId:tupleLock.excluMap.keySet()){
                    if(tupleLock.excluMap.get(recordId)!=null){
                        if(tupleLock.excluMap.get(recordId).equals(tid)){
                            synchronized (tupleLock.excluMap){
                                tupleLock.excluMap.remove(recordId);
                            }
                        }
                    }
                }
            }
            synchronized (tupleLock.sharedMap){
                for(RecordId recordId: tupleLock.sharedMap.keySet()){

                    if(tupleLock.sharedMap.get(recordId).contains(tid)){
                        synchronized (dirtyPages){
                            if(pageMap.get(recordId.getPageId())!=null){
                                dirtyPages.add(pageMap.get(recordId.getPageId()));
                            }
                        }
                        {
                            tupleLock.sharedMap.get(recordId).remove(tid);
                            if(tupleLock.sharedMap.get(recordId).size()==0){
                                tupleLock.sharedMap.remove(recordId);
                            }
                        }
                    }
                }
            }

        }

        if(commit){
            //脏页写入磁盘
            synchronized (dirtyPages){
                if(dirtyPages.size()!=0){
                    for(Page p:dirtyPages){
                        flushPage(p.getId(),tid);
                        p.setBeforeImage();
                    }
                }
            }
        }
        else{
            //恢复原始状态
            synchronized (pageMap){
                if(dirtyPages.size()!=0){
                    for(Page p:dirtyPages){
                        pageMap.replace(p.getId(),p.getBeforeImage());
                    }
                }
            }

        }
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
        DbFile heapFile = Database.getCatalog().getDatabaseFile(tableId);
        if(!isPage){
            //加tuple-level独占锁
            getTuple(tid,t.getRecordId(),Permissions.READ_WRITE);
        }
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
        DbFile heapFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        //关系 tuple有recordId->pageId->tableId
        ArrayList<Page> deletedPages = null;
        if(!isPage){
            //加tuple-level独占锁
            getTuple(tid,t.getRecordId(),Permissions.READ_WRITE);
        }
        deletedPages = heapFile.deleteTuple(tid, t);

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
        if(pageMap.containsKey(pid)){
            pageMap.remove(pid);
            Database.getLogFile().recover();
        }
//        for(Page page: pageMap.values()){
//
//            if(page.getId().equals(pid)){
//                pageMap.remove(pid);
//                page.markDirty(false,transactionId);
//                Database.getLogFile().recover();
//            }
//        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid, TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1
        //即变为不dirty
        DbFile heapFile = Database.getCatalog().getDatabaseFile(pid.getTableId());

        Page heapPage = pageMap.get(pid);
        heapFile.writePage(heapPage);
        heapPage.markDirty(false,tid);//这个transactionid不知道
    }
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        //即变为不dirty

        DbFile  heapFile =  Database.getCatalog().getDatabaseFile(pid.getTableId());
        Page heapPage = pageMap.get(pid);
        heapPage.markDirty(false,null);//这个transactionid不知道
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
        boolean canBeEvicted = false;
        Page evictPage = null;
        List<Page> pageList = new LinkedList<>();
        while(!canBeEvicted){
            if(usedQueue.element().isDirty()==null){
                //不是脏页，可以被取出
                canBeEvicted = true;
                evictPage = usedQueue.element();
            }
            else {
                //是脏页，不可被去除
                pageList.add(usedQueue.poll());
            }
            if(usedQueue.size()==0){
                //都是脏页,报错
                throw new DbException("Fail to evict any page!");
            }
        }
        synchronized (pageMap){
            pageMap.remove(evictPage.getId());
        }
        synchronized (usedQueue){
            usedQueue.remove(evictPage);//注意和pageMap主键的区别！！！
            for(int i=0;i<pageList.size();i++){
                usedQueue.add(pageList.get(i));
            }
            pageList.clear();
        }


        //自己循环遍历的（空间少，但时间复杂度高）
//        PageId minPage = null;
//        int minUsed = 999999;
//        for (PageId pageId : pageMap.keySet()) {
//            if (pageMap.get(pageId).getUsedTimes()<minUsed) s{
//                minUsed = pageMap.get(pageId).getUsedTimes();
//                minPage = pageId;
//            }
//        }
//        pageMap.remove(minPage);

    }

}
