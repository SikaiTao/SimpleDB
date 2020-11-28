package simpledb;


// page-level locks catalog

import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

public class LocksCatalog {
    private ConcurrentHashMap<PageId, Vector<PageLock>> locksOnPage;

    public LocksCatalog(){
        locksOnPage = new ConcurrentHashMap<>();
    }

    public synchronized boolean lock(PageId pid, TransactionId tid, Permissions perm) {
        Vector<PageLock> ls = locksOnPage.get(pid);

        if (ls == null) {
            PageLock l = new PageLock(tid, perm);
            Vector<PageLock> locks = new Vector<>();
            locks.add(l);
            locksOnPage.put(pid, locks);
            return true;
        }

        for(PageLock l : ls){
            if (l.tid == tid){
                if (l.perm == perm) return true;

                if (l.perm == Permissions.READ_ONLY && perm == Permissions.READ_WRITE){
                    if (ls.size() != 1) return false;
                    l.perm = Permissions.READ_WRITE;
                    return true;
                }

                if (l.perm == Permissions.READ_WRITE && perm == Permissions.READ_ONLY){
                    return true;
                }
            }

        }

        if (ls.get(0).perm == Permissions.READ_WRITE || perm == Permissions.READ_WRITE) return false;

        PageLock pl = new PageLock(tid, perm);
        ls.add(pl);
        locksOnPage.put(pid, ls);
        return true;
    }


    public synchronized void unlock(PageId pid, TransactionId tid){
        Vector<PageLock> ls = locksOnPage.get(pid);

        for (int i = 0; i < ls.size(); i++){
            if (ls.get(i).tid == tid){
                ls.remove(i);
                if (ls.size() == 0) locksOnPage.remove(pid);
                return;
            }
        }
    }


    public synchronized boolean isLocked(PageId pid, TransactionId tid){
        Vector<PageLock> ls = locksOnPage.get(pid);
        if (ls == null) return false;

        for (PageLock pl : ls){
            if (pl.tid == tid) return true;
        }

        return false;
    }


    private class PageLock{
        TransactionId tid;
        Permissions perm;

        PageLock(TransactionId tid, Permissions perm){
            this.tid = tid;
            this.perm = perm;
        }
    }
}
