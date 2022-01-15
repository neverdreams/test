import jdk.jshell.spi.ExecutionControl;

import java.time.LocalDateTime;
import java.util.*;

public class KeyValueStoreMvcc {
    public enum IsolationLevel {
        DIRTY_READ,
        READ_COMMITTED,
        REAPEATABLE_READS,
        SERIALIZABLE
    }

    private IsolationLevel isolationLevel;

    // example of data:
    // key: abc, value: [{v1: 123}, {v2: 456}]
    private HashMap<String, TreeMap<Integer, String>> committedMap;
    private HashMap<String, TreeMap<Integer, String>> uncommittedMap;
    private int clientVersion;
    private Object lockObj = new Object();

    public KeyValueStoreMvcc(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
        this.committedMap = new HashMap<>();
        this.uncommittedMap = new HashMap<>();
        this.clientVersion = 0;
    }

    public String read(String key, int clientVersion) throws Exception {
        switch (isolationLevel) {
            case DIRTY_READ:
                // for DIRTY READ, we can read uncommitted changes.
                if (uncommittedMap.containsKey(key)) {
                    var treeMap = uncommittedMap.get(key);
                    // get the value by latest version.
                    return treeMap.lastEntry().getValue();
                }

                if (committedMap.containsKey(key)) {
                    var treeMap = committedMap.get(key);
                    return treeMap.lastEntry().getValue();
                }
                return null;
            case READ_COMMITTED:
                // for READ_COMMITTED, we can only read from committed memory.
                if (committedMap.containsKey(key)) {
                    var treeMap = committedMap.get(key);
                    return treeMap.lastEntry().getValue();
                }
                return null;
            case REAPEATABLE_READS:

                // for REAPEATABLE_READS, we need to check versions of the data and return the most close (<=) to client version one in committed memory.
                if (committedMap.containsKey(key)) {
                    var treeMap = committedMap.get(key);
                    if (treeMap.containsKey(clientVersion)) {
                        return treeMap.get(clientVersion);
                    }

                    return treeMap.lowerEntry(clientVersion) == null ? null : treeMap.lowerEntry(clientVersion).getValue();
                }
                return null;
            case SERIALIZABLE:
                throw new Exception("not implemented.");
        }

        return null;
    }

    public void write(String key, String value, int clientVersion) {
        if (!uncommittedMap.containsKey(key)) {
            uncommittedMap.put(key, new TreeMap<>());
        }

        uncommittedMap.get(key).put(clientVersion, value);
    }

    public boolean commit(List<String> keys, int clientVersion) throws Exception {
        // we need to move all the keys in the uncommitted memory to committed memory with latest clientVersion.
        for (String key : keys) {
            if (!uncommittedMap.containsKey(key)) {
                throw new Exception("uncommittedMap lost key " + key);
            }

            var uncommittedTreeMap = uncommittedMap.get(key);
            if (!uncommittedTreeMap.containsKey(clientVersion)) {
                throw new Exception("uncommittedTreeMap lost client version  " + clientVersion);
            }

            if (!committedMap.containsKey(key)) {
                committedMap.put(key, new TreeMap<>());
            }

            if (this.isolationLevel == IsolationLevel.SERIALIZABLE) {
                if (committedMap.get(key).size() > 0 && committedMap.get(key).lastKey() > clientVersion) {
                    // if we found client version is less than last committed version, it means during current transaction processing,
                    // there're other transactions committed the same key. In SERIALIZABLE mode we should abort our cur transaction.
                    System.out.printf("[WARN] In committedMap, lastKey commitId = %s, clientVersion = %s, abort the transaction.\n", committedMap.get(key).lastKey(), clientVersion);
                    return false;
                }
            }

            int commitVersion =  distributeClientVersion();
            committedMap.get(key).put(commitVersion, uncommittedTreeMap.get(clientVersion));
        }

        return true;
        // after commit, client should always request a new client version.
    }

    public int distributeClientVersion() {
        synchronized (lockObj) {
            clientVersion++;
            return clientVersion;
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // test DIRTY_READ
        testDirtyRead();

        // test READ_COMMITTED
        //testReadCommitted();

        // test REAPEATABLE_READ
        //testReapeatableRead();

        // test SERIALIZABLE
        //testSerializable();
    }

    static void testDirtyRead() throws InterruptedException {
        final KeyValueStoreMvcc store1 = new KeyValueStoreMvcc(IsolationLevel.DIRTY_READ);

        // thread1 keep reading key abc every 1 sec.
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                int clientVersion = store1.distributeClientVersion();
                while (true) {
                    try {
                        System.out.printf("Thread %s, ClientVersion = %s, key = %s, value = %s\n", Thread.currentThread().getId(),clientVersion, "abc", store1.read("abc", clientVersion) == null ? "null":store1.read("abc", clientVersion));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        thread1.start();

        // thread2 update abc to a different value, sleep 5 sec and commit.
        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                List<String> upsertKeys = Arrays.asList("abc");

                while (true) {
                    int clientVersion = store1.distributeClientVersion();
                    String newVal = LocalDateTime.now().toString();
                    store1.write("abc", newVal, clientVersion);
                    System.out.printf("Thread %s, ClientVersion = %s, key = %s, write val = %s but not committed yet.\n", Thread.currentThread().getId(), clientVersion, "abc", newVal);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        store1.commit(upsertKeys, clientVersion);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        thread2.start();

        thread1.join();
        thread2.join();
    }

    static void testReadCommitted() throws InterruptedException {
        final KeyValueStoreMvcc store1 = new KeyValueStoreMvcc(IsolationLevel.READ_COMMITTED);

        // thread1 keep reading key abc every 1 sec.
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                int clientVersion = store1.distributeClientVersion();
                while (true) {
                    try {
                        System.out.printf("Thread %s, ClientVersion = %s, key = %s, value = %s\n", Thread.currentThread().getId(),clientVersion, "abc", store1.read("abc", clientVersion) == null ? "null":store1.read("abc", clientVersion));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        thread1.start();

        // thread2 update abc to a different value, sleep 5 sec and commit.
        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                List<String> upsertKeys = Arrays.asList("abc");

                while (true) {
                    int clientVersion = store1.distributeClientVersion();
                    String newVal = LocalDateTime.now().toString();
                    store1.write("abc", newVal, clientVersion);
                    System.out.printf("Thread %s, ClientVersion = %s, key = %s, write val = %s but not committed yet.\n", Thread.currentThread().getId(), clientVersion, "abc", newVal);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        store1.commit(upsertKeys, clientVersion);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        thread2.start();

        thread1.join();
        thread2.join();
    }

    static void testReapeatableRead() throws InterruptedException {
        final KeyValueStoreMvcc store1 = new KeyValueStoreMvcc(IsolationLevel.REAPEATABLE_READS);

        // thread1 keep reading key abc every 1 sec.
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    // let thread 2 write the first value.
                    Thread.sleep(6000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                int clientVersion = store1.distributeClientVersion();
                while (true) {
                    try {
                        System.out.printf("Thread %s, ClientVersion = %s, key = %s, value = %s\n", Thread.currentThread().getId(),clientVersion, "abc", store1.read("abc", clientVersion) == null ? "null":store1.read("abc", clientVersion));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        thread1.start();

        // thread2 update abc to a different value, sleep 5 sec and commit.
        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                List<String> upsertKeys = Arrays.asList("abc");

                while (true) {
                    int clientVersion = store1.distributeClientVersion();
                    String newVal = LocalDateTime.now().toString();
                    store1.write("abc", newVal, clientVersion);
                    System.out.printf("Thread %s, ClientVersion = %s, key = %s, write val = %s but not committed yet.\n", Thread.currentThread().getId(), clientVersion, "abc", newVal);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        store1.commit(upsertKeys, clientVersion);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        thread2.start();

        thread1.join();
        thread2.join();
    }

    static void testSerializable() throws InterruptedException {
        final KeyValueStoreMvcc store1 = new KeyValueStoreMvcc(IsolationLevel.SERIALIZABLE);

        // thread1 commit update every 4 seconds
        Thread thread1 = new Thread(new Runnable() {
            @Override
            public void run() {
                List<String> upsertKeys = Arrays.asList("abc");

                while (true) {
                    int clientVersion = store1.distributeClientVersion();
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    String newVal = LocalDateTime.now().toString();
                    store1.write("abc", newVal, clientVersion);
                    System.out.printf("Thread %s, ClientVersion = %s, key = %s, write val = %s but not committed yet.\n", Thread.currentThread().getId(), clientVersion, "abc", newVal);
                    try {
                        Thread.sleep(4000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        store1.commit(upsertKeys, clientVersion);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        thread1.start();

        // thread2 update abc to a different value, sleep 5 sec and commit.
        Thread thread2 = new Thread(new Runnable() {
            @Override
            public void run() {
                List<String> upsertKeys = Arrays.asList("abc");

                while (true) {
                    int clientVersion = store1.distributeClientVersion();
                    String newVal = LocalDateTime.now().toString();
                    store1.write("abc", newVal, clientVersion);
                    System.out.printf("Thread %s, ClientVersion = %s, key = %s, write val = %s but not committed yet.\n", Thread.currentThread().getId(), clientVersion, "abc", newVal);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        store1.commit(upsertKeys, clientVersion);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
        thread2.start();

        thread1.join();
        thread2.join();
    }
}

