import java.util.*;

/**
 * Find the shortest path from start node to end node in a graph. The work can be distributed to multiple workers.
 */
public class ParallelShortestPath {
    // version 1: one worker version. (no parallel)
    public int findShortestPathV1(Node start, Node end) {
        if (start == null || end == null) {
            return -1;
        }

        Queue<Node> queue = new LinkedList<>();
        HashSet<Node> visited = new HashSet<>();
        int shortestPath = 0;
        queue.add(start);
        visited.add(start);

        while (!queue.isEmpty()) {
            int qSize = queue.size();
            for (int i = 0; i < qSize; i++) {
                Node cur = queue.poll();
                if (cur == end) {
                    return shortestPath;
                }
                for (Node nb : cur.neighbors) {
                    if (visited.contains(nb)) {
                        continue;
                    }

                    queue.add(nb);
                    visited.add(nb);
                }
            }
            shortestPath++;
        }

        // cannot reach to end node.
        return -1;
    }
    // end of version 1

    private Queue<Node> queue = new LinkedList<>();
    private HashSet<Node> visited = new HashSet<>();
    private int shortestPath = 0;
    private boolean foundShortestPath = false;
    private int curLevelExecutedNodes = 0;
    private int curLevelMaxNodes = 0;
    private Object lockObj = new Object();

    public int findShortestPathV2(Node start, Node end, int nWorkers) throws InterruptedException {
        if (start == null || end == null) {
            return -1;
        }

        queue.add(start);
        visited.add(start);

        while (!queue.isEmpty()) {
            curLevelMaxNodes = queue.size();

            List<Worker> workers = new ArrayList<>();
            for (int i = 0; i < nWorkers; i++) {
                workers.add(new Worker(end));
                workers.get(i).start();
            }

            for (Worker worker : workers) {
                worker.join();
            }

            if (foundShortestPath) {
                return shortestPath;
            }
            shortestPath++;
            curLevelMaxNodes = 0;
            curLevelExecutedNodes = 0;
        }

        return -1;
    }

    // multiple workers can
    public class Worker extends Thread {
        private Node end;
        public Worker(Node end) {
            this.end = end;
        }

        public void run() {
            while (!queue.isEmpty()) {
                Node cur = null;
                synchronized (lockObj) {
                    if (curLevelExecutedNodes == curLevelMaxNodes) {
                        // reached current level max nodes.
                        break;
                    }
                    curLevelExecutedNodes++;
                    cur = queue.poll();
                }

                if (cur == null) {
                    break;
                }

                System.out.printf("Worker %s is processing Node %s\n", Thread.currentThread().getId(), cur.val);

                if (cur == end) {
                    foundShortestPath = true;
                    return;
                }

                synchronized (lockObj) {
                    for (Node nb : cur.neighbors) {
                        if (visited.contains(nb)) {
                            continue;
                        }

                        queue.add(nb);
                        visited.add(nb);
                    }
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        // test version 1
        Node[] nodes = new Node[5];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new Node(i);
        }

        nodes[0].neighbors.add(nodes[1]);
        nodes[0].neighbors.add(nodes[2]);

        nodes[1].neighbors.add(nodes[0]);
        nodes[1].neighbors.add(nodes[4]);

        nodes[2].neighbors.add(nodes[0]);
        nodes[2].neighbors.add(nodes[3]);

        nodes[3].neighbors.add(nodes[2]);
        nodes[3].neighbors.add(nodes[4]);

        nodes[4].neighbors.add(nodes[1]);
        nodes[4].neighbors.add(nodes[3]);

        ParallelShortestPath solution = new ParallelShortestPath();
        //int shortest = solution.findShortestPathV1(nodes[0], nodes[4]);
        int shortest = solution.findShortestPathV2(nodes[0], nodes[4], 2);
        System.out.printf("shortest path between node %s and node %s is %s\n", nodes[0].val, nodes[4].val, shortest);
    }
}

