import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;

public class Dijkstra {
    /**
     * Find shortest path in a weighted graph. Dijkstra algorithm.
     * @param nodes all nodes in the graph.
     * @param weights weight between each two nodes. if there's no connection between two nodes, the weight should be infinite.
     * @param start start node.
     * @param end end node.
     * @return shortest path if found, else return -1.
     */
    public int findShortestPath(Node[] nodes, int[][] weights, int start, int end) {
        if (nodes == null || nodes.length == 0 || weights == null || weights.length == 0 || start < 0 || start >= nodes.length || end < 0 || end >= nodes.length) {
            return -1;
        }

        PriorityQueue<Distance> heap = new PriorityQueue(new Comparator<Distance>() {
            @Override
            public int compare(Distance o1, Distance o2) {
                return o1.distance - o2.distance;
            }
        });
        heap.add(new Distance(start, 0));

        HashSet<Integer> visited = new HashSet<>();
        while (!heap.isEmpty()) {
            Distance curDistance = heap.poll();
            int curNodeIndex = curDistance.nodeIdx;
            int curWeight = curDistance.distance;

            if (curNodeIndex == end) {
                return curWeight;
            }

            // update all neighbors.
            for (int j = 0; j < weights[curNodeIndex].length; j++) {
                if (visited.contains(j)) {
                    continue;
                }

                if (j == curNodeIndex || weights[curNodeIndex][j] == Integer.MAX_VALUE) {
                    continue;
                }

                int curNbWeight = curWeight + weights[curNodeIndex][j];
                heap.add(new Distance(j, curNbWeight));
            }
            visited.add(curNodeIndex);
        }

        return -1;
    }


    private class Distance {
        public int nodeIdx;
        public int distance;
        public Distance(int nodeIdx, int distance) {
            this.nodeIdx = nodeIdx;
            this.distance = distance;
        }
    }

    public static void main(String[] args) {
        Dijkstra dijkstra = new Dijkstra();
        Node[] nodes = new Node[3];
        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = new Node(i);
        }
        int[][] weights = new int[nodes.length][nodes.length];

        for (int i = 0; i < weights.length; i++) {
            Arrays.fill(weights[i], Integer.MAX_VALUE);
        }

        weights[0][1] = 1;
        weights[0][2] = 3;
        weights[1][2] = 1;

        int shortestPath = dijkstra.findShortestPath(nodes, weights, 0, 2);
        System.out.printf("shortest path between node %s and node %s is %s", 0, 2, shortestPath);
    }
}
