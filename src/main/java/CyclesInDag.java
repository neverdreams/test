import java.util.ArrayList;
import java.util.List;

public class CyclesInDag {
    public List<List<List<Integer>>> findCyclesInDagV1(int[][] graph) {
        // BruteForce solution. Iterate every two nodes, DFS find all paths from start to end node, if result length > 1
        // meaning there's cycle between two nodes.
        if (graph == null || graph.length == 0) {
            return new ArrayList<>();
        }
        List<List<List<Integer>>> solution = new ArrayList<>();

        for (int i = 0; i < graph.length - 1; i++) {
            for (int j = i + 1; j < graph.length; j++) {
                List<List<Integer>> result = new ArrayList<>();
                List<Integer> curList = new ArrayList<>();
                curList.add(i);
                dfs(graph, i, j, curList, result);
                if (result.size() <= 1) {
                    // there's no two paths from i to j, so no cycle.
                    continue;
                }
                solution.add(result);
            }
        }

        return solution;
    }

    private void dfs(int[][] graph, int start, int end, List<Integer> curResult, List<List<Integer>> result) {
        if (start == end) {
            result.add(new ArrayList<>(curResult));
            return;
        }

        for (int nb : graph[start]) {
            curResult.add(nb);
            dfs(graph, nb, end, curResult, result);
            curResult.remove(curResult.size() - 1);
        }
    }

    public static void main(String[] args) {
        CyclesInDag cyclesInDag = new CyclesInDag();
        int[][] graph = new int[4][];
        graph[0] = new int[] {1,2};
        graph[1] = new int[] {3};
        graph[2] = new int[] {3};
        graph[3] = new int[] {};

        var solution = cyclesInDag.findCyclesInDagV1(graph);
        for (var s : solution) {
            System.out.printf("solution --> \n");
            for (var path : s) {
                System.out.println(path.toString());
            }
        }
    }

}
