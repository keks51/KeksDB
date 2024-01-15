package utils;

import java.util.*;
import java.util.stream.Collectors;


public class ShowAsTable {

    public static void main(String[] args) {

//        List<List<String>> rows = new LinkedList<>() {{
//            add(Arrays.asList("aaaaa", "1", "0"));
//            add(Arrays.asList("bbbbb", "2", "00"));
//            add(Arrays.asList("ccccc", "3", "000"));
//        }};
//        List<String> cols = Arrays.asList("id", "age", "time");
//
//        System.out.println(show(rows, cols));

//        System.out.printf("%.2f", 10.0 / 3);

        System.out.println(String.format("%.2f", 10.0 / 3));

    }

    public static String show(List<List<String>> rowsX,
                       List<String> cols) {
        List<List<String>> rows = rowsX.stream().map(ArrayList::new).collect(Collectors.toList());
        Collections.copy(rows, rowsX);
        rows.add(0, cols);

        StringBuilder sb = new StringBuilder();
        int numCols = cols.size();
        int minimumColWidth = 3;

        int[] colWidths = new int[numCols];

        for (List<String> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                String colValue = row.get(i);
                colWidths[i] = Math.max(colWidths[i], Math.max(minimumColWidth, colValue.length()));
            }
        }

        for (List<String> row : rows) {
            for (int colIdx = 0; colIdx < row.size(); colIdx++) {
                String cell = row.get(colIdx);
                String element = leftPad(colWidths[colIdx], cell);
                row.set(colIdx, element);
            }
        }
        String sep = Arrays.stream(colWidths).boxed().map("-"::repeat).collect(Collectors.joining("+", "+", "+\n"));
        sb.append(sep);

        List<String> headRow = rows.get(0);
        String headStr = headRow.stream().collect(Collectors.joining("|", "|", "|\n"));
        sb.append(headStr);
        sb.append(sep);
        rows.remove(0);

        for (List<String> row : rows) {
            String rowStr = row.stream().collect(Collectors.joining("|", "|", "|\n"));
            sb.append(rowStr);
        }
        sb.append(sep, 0, sep.length() - 1);
        return sb.toString();
    }

    private static String leftPad(int size, String str) {
        if (str.length() == size) {
            return str;
        } else if (str.length() > size) {
            return str.substring(0, size);
        } else {
            return " ".repeat(size - str.length()) + str;
        }
    }

}
