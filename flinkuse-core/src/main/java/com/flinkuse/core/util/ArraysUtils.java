package com.flinkuse.core.util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;
import java.util.stream.Collectors;


/**
 * @author 94391
 */
public class ArraysUtils {

    /**
     * 判断交集
     * 如果都是空则为false
     * @param array1
     * @param array2
     * @return
     */
    public static boolean hasIntersectionByIndexOf(String[] array1, String[] array2) {
        if (Objects.isNull(array1) && Objects.isNull(array2)){
            return false;
        }
        String map = Arrays.toString(array2);
        for (String str : array1) {
            if (map.indexOf(str) > -1) {
                return true;
            }
        }
        return false;
    }


    /**
     * 去重
     * @param a
     * @param b
     * @return
     */
    public static String[] deWeight(String[] a, String[] b) {
        int subscript = 0;
        Map<String, String> map = new TreeMap<>();
        for (int i = 0; i < a.length; i++) {
            map.put(a[i], a[i]);
        }
        for (int i = 0; i < b.length; i++) {
            map.put(b[i], b[i]);
        }
        Collection<String> values = map.values();
        Iterator<String> iterator = values.iterator();
        String res[] = new String[values.size()];
        while (iterator.hasNext()) {
            res[subscript++] = iterator.next();
        }
        return res;
    }

    /**
     * 合并数组
     * @param <T> 泛型，用于合并不同类型数组
     * @param first 同下
     * @param rest 传入的数组，可以传递多个
     * */

    public static <T> T[] concatAll(T[] first, T[]... rest) {

        int totalLength = first.length;
        for (T[] array : rest) {
            totalLength += array.length;
        }

        T[] result = Arrays.copyOf(first, totalLength);
        int offset = first.length;
        for (T[] array : rest) {
            System.arraycopy(array, 0, result, offset, array.length);
            offset += array.length;
        }
        return result;
    }

    public static Tuple2<String, Map<Integer, List<Long>>> paramsSqlArray(Map<Long, Integer> map, int parallelism) {
        // 根据数量排序ID
        List<Map.Entry<Long, Integer>> sortedEntries = new ArrayList<>(map.entrySet());
        sortedEntries.sort(Map.Entry.comparingByValue());

        // 分配ID到不同的组
        List<List<Long>> groups = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            groups.add(new ArrayList<>());
        }

        int totalSum = sortedEntries.stream().mapToInt(Map.Entry::getValue).sum();
        int[] groupSums = new int[parallelism];
        int halfSum = totalSum / parallelism;

        // 分治策略，尽量平衡两个组的数量之和
        for (int i = 0; i < sortedEntries.size();) {
            for (int gsi = 0; gsi < groupSums.length; gsi++) {
                if (groupSums[gsi] <= halfSum) {
                    groups.get(gsi).add(sortedEntries.get(i).getKey());
                    groupSums[gsi] += sortedEntries.get(i++).getValue();
                    break;
                }
            }
        }
        long recParallelism = groups.stream().filter(f -> f.size() > 0).count();
        groups = groups.stream().peek(m -> {if (m.size() == 0) m.add(0L);}).collect(Collectors.toList());
        if (recParallelism < parallelism)
            System.out.println("设置" + parallelism + "分区过大建议分区数" + recParallelism);
        Map<Integer, List<Long>> cv = new HashMap<>();
        StringBuilder jsb = new StringBuilder();
        int max = 0;
        for (int dd = 0; dd < groups.size(); dd++) {
            if (max < groups.get(dd).size()) max = groups.get(dd).size();
            cv.put(dd, groups.get(dd));
        }
        if (max > 0) jsb.append("?,".repeat(max));
        if (jsb.length() > 1) jsb.delete(jsb.length() - 1, jsb.length());

        for (Map.Entry<Integer, List<Long>> e : cv.entrySet()) {
            if (e.getValue().size() < max) {
                int cc = max - e.getValue().size();
                for (int ii = 0; ii < cc; ii++) {
                    e.getValue().add(0L);
                }
            }
        }
        return Tuple2.of(jsb.toString(), cv);
    }
}
