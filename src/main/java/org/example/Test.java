package org.example;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by admin on 17/7/7.
 * 布隆过滤器
 * 做个对比， 1个用户，每天存储1000条文章ID，15天存储1.5万条， 5000篇文章，判断是否在文章中， 然后来1000个用户
 */
public class Test {
    private static int size = 1000;

    private static BloomFilter<Integer> bloomFilter = BloomFilter.create(Funnels.integerFunnel(), size, 0.001);

    // 每天1个bloom filter

    // 1000 条文章

    public static void main(String[] args) throws IOException {
        for (int i = 0; i < size; i++) {
            bloomFilter.put(i);
        }

        for (int i = 0; i < size; i++) {
            if (!bloomFilter.mightContain(i)) {
                System.out.println("有坏人逃脱了");
            }
        }
        System.out.println(bloomFilter.approximateElementCount());

        List<Integer> list = new ArrayList<Integer>(1000);
        for (int i = size + 10000; i < size + 20000; i++) {
            if (bloomFilter.mightContain(i)) {
                list.add(i);
            }
        }
        System.out.println("有误伤的数量：" + list.size());

        // 导出到当前文件中

        File f= new File("out.txt");
        OutputStream out = null;
        out = new FileOutputStream(f);
        bloomFilter.writeTo(out);

    }
}

