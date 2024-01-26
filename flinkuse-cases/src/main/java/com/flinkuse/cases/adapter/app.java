package com.flinkuse.cases.adapter;

import com.flinkuse.cases.adapter.trans.CqTransformImpl;

/**
 * @author learn
 * @date 2023/5/16 15:15
 */
public class app {

    public static void main(String[] args) throws Exception {
        new CqTransformImpl(args,"category query sync").start();
    }
}

