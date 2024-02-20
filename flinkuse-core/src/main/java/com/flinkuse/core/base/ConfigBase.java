package com.flinkuse.core.base;

import org.apache.flink.configuration.Configuration;

import java.io.Serializable;

/**
 * @author learn
 * @date 2023/1/29 11:12
 */
public abstract class ConfigBase implements Serializable {

    protected final Configuration scpsConfig;

    protected ConfigBase(Configuration scpsConfig){
        this.scpsConfig = scpsConfig;
    }
}
