package com.flinkuse.core.connector.cos.util;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class CosSimpleObject implements Serializable {

    private static final long serialVersionUID = -7199607264925678753L;

    private String key;
}
