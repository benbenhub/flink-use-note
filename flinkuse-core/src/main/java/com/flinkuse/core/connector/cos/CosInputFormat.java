package com.flinkuse.core.connector.cos;

import com.flinkuse.core.connector.cos.util.CosUtil;
import com.flinkuse.core.connector.cos.util.ReaderUtil;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.COSObjectInputStream;
import com.qcloud.cos.model.GetObjectRequest;
import com.flinkuse.core.connector.cos.util.CosSimpleObject;
import com.flinkuse.core.constance.ConfigKeys;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * @author learn
 * @date 2024/1/24 9:26
 */
public class CosInputFormat extends RichInputFormat<String, InputSplit> {

    private Configuration parameters;
    private final String[] cosObjects;
    private String bucketName;
    private Iterator<String> splits;

    private transient COSClient cosClient;
    private transient ReaderUtil readerUtil = null;

    public CosInputFormat(String... cosObjects) {
        this.cosObjects = cosObjects;
    }

    @Override
    public void configure(Configuration parameter) {
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        /*
        List<CosSimpleObject> objects = resolveObjects();
        List<CosSimpleObject> objects = resolveObjects();
        if (objects.isEmpty()) {
            throw new RuntimeException();
        }
        // log.info("read file {}", GsonUtil.GSON.toJson(objects));
        List<String> keys = new ArrayList<>();
        for (CosSimpleObject object : objects) {
            keys.add(object.getKey());
        }
         */
        CosInputSplit[] splits = new CosInputSplit[minNumSplits];
        for (int i = 0; i < minNumSplits; i++) {
            splits[i] = new CosInputSplit(i, minNumSplits, Arrays.asList(cosObjects));
        }
        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return reachedEndWithoutCheckState();
    }

    @Override
    public void open(InputSplit split) throws IOException {
        cosClient = CosUtil.getCosClient(parameters);
        CosInputSplit inputSplit = (CosInputSplit) split;
        List<String> splitsList = inputSplit.getSplits();
        LinkedList<String> result = new LinkedList<>();

        for (int i = 0; i < splitsList.size(); i++) {
            String object = splitsList.get(i);
            if (i % inputSplit.getTotalNumberOfSplits() == inputSplit.getSplitNumber()) {
                result.add(object);
            }
        }

        splits = result.iterator();
    }

    @Override
    public String nextRecord(String reuse) throws IOException {
        reuse = readerUtil.getRawRecord();
        return reuse;
    }

    @Override
    public void close() throws IOException {
        if (cosClient != null) {
            cosClient.shutdown();
            cosClient = null;
        }
        if (readerUtil != null) {
            readerUtil.close();
            readerUtil = null;
        }
    }

    @Override
    public void openInputFormat() throws IOException {
        // Map<String, String> vars = getRuntimeContext().getMetricGroup().getAllVariables();

        parameters = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        bucketName = parameters.get(ConfigKeys.cos_bucket_name);
    }

    @Override
    public void closeInputFormat() {
        // log.info("subtask input close finished");
    }

    public List<CosSimpleObject> resolveObjects() {
        Set<CosSimpleObject> resolved = new HashSet<>();
        COSClient cosClient = CosUtil.getCosClient(parameters);
        for (String key : cosObjects) {
            if (StringUtils.isNotBlank(key)) {
                if (key.endsWith(".*")) {
                    // End with .*, indicating that the object is prefixed
                    String prefix = key.substring(0, key.indexOf(".*"));
                    int maxKeys = 1000;
                    List<String> subObjects =
                                CosUtil.listObjectsKeyByPrefix(
                                        cosClient, bucketName, prefix, maxKeys);

                    for (String subObject : subObjects) {
                        CosSimpleObject s3SimpleObject = CosUtil.getCosSimpleObject(subObject);
                        resolved.add(s3SimpleObject);
                    }
                } else if (CosUtil.doesObjectExist(cosClient, bucketName, key)) {
                    // Exact query and object exists
                    CosSimpleObject s3SimpleObject = CosUtil.getCosSimpleObject(key);
                    resolved.add(s3SimpleObject);
                }
            }
        }

        return new ArrayList<>(resolved);
    }

    public boolean reachedEndWithoutCheckState() throws IOException {
        // br is empty, indicating that a new file needs to be read
        if (readerUtil == null) {
            if (splits.hasNext()) {
                // If there is a new file, read the new file
                String currentObject = splits.next();
                GetObjectRequest rangeObjectRequest =
                        new GetObjectRequest(bucketName, currentObject);
                //log.info("Current read file {}", currentObject);

                // The resumable upload is not enabled or the resumable upload is enabled but
                // the file has not been read
                COSObject o = cosClient.getObject(rangeObjectRequest);
                COSObjectInputStream s3is = o.getObjectContent();
                readerUtil =
                        new ReaderUtil(
                                new InputStreamReader(s3is, StandardCharsets.UTF_8),
                                ',',
                                0L,
                                false);
                /*
                boolean isFirstLineHeader = false;
                if (isFirstLineHeader) {
                    readerUtil.readHeaders();
                }
                 */
            } else {
                // All files have been read
                return true;
            }
        }
        if (readerUtil.readRecord()) {
            // The file has not been read
            return false;
        } else {
            // After reading the file read this time, close br and clear it
            readerUtil.close();
            readerUtil = null;
            // try to read the new file
            return reachedEndWithoutCheckState();
        }
    }
}
