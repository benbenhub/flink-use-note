package com.flinkuse.core.connector.cos;

import com.flinkuse.core.connector.cos.util.CosUtil;
import com.flinkuse.core.connector.cos.util.WriterUtil;
import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.*;
import com.flinkuse.core.constance.ConfigKeys;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author learn
 * @date 2023/2/9 22:13
 */
public class CosOutputFormat extends RichOutputFormat<Row> {

    private Configuration parameters;
    private transient COSClient cosClient;
    private transient StringWriter sw;
    private transient List<MyPartETag> myPartETags;
    private transient WriterUtil writerUtil;
    private static final Logger log = LoggerFactory.getLogger(CosOutputFormat.class);

    /** Must start at 1 and cannot be greater than 10,000 */
    private int currentPartNumber;

    private String currentUploadId;

    private final String cosObject;
    private String bucketName;
    private final char delimiter;

    /** 任务索引id */
    private int taskNumber;
    /** 子任务数量 */
    private int numTasks;

    private boolean willClose = false;

    public CosOutputFormat(String s3Object, char delimiter){
        this.cosObject = s3Object;
        this.delimiter = delimiter;
    }

    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {
        taskNumber = i;
        numTasks = i1;
        parameters = (Configuration) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        bucketName = parameters.get(ConfigKeys.cos_bucket_name);
        openSource();
        checkOutputDir();
        createActionFinishedTag();
        nextBlock();
    }

    @Override
    public void writeRecord(Row row) throws IOException {
        try {
            if (this.writerUtil == null) {
                nextBlock();
            }
            try {
                for (int i = 0; i < row.getArity(); ++i) {

                    String column = Objects.toString(row.getField(i));

                    if (column == null) {
                        continue;
                    }
                    writerUtil.write(column);
                }
                writerUtil.endRecord();
                flushDataInternal();
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        willClose = true;
        flushDataInternal();
        completeMultipartUploadFile();
        if (cosClient != null) {
            cosClient.shutdown();
            cosClient = null;
        }
    }

    private void openSource() {
        this.cosClient = CosUtil.getCosClient(parameters);
        this.myPartETags = new ArrayList<>();
        this.currentPartNumber = taskNumber - numTasks + 1;
    }

    private void checkOutputDir() {
        if (cosClient.doesObjectExist(bucketName, cosObject)) {
            if ("overwrite".equalsIgnoreCase(parameters.get(ConfigKeys.cos_write_mode))) {
                cosClient.deleteObject(bucketName, cosObject);
            }
        }
    }

    private void nextBlock() {
        sw = new StringWriter();
        this.writerUtil = new WriterUtil(sw, this.delimiter);
        this.currentPartNumber = this.currentPartNumber + numTasks;
    }

    /** Create file multipart upload ID */
    private void createActionFinishedTag() {
        if (!StringUtils.isNotBlank(currentUploadId)) {
            InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, cosObject);
            ObjectMetadata objectMetadata = new ObjectMetadata();
            request.setObjectMetadata(objectMetadata);
            InitiateMultipartUploadResult initResult = cosClient.initiateMultipartUpload(request);
            // 获取 uploadid
            this.currentUploadId = initResult.getUploadId();

        }
    }
    protected void flushDataInternal() {
        StringBuffer sb = sw.getBuffer();
        long MIN_SIZE = 1024 * 1024 * 25L;
        if (sb.length() > MIN_SIZE || willClose) {
            byte[] byteArray;

            byteArray = sb.toString().getBytes(StandardCharsets.UTF_8);

            InputStream inputStream = new ByteArrayInputStream(byteArray);

            UploadPartRequest uploadRequest =
                    new UploadPartRequest()
                            .withBucketName(bucketName)
                            .withKey(cosObject)
                            .withUploadId(this.currentUploadId)
                            .withPartNumber(this.currentPartNumber)
                            .withInputStream(inputStream)
                            .withPartSize(byteArray.length);
            UploadPartResult uploadResult = cosClient.uploadPart(uploadRequest);

            PartETag partETag = uploadResult.getPartETag();

            MyPartETag myPartETag = new MyPartETag(partETag);
            myPartETags.add(myPartETag);
            log.info(
                    "task-{} upload etag:[{}]",
                    taskNumber,
                    myPartETags.stream().map(Objects::toString).collect(Collectors.joining(",")));
            if (writerUtil != null) {
                writerUtil.close();
                writerUtil = null;
            }
        }
    }

    private void completeMultipartUploadFile() {
        if (this.currentPartNumber > 10000) {
            throw new IllegalArgumentException("part can not bigger than 10000");
        }
        List<PartETag> partETags =
                myPartETags.stream().map(MyPartETag::genPartETag).collect(Collectors.toList());
        if (partETags.size() > 0) {
            log.info(
                    "Start merging files partETags:{}",
                    partETags.stream().map(PartETag::getETag).collect(Collectors.joining(",")));

            CompleteMultipartUploadRequest compRequest =
                    new CompleteMultipartUploadRequest(bucketName, cosObject, this.currentUploadId, partETags);
            cosClient.completeMultipartUpload(compRequest);
        } else {

            cosClient.abortMultipartUpload(
                    new AbortMultipartUploadRequest(bucketName, cosObject, this.currentUploadId));
            cosClient.putObject(bucketName, cosObject, "");
        }
    }
}
