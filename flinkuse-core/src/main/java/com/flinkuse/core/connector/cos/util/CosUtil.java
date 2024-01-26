package com.flinkuse.core.connector.cos.util;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.http.HttpProtocol;
import com.qcloud.cos.model.*;
import com.qcloud.cos.region.Region;
import com.flinkuse.core.constance.ConfigKeys;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CosUtil {

    public static COSClient getCosClient(Configuration parameters) {
        COSCredentials cred = new BasicCOSCredentials(parameters.get(ConfigKeys.cos_secret_id), parameters.get(ConfigKeys.cos_secret_key));

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRegion(new Region(parameters.get(ConfigKeys.cos_region)));

        clientConfig.setHttpProtocol(HttpProtocol.https);

        // 以下的设置，是可选的：

        // 设置 socket 读取超时，默认 30s
        clientConfig.setSocketTimeout(parameters.get(ConfigKeys.cos_socket_timeout));
        // 设置建立连接超时，默认 30s
        clientConfig.setConnectionTimeout(parameters.get(ConfigKeys.cos_connection_timeout));

        // 如果需要的话，设置 http 代理，ip 以及 port
//        clientConfig.setHttpProxyIp("httpProxyIp");
//        clientConfig.setHttpProxyPort(80);

        // 生成 cos 客户端。
        return new COSClient(cred, clientConfig);
    }

    public static PutObjectResult putStringObject(
            COSClient s3Client, String bucketName, String key, String content) {
        return s3Client.putObject(bucketName, key, content);
    }

    public static List<String> listObjectsKeyByPrefix(
            COSClient cosClient, String bucketName, String prefix, int fetchSize) {
        List<String> objects = new ArrayList<>(fetchSize);
        ListObjectsRequest req =
                new ListObjectsRequest().withBucketName(bucketName).withMaxKeys(fetchSize);
        if (StringUtils.isNotBlank(prefix)) {
            req.setPrefix(prefix);
        }
        ObjectListing objectListing;
        do {
            objectListing = cosClient.listObjects(req);

            for (COSObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                objects.add(objectSummary.getKey());
            }

            String token = objectListing.getNextMarker();
            req.setMarker(token);

        } while (objectListing.isTruncated());
        return objects;
    }

    public static List<String> listObjectsByv1(
            COSClient s3Client, String bucketName, String prefix, int fetchSize) {
        List<String> objects = new ArrayList<>(fetchSize);

        ListObjectsRequest req = new ListObjectsRequest(bucketName, prefix, null, null, fetchSize);
        ObjectListing ol;
        do {
            ol = s3Client.listObjects(req);

            for (COSObjectSummary  os : ol.getObjectSummaries()) {
                objects.add(os.getKey());
            }

            if (ol.isTruncated()) {
                // next page
                String marker = ol.getNextMarker();
                if (StringUtils.isNotBlank(marker)) {
                    req.setMarker(marker);
                } else {
                    log.warn("Warning: missing NextMarker when IsTruncated");
                }
            }
        } while (ol.isTruncated());
        return objects;
    }

    public static boolean doesObjectExist(COSClient s3Client, String bucketName, String object) {
        return s3Client.doesObjectExist(bucketName, object);
    }

    /**
     * get S3SimpleObject{@link CosSimpleObject} from AWS S3
     *
     * @param object
     * @return
     */
    public static CosSimpleObject getCosSimpleObject(String object) {
        return new CosSimpleObject(object);
    }

    public static void deleteObject(COSClient s3Client, String bucketName, String object) {
        s3Client.deleteObject(bucketName, object);
    }

    public static void closeS3(COSClient amazonS3) {
        if (amazonS3 != null) {
            amazonS3.shutdown();
            amazonS3 = null;
        }
    }

    public static String initiateMultipartUploadAndGetId(
            COSClient s3Client, String bucketName, String object) {
        InitiateMultipartUploadRequest initRequest =
                new InitiateMultipartUploadRequest(bucketName, object);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);
        return initResponse.getUploadId();
    }

    public static PartETag uploadPart(
            COSClient s3Client,
            String bucketName,
            String object,
            String uploadId,
            int partNumber,
            byte[] data) {
        InputStream inputStream = new ByteArrayInputStream(data);

        UploadPartRequest uploadRequest =
                new UploadPartRequest()
                        .withBucketName(bucketName)
                        .withKey(object)
                        .withUploadId(uploadId)
                        .withPartNumber(partNumber)
                        .withInputStream(inputStream)
                        .withPartSize(data.length);
        UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
        return uploadResult.getPartETag();
    }

    public static void completeMultipartUpload(
            COSClient s3Client,
            String bucketName,
            String object,
            String uploadId,
            List<PartETag> partETags) {
        CompleteMultipartUploadRequest compRequest =
                new CompleteMultipartUploadRequest(bucketName, object, uploadId, partETags);
        s3Client.completeMultipartUpload(compRequest);
    }

    public static void abortMultipartUpload(
            COSClient s3Client, String bucketName, String object, String uploadId) {
        s3Client.abortMultipartUpload(
                new AbortMultipartUploadRequest(bucketName, object, uploadId));
    }

    public static long getFileSize(COSClient s3Client, String bucketName, String keyName) {
        GetObjectRequest getObjectRequest = new GetObjectRequest(bucketName, keyName);
        return s3Client.getObject(getObjectRequest).getObjectMetadata().getInstanceLength();
    }
}
