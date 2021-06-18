import com.bingocloud.ClientConfiguration;
import com.bingocloud.Protocol;
import com.bingocloud.auth.BasicAWSCredentials;
import com.bingocloud.services.s3.AmazonS3Client;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;

public class Writer {
    private final static String bucketName = "zhu";
    private final static String folderPath = "D:\\download\\";
    private final static String accessKey = "FEA16476EE52A96FD9B6";
    private final static String secretKey = "WzFENDNBM0U1RjZGNTAwQzQ1N0VGQURBM0UzOEZC";
    private final static String serviceEndpoint = "http://scut.depts.bingosoft.net:29997";

    public Writer() {
        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTP);
        AmazonS3Client s3 = new AmazonS3Client(credentials, clientConfig);
        s3.setEndpoint(serviceEndpoint);
        ArrayList<String> pathList=getFiles(folderPath,new ArrayList<>());
        for(String filePath:pathList){
            System.out.format("Uploading %s to S3 bucket %s...\n", filePath, bucketName);
            final String keyName = Paths.get(filePath).getFileName().toString();
            final File file = new File(filePath);

            for (int i = 0; i < 2; i++) {
                s3.putObject(bucketName, keyName, file);
            }
        }
        System.out.println("Done!");
    }
    public static ArrayList<String> getFiles(String folderPath,ArrayList<String> pList){
        File file = new File(folderPath);
        File[] fileList = file.listFiles();
        if(fileList==null) {
            return pList;
        }

        for (int i = 0; i < fileList.length; i++) {
            if(fileList[i].isDirectory()) {
                getFiles(fileList[i].getAbsolutePath(),pList);
            }
            if(fileList[i].isFile()) {//判断是否为文件
                pList.add(fileList[i].getAbsolutePath());
            }
        }
        return pList;
    }

}