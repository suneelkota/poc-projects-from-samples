package org.murygin.archive.dao;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.murygin.archive.service.DocumentMetadata;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AmazonS3InquiryDao {
	private static String bucketName = "firsts3bucket-skota-aws";
	
    public static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd";
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat(DATE_FORMAT_PATTERN);

	
	public List<DocumentMetadata> getFilesList() throws IOException {
		AWSCredentials credentials = new BasicAWSCredentials(
				"AKIAIZ4M5AHN3ZTJLUEQ", 
				"6borFhgqMFYQCZ9Lm45kZjZYfzfvx3WyL2PWEVmf");
		
		// create a client connection based on credentials
		AmazonS3 s3client = new AmazonS3Client(credentials);
		 List<DocumentMetadata> metadataList = new ArrayList<DocumentMetadata>();
        try {
            //System.out.println("Listing objects");
   
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
                .withBucketName(bucketName)
                .withPrefix("a");
            ObjectListing objectListing;            
            do {
                objectListing = s3client.listObjects(listObjectsRequest);
               
                for (S3ObjectSummary objectSummary : 
                	objectListing.getObjectSummaries()) {
/*                    System.out.println(" - " + objectSummary.getKey() + "  " +
                            "(size = " + objectSummary.getSize() + "   "+ objectSummary.getOwner() + "  " + objectSummary.getLastModified() +
                            ")");
                	*/
                
                    if (!objectSummary.getKey().endsWith("/"))
                    {
                    	String actualKey = objectSummary.getKey();
                    	int lastIndex = actualKey.lastIndexOf("/");
                    	
                    	String uuid = actualKey.substring(0,lastIndex);
                    	String fileName = actualKey.substring(lastIndex+1);
                    	String fileDate = DATE_FORMAT.format(objectSummary.getLastModified());
                    	String name = objectSummary.getOwner().getDisplayName();
                    	Properties fileProperties = new Properties();
                    	fileProperties.put("uuid", uuid);
                    	fileProperties.put("file-name", fileName);
                    	fileProperties.put("document-date", fileDate);
                    	fileProperties.put("person-name", name);
                    	DocumentMetadata document = new DocumentMetadata(fileProperties);
                    	metadataList.add(document);
                    }
                
                }
                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());
         } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, " +
            		"which means your request made it " +
                    "to Amazon S3, but was rejected with an error response " +
                    "for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, " +
            		"which means the client encountered " +
                    "an internal error while trying to communicate" +
                    " with S3, " +
                    "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        }
     return metadataList;
	
	}
}