package quickstart;

import java.io.BufferedReader;
import java.io.File;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.blob.BlobRange;
import com.microsoft.azure.storage.blob.BlockBlobURL;
import com.microsoft.azure.storage.blob.ContainerURL;
import com.microsoft.azure.storage.blob.ListBlobsOptions;
import com.microsoft.azure.storage.blob.PipelineOptions;
import com.microsoft.azure.storage.blob.ServiceURL;
import com.microsoft.azure.storage.blob.SharedKeyCredentials;
import com.microsoft.azure.storage.blob.StorageURL;
import com.microsoft.azure.storage.blob.TransferManager;
import com.microsoft.azure.storage.blob.models.BlobItem;
import com.microsoft.azure.storage.blob.models.ContainerCreateResponse;
import com.microsoft.azure.storage.blob.models.ContainerListBlobFlatSegmentResponse;
import com.microsoft.rest.v2.RestException;
import com.microsoft.rest.v2.util.FlowableUtil;

import io.reactivex.*;
import io.reactivex.Flowable;

public class Quickstart {

    static void uploadFile(BlockBlobURL blob, File sourceFile) throws IOException {

            AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(sourceFile.toPath());
            TransferManager.uploadFileToBlockBlob(fileChannel, blob, 8*1024*1024, null, null)
            .subscribe(response-> {
                System.out.println("Completed upload request.");
                System.out.println(response.response().statusCode());
            });
    }

    static void listBlobs(ContainerURL containerURL) {      
        ListBlobsOptions options = new ListBlobsOptions();
        options.withMaxResults(10);

        containerURL.listBlobsFlatSegment(null, options, null).flatMap(containerListBlobFlatSegmentResponse ->
            listAllBlobs(containerURL, containerListBlobFlatSegmentResponse))
            .subscribe(response-> {
                System.out.println("Completed list blobs request.");
                System.out.println(response.statusCode());
            });
    }

    private static Single <ContainerListBlobFlatSegmentResponse> listAllBlobs(ContainerURL url, ContainerListBlobFlatSegmentResponse response) {
        if (response.body().segment() != null) {
            for (BlobItem b : response.body().segment().blobItems()) {
                String output = "Blob name: " + b.name();
                if (b.snapshot() != null) {
                    output += ", Snapshot: " + b.snapshot();
                }
                System.out.println(output);
            }
        }
        else {
            System.out.println("There are no more blobs to list off.");
        }

        if (response.body().nextMarker() == null) {
            return Single.just(response);
        } 
        else {
            String nextMarker = response.body().nextMarker();


            return url.listBlobsFlatSegment(nextMarker, new ListBlobsOptions().withMaxResults(10), null)
                    .flatMap(containersListBlobFlatSegmentResponse ->
                            listAllBlobs(url, containersListBlobFlatSegmentResponse));
        }
    }

    static void deleteBlob(BlockBlobURL blobURL) {
        // Delete the blob
        blobURL.delete(null, null, null)
        .subscribe(
            response -> System.out.println(">> Blob deleted: " + blobURL),
            error -> System.out.println(">> An error encountered during deleteBlob: " + error.getMessage()));
    }

    static void getBlob(BlockBlobURL blobURL, File sourceFile) throws IOException {
    	System.out.println(sourceFile.getName());
        AsynchronousFileChannel fileChannel = AsynchronousFileChannel.open(sourceFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        System.out.println(sourceFile.getName());
        TransferManager.downloadBlobToFile(fileChannel, blobURL, null, null)
        .subscribe(response-> {
            System.out.println("Completed download request.");
            System.out.println("The blob was downloaded to " + sourceFile.getAbsolutePath());
        });
    }

    public static void main(String[] args) throws java.lang.Exception{
        ContainerURL containerURL;

        // Creating a sample file to use in the sample
        File sampleFile = new File("C:\\Users\\aayush.surana\\Documents\\as.txt");
        String name = sampleFile.getName();
        String extension = name.substring(name.lastIndexOf("."));

        try {
           // sampleFile = createTempFile();

            
            String accountName = "aayushstorage1";
            String accountKey = "mtHuhjjsdM3yPtEOY5nQaR2+ne/fvOdwrMQ4nxKa2lNZb1c0tiwBchTUM2dCGmI5vTrfXhyuYg+oWIAtV4o2mA==";
            SharedKeyCredentials creds = new SharedKeyCredentials(accountName, accountKey);
            final ServiceURL serviceURL = new ServiceURL(new URL("https://" + accountName + ".blob.core.windows.net"), StorageURL.createPipeline(creds, new PipelineOptions()));
            containerURL = serviceURL.createContainerURL("aayushsuranatest");
            File downloadedFile = new File(sampleFile.getName());
            System.out.println(downloadedFile.getName() + "----" + sampleFile.getName());
            try {
                ContainerCreateResponse response = containerURL.create(null, null, null).blockingGet();
                System.out.println("Container Create Response was " + response.statusCode());
            } catch (RestException e){
                if (e instanceof RestException && ((RestException)e).response().statusCode() != 409) {
                    throw e;
                } else {
                    System.out.println("quickstart container already exists, resuming...");
                }
            }
            
            // Create a BlockBlobURL to run operations on Blobs
            final BlockBlobURL blobURL = containerURL.createBlockBlobURL("SampleBlob1.txt");

            // Listening for commands from the console
            System.out.println("Enter a command");
            System.out.println("(P)utBlob | (L)istBlobs | (G)etBlob | (D)eleteBlobs | (E)xitSample");
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

            while (true) {

                System.out.println("# Enter a command : ");
                String input = reader.readLine();

                switch(input){
                    case "P":
                        System.out.println("Uploading the sample file into the container: " + containerURL );
                        uploadFile(blobURL, sampleFile);
                        break;
                    case "L":
                        System.out.println("Listing blobs in the container: " + containerURL );
                        listBlobs(containerURL);
                        break;
                    case "G":
                        System.out.println("Get the blob: " + blobURL.toString() );
                       // System.out.println(downloadedFile.getName());
                        getBlob(blobURL, downloadedFile);
                        break;
                    case "D":
                        System.out.println("Delete the blob: " + blobURL.toString() );
                        deleteBlob(blobURL);
                        System.out.println();
                        break;
                    default:
                        break;
                }
            }
        } catch (InvalidKeyException e) {
            System.out.println("Invalid Storage account name/key provided");
        } catch (MalformedURLException e) {
            System.out.println("Invalid URI provided");
        } catch (RestException e){
            System.out.println("Service error returned: " + e.response().statusCode() );
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}
