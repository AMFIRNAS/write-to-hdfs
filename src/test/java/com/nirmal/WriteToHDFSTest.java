package com.nirmal;

import java.io.InputStream;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.carbon.ml.core.exceptions.MLOutputAdapterException;
import org.wso2.carbon.ml.core.impl.FileInputAdapter;
import org.wso2.carbon.ml.core.impl.HdfsOutputAdapter;
import org.wso2.carbon.ml.core.interfaces.MLInputAdapter;
import org.wso2.carbon.ml.core.interfaces.MLOutputAdapter;

/**
 * This class could write a given file to HDFS.
 */
public class WriteToHDFSTest {
    private static final Log log = LogFactory.getLog(WriteToHDFSTest.class);

    @Test
    public void writeToHDFSTest() throws Exception {

        String hdfsUrl = System.getProperty("hdfs.url") != null ? System.getProperty("hdfs.url") : "hdfs://localhost:9000";
        log.info("HDFS URL: "+hdfsUrl);
        MLInputAdapter fileInAdapter = new FileInputAdapter();
        String filePrefix = "file://";
        String uriString = System.getProperty("file.path");
        if (uriString == null) {
            log.error("You have to set file.path system property and point it to the file you want to write to HDFS");
        }
        URI uri;
        InputStream in = null;

        // read a local file and get an input stream
        uri = new URI(filePrefix + uriString);

        in = fileInAdapter.read(uri);
        Assert.assertEquals(in != null, true, "File input stream is null.");

        // write it to hdfs
        MLOutputAdapter hdfsOutAdapter = new HdfsOutputAdapter();
        String outPath = hdfsUrl + System.getProperty("hdfs.path") != null ? System.getProperty("hdfs.path") : "/files/test.csv";
        log.info("HDFS path: "+outPath);
        try {
            hdfsOutAdapter.write(outPath, in);
        } catch (MLOutputAdapterException e1) {
            log.error(e1);
        }

    }

}
