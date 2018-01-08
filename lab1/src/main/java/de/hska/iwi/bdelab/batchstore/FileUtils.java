package de.hska.iwi.bdelab.batchstore;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

public class FileUtils {
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(FileUtils.class);

	// change this to your IZ account ID
	// make sure an operator created your hdfs home folder
	public static final String USERID = "esti1012";

    static {
        try (final InputStream stream =
                     FileUtils.class.getClassLoader().getResourceAsStream("hadoop.properties")) {
            properties.load(stream);
        } catch (IOException ioe) {
            log.error(ioe.toString());
        }
    }

    public static final String HDFS_USER = properties.getProperty("hadoop.user.name");
    public static final String HDFS_GROUPS = properties.getProperty("hadoop.group.names");;

    private static final String NEW_PAIL = "pageviews-new";
    private static final String MASTER_PAIL = "pageviews-master";

    private static final String HDFS_BASE = "/user/" + HDFS_USER;
    private static final String LOCAL_BASE = "/tmp/bde";
    private static final String TMP_BASE = "bdetmp";

    private static final String FACT_BASE = "facts";
    private static final String RESULT_BASE = "results";

    private static final String CORE_SITE = "/usr/local/lib/hadoop-2.7.0/etc/hadoop/core-site.xml";
    private static final String HDFS_SITE = "/usr/local/lib/hadoop-2.7.0/etc/hadoop/hdfs-site.xml";

    private static final String HDFS_HOST = "193.196.105.68";
    private static final int HDFS_PORT = 9000;
    private static final URI HDFS_URI = URI.create("hdfs://" + HDFS_HOST + ":" + HDFS_PORT + HDFS_BASE);

    public static FileSystem getFs(boolean local) {
        Configuration conf = new Configuration();
        FileSystem fs = null;
        try {
            if (!local) {
                conf.addResource(new Path(CORE_SITE));
                conf.addResource(new Path(HDFS_SITE));
                fs = FileSystem.get(HDFS_URI, conf, HDFS_USER);
            } else {
                fs = FileSystem.getLocal(conf);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
        return fs;
    }

    public static String prepareMasterFactsPath(boolean delete, boolean local) throws IOException {
        FileSystem fs = FileUtils.getFs(false);
        return getPath(fs, FileUtils.FACT_BASE, FileUtils.MASTER_PAIL, delete, local);
    }

    public static String prepareNewFactsPath(boolean delete, boolean local) throws IOException {
        FileSystem fs = FileUtils.getFs(false);
        return getPath(fs, FileUtils.FACT_BASE, FileUtils.NEW_PAIL, delete, local);
    }

    public static String prepareResultsPath(String resultType, boolean delete, boolean local) throws IOException {
        FileSystem fs = FileUtils.getFs(local);
        return getPath(fs, FileUtils.RESULT_BASE, resultType, delete, local);
    }

    public static String getTmpPath(FileSystem fs, String name, boolean delete, boolean local) throws IOException {
        return getPath(fs, TMP_BASE, name, delete, local);
    }

    private static String getPath(FileSystem fs, String pathName, String fileName, boolean delete, boolean local) throws IOException {
        String fqpn = (local ? LOCAL_BASE : HDFS_BASE) + Path.SEPARATOR + pathName;
        if (fileName != null)
            fqpn = fqpn + Path.SEPARATOR + fileName;
        log.info("Preparing FQPN '" + fqpn + "' which is " + (delete ? "to" : "not to") + " be deleted");
        Path fqp = new Path(fqpn);
        if (fs.exists(fqp) && delete) {
            log.debug("Deleting FQPN '" + fqpn + "'");
            fs.delete(fqp, true);
        } else {
            log.info("Preparing FQPN parent dir '" + fqp.getParent().toUri().getPath() + "'");
            fs.mkdirs(fqp.getParent());
        }
        return fqpn;
    }

}