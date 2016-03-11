package com.elephant.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.fuzzykmeans.FuzzyKMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * 对Grouplens中的电影属性模糊聚类
 * 将	item_id <-> cluster_id	<->	weight	的对应关系写入HDFS文件系统中
 */

public class FUzzyKMeansClustering {

	/** 读取u.item中的数据	*/
	public static final double[][] points= ReadData.readData();

	/**
	 * 将vectors以向量的形式写入HDFS
	 *
	 * @param vectors	List<Vector>	电影属性向量
	 * @param fileName	String			将要写入的文件位置
	 * @param fs		FileSystem
	 * @param conf		Configuration
	 * @throws IOException
	 */
	public static void writePointsToFile(List<Vector> vectors, String fileName, FileSystem fs, Configuration conf) throws IOException {

        Path path = new Path(fileName);
        SequenceFile.Writer writer =SequenceFile.createWriter(fs, conf, path, LongWritable.class, VectorWritable.class);
        long recNum = 0;
        VectorWritable vec = new VectorWritable();
        for (Vector point : vectors) {
            vec.set(point);
            writer.append(new LongWritable(recNum++), vec);
        }
        writer.close();
    }

	/**
	 * 将二维数组表示为向量
	 *
	 * @param 	array	double			二维数组
	 * @return	points	List<Vector>	List<mahout.math.Vector>
	 */
    public static List<Vector> getPoints(double[][] array) {
        List<Vector> points = new ArrayList<Vector>();
        for (int i = 0; i < array.length; i++) {
            double[] fr = array[i];
            Vector vec = new RandomAccessSparseVector(fr.length);
            vec.assign(fr);
            points.add(vec);
        }
        return points;
    }

	/**
	 * 	主函数
	 *
	 * @param args	null
	 * @throws Exception
	 */

    public static void main(String args[]) throws Exception {
		long beginTime=System.currentTimeMillis();

        List<Vector> vectors = getPoints(points);

        File testData = new File("clustering/testdata");
        if (!testData.exists()) {
            testData.mkdir();
        }
        testData = new File("clustering/testdata/points");
        if (!testData.exists()) {
            testData.mkdir();
        }
	//	Path coreFilePath=new Path("/Users/elephant/dev/hadoop/hadoop-2.6.0/etc/hadoop/core-site.xml");
        Configuration conf = new Configuration();
	//	conf.addResource(coreFilePath);
        FileSystem fs = FileSystem.get(conf);
        writePointsToFile(vectors, "clustering/testdata/points/file1", fs, conf);

        Path path = new Path("clustering/testdata/clusters/part-00000");
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Kluster.class);

// initialize cluster
		Path canopyinput=new Path("clustering/testdata/points/file1");
		//Path vectorwritablefile=new Path("hdfs://localhost:9000/fuzzykmeans/vectorwritable/part-r-00000");
		Path canopyoutput=new Path("clustering/canopyoutput/");
		EuclideanDistanceMeasure measure=new EuclideanDistanceMeasure();
		double t1=4.1;
		double t2=3.0;
		boolean overwrite=true;
		boolean runSequential=true;
		CanopyDriver.run(canopyinput, canopyoutput, measure, t1, t2, overwrite, 0.01, runSequential);
// Run FuzzyKMeans
		Path fuzzykmeansinputdataset=new Path("clustering/testdata/points");
		Path fuzzykmeansinitialcluster=new Path("clustering/canopyoutput/clusters-0-final/");
		Path fuzzykmeansoutput=new Path("clustering/fuzzykmeansoutput/");
		double convergence=0.001;
		int max_iterations=10;
		float fuzzy_factor=1.1f;
		double threshold=0.01;
		boolean runCluster=false;
		FuzzyKMeansDriver.run(fuzzykmeansinputdataset,
				fuzzykmeansinitialcluster,
				fuzzykmeansoutput,
				convergence,
				max_iterations,
				fuzzy_factor,
				true,
				false,
				threshold,
				runCluster);
//
		WriteResultToFile wr=new WriteResultToFile();
		HashMap<Integer,HashSet<String>> hashMap=wr.getData();
		wr.writeDataToLocal(hashMap);

		long endTime=System.currentTimeMillis();
		System.out.println("程序运行时间："+(endTime-beginTime)/1000.0+"秒");
	}
}