package com.elephant.test;

import org.apache.hadoop.fs.s3.S3FileSystem;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * 用于读取将要聚类的数据
 */
public class ReadGroupLensData {
	/**
	 * 读取GroupLens电影数据u.item
	 *
	 * @param 	filePath	String		数据文件路径
	 * @return 	points		double[][]	二维数组
	 */
	public  double[][] getItemVector(String filePath){
		double[][] points = new double[1682][19];
		try {
			Scanner scanner = new Scanner(new FileReader(filePath));//如果new File()只读540行，不知道为什么
			int i=0;
			while (scanner.hasNext()){
				String line=scanner.nextLine();
				String[]words=line.split("([|]+)");
				for (int j=0;j<words.length-4;j++){
					points[i][j]=Double.parseDouble(words[j+4]);
				}
				i++;
			}
		} catch(IOException e){
			System.out.println("Error reading file '" + filePath + "'");
		}
		return points;
	}

	/**
	 * 将得二维数组转换成List
	 *
	 * @param	points	double[][]
	 * @return	list	List<Vector>
	 */
	public  List<Vector> getPoints(double[][] points) {
		List<Vector> list = new ArrayList<Vector>();
		for (int i = 0; i < points.length; i++) {
			double[] row = points[i];
			Vector vector = new RandomAccessSparseVector(row.length);
			vector.assign(row);
			list.add(vector);
		}
		return list;
	}

	public static void main(String[] args){
		ReadGroupLensData rg=new ReadGroupLensData();
		String filePath = "./data/u.item";
		double[][] points=rg.getItemVector(filePath);
		List<Vector> list_Vector_GroupLens=rg.getPoints(points);

		for (Vector vector : list_Vector_GroupLens)
			System.out.println(vector);

	}
}
