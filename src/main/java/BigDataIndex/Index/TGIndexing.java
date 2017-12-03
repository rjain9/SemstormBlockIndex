package BigDataIndex.Index;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;

/**
 * @author rishijain
 *
 */
public class TGIndexing {
	
	
	/**
	 * 
	 */
	public void TGIndex(/*BlockData*/){
		/*
		 *	Code for-
		 * 	1.Getting the block data as parameter.
		 *	2.Extracting Object columns from the block data and modifying it accordingly 
		 *	to call indexBlock method.
		 *	3.returning/adding the generated index and statistics to the block data.
		 * 
		 * */
		
	}
	
	/**
	 * @param Map<Text, List<Text>> objColumns - Map<ColumnName, List<Column Values>>
	 * @return Map<Text, Map<Text, Text>> - Map<ColumnName, Map<Statistic name, value>>
	 */
	public static Map<Text, Map<Text, Text>> indexBlock(Map<Text, List<Text>> objColumns){
		Map<Text, Map<Text, Text>> Index = new HashMap<Text, Map<Text, Text>>();
		
		for(Text key: objColumns.keySet()){
			List<Text> columnValues = objColumns.get(key);
			Text[] minMax = calculateMinMax(columnValues);
			
			Map<Text, Text> temp= new HashMap<Text, Text>();
			temp.put(new Text("count"), new Text(columnValues.size()+""));
			temp.put(new Text("minimum"), minMax[0]);
			temp.put(new Text("maximum"), minMax[1]);
			
			Index.put(key, temp);
		}
		return Index;
	}
		
	
	/**
	 * @param columnValues - values pertining to any one particular column
	 * @return Text[] index 0- minimum value; index 1- maximum value;
	 */
	public static Text[] calculateMinMax(List<Text> columnValues){
		Text[] minMax = new Text[2];
		Text min = columnValues.get(0);
		Text max = columnValues.get(0);
		
		for(Text value:columnValues){
			if(value.compareTo(max)>0){
				max = value;
			}
			
			if(value.compareTo(min)<0){
				min = value;
			}
		}
		
		minMax[0] = min;
		minMax[1] = max;
		
		return minMax;
	}
	
	public static void main(String args[]) {
		// TODO Auto-generated method stub
			List<Text> marks = new ArrayList<Text>();
			for(int i=0; i<10; i++){
				Text temp = new Text((100*Math.random())+"");
				System.out.println(temp);
				marks.add(temp);
			}
			List<Text> course = new ArrayList<Text>();
			course.add(new Text("Database Systems"));
			course.add(new Text("Advanced Data Structures"));
			course.add(new Text("Foundation Of Data Science"));
			course.add(new Text("Algorithm"));
			course.add(new Text("DevOps"));
			course.add(new Text("Graph Theory"));
			course.add(new Text("Internet Protocol"));
			course.add(new Text("Software Security"));
			course.add(new Text("Computer Networks"));
			course.add(new Text("Machine Learning"));
			course.add(new Text("Social Computing"));
			
			Map<Text, List<Text>> objColumns = new HashMap<Text, List<Text>>();
			objColumns.put(new Text("marks"), marks);
			objColumns.put(new Text("course"), course);
			
			Map<Text, Map<Text, Text>> index = indexBlock(objColumns);
			
			for(Text key:index.keySet()){
				Map<Text, Text> temp = index.get(key);
				System.out.println(key+":");
				for(Text subkey:temp.keySet()){
					System.out.println(subkey+": "+temp.get(subkey));

				}
			}

	}
}


