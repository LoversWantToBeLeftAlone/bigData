package MR;

import java.io.IOException;

public class Driver {
	private static int times=10;
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
	
		String [] path_job1 = {args[0],"out1"};
		jobOne.main(path_job1);
		
		String [] path_job2 = {"out1","out2"};
		jobTwo.main(path_job2);
		
		String [] path_job3 = {"out2","out3"};
		jobThree.main(path_job3);
		
		String [] path0 = {"out3",args[1]+"/data0"};
		pre_pagerank.main(path0);
		//使用job1的输出文件result1作为job2的输入文件，然后指定job2的输出文件result2
		String []path1={"",""};
		for(int i=0;i<times;i++){
			path1[0]=args[1]+"/data"+i;
			path1[1]=args[1]+"/data"+String.valueOf(i+1);
			pageRank.main(path1);
		}
		
		String[]path2={args[1]+"/data"+times,args[1]+"/finalrank"};
		
		rankSort.main(path2);
		
		//标签传播
		
		String [] path3 = {"out3",args[1]+"/LABEL/label0"};
		preLPA.main(path3);
		//使用job1的输出文件result1作为job2的输入文件，然后指定job2的输出文件result2
		String []path4={"",""};
		for(int i=0;i<times;i++){
			path4[0]=args[1]+"/LABEL/label"+i;
			path4[1]=args[1]+"/LABEL/label"+String.valueOf(i+1);
			LPA.main(path4);
		}
		
		String[]path5={args[1]+"/LABEL/label"+times,args[1]+"/finalLabel"};	
		LPASort.main(path5);
	}

}
