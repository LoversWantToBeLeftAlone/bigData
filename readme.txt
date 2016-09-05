大数据实验：金鹰的江湖。
组号：2016st18
组员:131220044查鹏（组长）,131220034许金强。


1.jobOne.class:将同一段落中出现的人名提取出来。比如郭靖黄蓉出现再同一段，输出“郭靖，黄蓉”；

2.jobTwo.class:如果两个人在同一段中出现，则将两个人之间的这条边权重加１．比如:<郭靖，黄蓉>　2,<杨康，杨过>　1。

3.jobThree.class:输入<A,B>1;<A,C>2;<A,D>3,,MAP输出A B,1|C,2|D,3|,REDUECE输出:A B,1/6|B,2/6|C,3/6。

4.pre_pagerank.class和pagerank.class:进行pagerank值的计算。其中pre_pagerank作用是把jobThree的输出转变为pagerank可以接受的输入。

5.pre_lpa.class和LPA.class实现了标签传播。


实验环境:

1.JDK 1.8.0;
2.Hadoop 2.7.1;Ubuntu 12.04.


运行方式:
hadoop jar Driver.jar /data/task2/novels result。

最后hadoop fs -ls out1 out2 out3可以查看前三个job的结果。

hadoop fs -ls result/finalrank和hadoop fs -ls result/finallabel可以查看最终的pagerank和label。

附注：rank.csv里是每一个人物在pagerank过程中的pr值的变化。可以看出是否收敛。
	node.csv和edge.csv是gephi绘图时候的节点表和边表。
