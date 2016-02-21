# 实例分析：基于 MapReduce 的电影推荐系统


## 准备知识

* 推荐系统
* Python
* Mapreduce

推荐系统在互联网领域有着广泛的运用,本案例实现了对大量电影评论数据的整合处理,并根据电影间的相关程度,根据用户观影历史向用户推荐相关度较高的电影。## 数据准备

本文所用数据来自 [GroupLens 网站](http://grouplens.org/datasets/movielens/),包含 1000 名用户对 1700 部电影的 100000 条 评论(1998 年),为方便处理首先对数据进行了简单预处理。10 万条数据中,每条数据由用户编号、电影名称、评分组成。## 建立模型

针对数据特点,电 影的推荐系统设计如下:
* 对每一对电影 A 和 B,对所有评论过这两部电影的用户及评分数据进行聚集; * 使用评分数据计算电影 A 与 B 的相关系数;* 对所有看过某电影的用户,我们可以向其推荐与其相关系数最高的几部电影。 
该推荐系统的简要 MapReduce 步骤如下。### MapReduce1：把评分数据按用户进行聚集
原始数据(user|movie|rating)可视为 Key 为用户 ID,Value 为电影名和评分的 Key-Value 对,因此 Map 阶段不需要过多操作,可以直接使用 cat 函数作为 Mapper, 将 Key-Value 对传递给 Reducer。而 Reducer 则根据用户 ID,将同一用户评分过的所 有电影名和评分进行汇集。最后将同一用户的所有评分数据储存在同一行,输出数据 共 1000 行。此处通过分行将不同用户的评分区分开来,因此用户 ID 可以在后续分析 中舍弃。
Mapper: cat 函数
Reducer:	#! /usr/bin/python3	#reduce1: aggregate all the films and scores by the same user import sys	from collections import defaultdict	#reduce process	dict=defaultdict(list)	#use defaultdict for key-value storage	while True: 		try:			line=sys.stdin.readline() 			if not line:				break	        user_id, item_id, rating = line.split('|')			dict[user_id].append((item_id,float(rating))) 		except:			continue	sys.stdin.close()	#reducer output: user_id:(film_id,rating)	#save to local file in local model	#use different separator for line split in next stage 	with open('ratings1.csv','w') as f:		for k in dict.keys(): for j in dict[k]:	        f.write(str(j[0])+'*'+str(j[1])+'|')	    f.write('\n')	#standard output to HDFS	#use different separator for line split in next stage 	for k in dict.keys():		for j in dict[k]: 			print(str(j[0])+'*'+str(j[1])+'|',end="")	    print('\n')reduce 代码中,使用了 defaultdict 类型对 Key-Value 型数据进行储存。相对于普通的字典型数据,defaultdict 可以自动设定默认值,可以方便地储存本文的数据类 型。在读入数据时,使用了 try 函数防止错误格式的数据输入,当某一行数据格式有
误时,自动从下一行继续读入。为了方便第二阶段 MapReduce 对数据进行分隔,此处 reduce 的输出使用了*和|作为分隔符。模拟 Hadoop 模式调试:
	cat ratings.csv | sort -t $'|' -k 1,1 | ./reduce1.pyHadoop 集群运行:
	hadoop jar /home/dmc/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar \	-D stream.map.output.field.separator=\| \	-file /home/ruc15/chen_sicong/MR/reduce1.py \	-input chen_sicong/ratings.csv \	-output chen_sicong/output \	-mapper "/bin/cat" \	-reducer "reduce1.py"Hadoop 集群模式下运行时,需要事先把数据文件上传至 HDFS,由于 mapper 的输 出不是默认的逗号分隔,需要使用-D 指令进行设置。使用-file 指令将 Reducer 代码 分发到各个节点,以解决“找不到执行程序”的错误。
###MapReduce2:计算每两部电影之间的皮尔逊相关系数Mapper: 	#!/usr/bin/python3	#map2: get all the combinations of 2 films and their scores by the same user	from itertools import combinations	import sys	while True:		try:			line=sys.stdin.readline()
			if not line:				break			line=line.strip()			values=line.split('|')	#combinations(values,2) get all the combinations of 2 films	for item1, item2 in combinations(values,2):	#check if the items are empty		if len(item1)>1 and len(item2)>1:			item1,item2=item1.split('*'),item2.split('*')			print((item1[0],item2[0]),end='')			print('|',end='')			print(item1[1]+','+item2[1])	#output of map2: (film1,film2)|(score of film1,score of film2)		except:			continue
			Mapper 的主要任务为输出两两电影组合及其评分的 Key-Value 对,输出给 Reducer。	#!/usr/bin/python3	#reduce2: aggregate and calculate the corralation of all the film combinations	import sys	from collections import defaultdict	from math import sqrt	#get the key-value data sent by mapper	#save the key-value data into a defaultdict	dict=defaultdict(list)	while True:		try:			line=sys.stdin.readline()			if not line:				break			line=line.strip()			keys,scores=line.split('|')			dict[keys].append(scores)		except:			continue	#calculate the correlation of two films	def normalized_correlation(n,sum_xy,sum_x,sum_y,sum_xx,sum_yy):		numerator=(n*sum_xy-sum_x*sum_y)		denominator=sqrt(n*sum_xx-sum_x*sum_x)*sqrt(n*sum_yy-sum_y*sum_y) 		if denominator != 0:			similarity=numerator/denominator 		else:			similarity=None 		return similarity,n	#aggregate all the scores of film combinations and send them to correlation function	final=defaultdict(list)	for key in dict:		sum_xx,sum_xy,sum_yy,sum_x,sum_y,n=(0.0,0.0,0.0,0.0,0.0,0)		for item in dict[key]:			try:				score1,score2=item.split(',') 				score1,score2=float(score1),float(score2) 				sum_xx+=score1*score1 				sum_yy+=score2*score2 				sum_xy+=score1*score2				sum_y+=score2				sum_x+=score1				n+=1			except: 				continue		similarity=normalized_correlation(n,sum_xy,sum_x,sum_y,sum_xx,sum_yy) 		if None not in similarity:		        final[key].append(similarity)	        	#save file to local in local model	with open('final.csv','w') as f: 	for k in final.keys():	    f.write(str(k)+','+str(final[k])+'\n')	#standard output to HDFS	for k in final.keys(): 		print(str(k)+','+str(final[k]))	#output of recuder2: (filmA,filmB),correlations,numer of usersReducer 的主要任务是将 Mapper 传递的数据进行汇总并计算相关系数。
模拟 Hadoop 模式调试:
	cat ratings1.csv | ./map2.py | sort -t $'|' -k 1,1 | ./reduce2.py

Hadoop 集群运行:	hadoop jar /home/dmc/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar \	-D stream.map.output.field.separator=\| \	-file /home/ruc15/chen_sicong/MR/map2.py \	-file /home/ruc15/chen_sicong/MR/reduce2.py \	-input chen_sicong/ratings1.csv \	-output chen_sicong/output \	-mapper "map2.py" \	-reducer "reduce2.py"根据两次 mapreduce 计算,我们得出了任意两部电影之间的相关系数。这样,当某 一用户看过电影 A 时,我们可以对其推荐相关程度最大的几部电影。一个简单的电影 推荐系统得以实现。在使用时应该将相关系数与评价人数结合起来进行考虑,如果评 价人数过少,即使相关系数较高,结果也未必可信。  
本案例实现的基于内容过滤的推荐系统简单快速,但不能为用户发现新的感兴趣的商品,只能发现和用户已有兴趣相似的商品。在后续的改进中,可以建立更准确有效的推荐系统。

（感谢中国人民大学陈思聪提供素材和案例。）