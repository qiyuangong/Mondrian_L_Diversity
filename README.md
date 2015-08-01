Mondrian for L-Diversity
===========================
Mondrian is a Top-down greedy data anonymization algorithm for relational dataset, proposed by Kristen LeFevre in his papers[1]. To our knowledge, Mondrian is the fastest local recording algorithm, which preserve good data utility at the same time. Although LeFevre gave the pseudocode in his papers, the original source code is not available. You can find the Java implementation in Anonymization Toolbox[3]. Mondrian for L-diversity is based on InfoGain Mondrian[2], but more simple.

This repository is an **open source python implementation for Mondrian for L-Diversity**. I release this algorithm in python for further study.

### Motivation 
Researches on data privacy have lasted for more than ten years, lots of great papers have been published. However, only a few open source projects are available on Internet [3-4], most open source projects are using algorithms proposed before 2004! Fewer projects have been used in real life. Worse more, most people even don't hear about it. Such a tragedy! 

I decided to make some effort. Hoping these open source repositories can help researchers and developers on data privacy (privacy preserving data publishing).


### Usage:
My Implementation is based on Python 2.7 (not Python 3.0). Please make sure your Python environment is correctly installed. You can run Mondrian in following steps: 

1) Download (or clone) the whole project. 

2) Run "anonymized.py" in root dir with CLI.


	# run Mondrian with default l(l=10)
	python anonymizer.py 
	
	# run Mondrian with l=20
	python anonymized.py 20


## For more information:
[1]  LeFevre, Kristen, David J. DeWitt, and Raghu Ramakrishnan. Mondrian multidimensional k-anonymity. Data Engineering, 2006. ICDE'06. Proceedings of the 22nd International Conference on. IEEE, 2006.

[2] Workload-aware Anonymization Techniques for Large-scale Datasets ACM Trans. Database Syst., ACM, 2008, 33, 17:1-17:47

[3] [UTD Anonymization Toolbox](http://cs.utdallas.edu/dspl/toolbox/)

[4] [ARX- Powerful Data Anonymization](https://github.com/arx-deidentifier/arx)
