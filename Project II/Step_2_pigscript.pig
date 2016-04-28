Register /usr/local/pig/lib/piggybank.jar;
A = LOAD 'StatewiseDistrictwisePhysicalProgress.xml' using org.apache.pig.piggybank.storage.XMLLoader('row') as (index:chararray);

Dump A;

DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();

B = FOREACH A GENERATE 
XPath(index,'/row/State_Name') as State_Name,
XPath(index,'/row/District_Name') as District_Name, 
XPath(index,'/row/Project_Objectives_IHHL_BPL') as Project_Objectives_IHHL_BPL,
XPath(index,'/row/Project_Objectives_IHHL_APL') as Project_Objectives_IHHL_APL,
XPath(index,'/row/Project_Objectives_IHHL_TOTAL') as Project_Objectives_IHHL_TOTAL,
XPath(index,'/row/Project_Objectives_SCW') as Project_Objectives_SCW,
XPath(index,'/row/Project_Objectives_School_Toilets') as Project_Objectives_School_Toilets,
XPath(index,'/row/Project_Objectives_Anganwadi_Toilets') as Project_Objectives_Anganwadi_Toilets,
XPath(index,'/row/Project_Objectives_RSM') as Project_Objectives_RSM,
XPath(index,'/row/Project_Objectives_PC') as Project_Objectives_PC,
XPath(index,'/row/Project_Performance-IHHL_BPL') as Project_Performance_IHHL_BPL,
XPath(index,'/row/Project_Performance-IHHL_APL') as Project_Performance_IHHL_APL,
XPath(index,'/row/Project_Performance-IHHL_TOTAL') as Project_Performance_IHHL_TOTAL,
XPath(index,'/row/Project_Performance-SCW') as Project_Performance_SCW,
XPath(index,'/row/Project_Performance-School_Toilets') as Project_Performance_School_Toilets,
XPath(index,'/row/Project_Performance-Anganwadi_Toilets') as Project_Performance_Anganwadi_Toilets,
XPath(index,'/row/Project_Performance-RSM') as Project_Performance_RSM,
XPath(index,'/row/Project_Performance-PC') as Project_Performance_PC;
Describe B;
Dump B;
store B into 'pigout';
cat pigout;
res =Foreach  B Generate District_Name, myudfs.Ave_Obj_Perf_Perc($2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16);

Result =Filter res By $1 >=100;
store Result into 'Problem_Statement1';

res2 =Filter B By  not  myudfs.Ave_Obj_Perf_Perc_Flt($2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16);

Result2 =Foreach res2 Generate $0 As District_Name;
store Result2 into 'Problem_statement2';

res3 =Foreach  B Generate District_Name, myudfs.Ave_Obj_Perf_Perc_Flt($2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16);

Result =Filter res3 By $1 ==true;
store Result into 'Problem_Statement3';







