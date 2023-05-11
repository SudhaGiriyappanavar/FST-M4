Scriptfile1 = LOAD 'hdfs:///user/sudhapg/inputs/episodeIV_dialouges.txt' USING PigStorage('\t') AS (Name:chararray,Speech:chararray);
Scriptfile2 = LOAD 'hdfs:///user/sudhapg/inputs/episodeV_dialouges.txt' USING PigStorage('\t') AS (Name:chararray,Speech:chararray);
Scriptfile3 = LOAD 'hdfs:///user/sudhapg/inputs/episodeVI_dialouges.txt' USING PigStorage('\t') AS (Name:chararray,Speech:chararray);
--remove first two line
ranked1 = RANK Scriptfile1;
OnlyDilouges1 = FILTER ranked1 BY(rank_Scriptfile1 >2);
ranked2 = RANK Scriptfile2;
OnlyDilouges2 = FILTER ranked2 BY(rank_Scriptfile2 >2);
ranked3 = RANK Scriptfile3;
OnlyDilouges3 = FILTER ranked3 BY(rank_Scriptfile3 >2);
FullDialouges = UNION OnlyDilouges1 ,OnlyDilouges2 , OnlyDilouges3;
-- Group data using the name column
GroupByname = GROUP FullDialouges BY Name;
--GroupByname2 = GROUP Scriptfile2 BY Name;
--GroupByname3 = GROUP Scriptfile3 BY Name;
CountByName = FOREACH GroupByname GENERATE $0 AS Name , COUNT($1) AS no_of_lines;
--CountByName2 = FOREACH GroupByname2 GENERATE CONCAT((chararray)$0, CONCAT(':', (chararray)COUNT($1)));
--CountByName3 = FOREACH GroupByname3 GENERATE CONCAT((chararray)$0, CONCAT(':', (chararray)COUNT($1)));
Ordername = ORDER CountByName BY no_of_lines DESC;
--remove output folders if already exist.
rmf hdfs///root/user/sudhapg/inputs
-- Save result in HDFS folder
STORE CountByName INTO 'Project_Activity1_Output' USING PigStorage('\t');
--STORE CountByName2 INTO 'Project_Activity1_Output'2 USING PigStorage2('\t');
--STORE CountByName3 INTO 'Project_Activity1_Output' 3USING PigStorage3('\t');
