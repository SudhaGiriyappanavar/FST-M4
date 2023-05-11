-- load the input file
inputFile = LOAD 'hdfs:///user/sudhapg/input.txt' AS (lines:chararray);
-- Tokenize the line into words
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(lines)) AS word;
-- Group the words
grpd = GROUP words BY word;
-- count the number of words (REDUCE)
wordCount = FOREACH grpd GENERATE group, COUNT(words);
-- Store the output in HDFS
STORE wordCount INTO 'hdfs:///user/sudhapg/results';
