
# INPUT_DATA_PATH=/tmp/xiaolin/wordcount/input
INPUT_DATA_PATH=/tmp/xiaolin/wordcount/clean_review
OUTPUT_DATA_PATH=/tmp/xiaolin/wordcount/output

# MAIN_CLASS=com.zetdata.MapReduceSentimentAnalyzer
MAIN_CLASS=com.zetdata.MapReduceAvroSentimentAnalyzer

mvn clean compile assembly:single && \
    hadoop fs -rm -R -f $OUTPUT_DATA_PATH && \
    hadoop jar target/HelloAvro-1.0-SNAPSHOT-jar-with-dependencies.jar $INPUT_DATA_PATH $OUTPUT_DATA_PATH && \
    rm -rf output && hadoop fs -get $OUTPUT_DATA_PATH
