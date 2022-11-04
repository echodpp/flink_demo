package com.gt.wordcount;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class StudentLearnStatCore {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用一个自定义的source来生成数据
        DataStreamSource<String> sourceStream = env.addSource(new StudentLearnDataSource()).setParallelism(1); // 设置并行度=1
        SingleOutputStreamOperator<String> outDataStream = (SingleOutputStreamOperator<String>) sourceStream
                .map(new StudentLearnRichMapFunction())
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new StudentLearnRichProcessFunction());
                // 打印学生的数据
                outDataStream.print().setParallelism(1);
                env.execute("StudentLearnStat");
    }
}

package com.gt.wordcount;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * 读取文件，并间隔5s将数据写到下游
 */
public class StudentLearnDataSource implements SourceFunction<String> {
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        try {
            String inputFilePath ="/data/bigdata/student_event_data.txt";
            File file = new File(inputFilePath);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String strLine = null;
            while (null != (strLine = bufferedReader.readLine())) {
                sourceContext.collect(strLine);
// 执行一条sleep 5s
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
    }

    public static void main(String[] args) throws Exception {
        new StudentLearnDataSource().run(null);
    }
}

package com.gt.wordcount;


import org.apache.flink.api.common.functions.RichMapFunction;
import com.alibaba.fastjson.JSONObject;
import java.util.Date;
import java.util.Objects;

/**
 * 将source端的字符串日志转换成对象，便于下游使用
 */
public class StudentLearnRichMapFunction extends RichMapFunction<String, StudentInfoModel> {
    @Override
    public StudentInfoModel map(String input) throws Exception {
        if (Objects.isNull(input)) {
            return null;
        }
        JSONObject inputJson = JSONObject.parseObject(input);
        StudentInfoModel studentInfoModel = new StudentInfoModel(); // 本次测试阶段，我们使用当前时间戳来替代事件时间
        Date date = new Date();
        studentInfoModel.setTime(String.valueOf(date.getTime()));
        studentInfoModel.setName(inputJson.getString("name"));
        studentInfoModel.setEventId(inputJson.getString("event_id"));
        studentInfoModel.setIp(inputJson.getString("ip"));
        System.out.println("studentInfoModel:" + studentInfoModel.toString());
        return studentInfoModel;
    }
}

package com.gt.wordcount;

import com.gt.wordcount.StudentInfoModel;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 汇总计算逻辑
 * 计算每个学生和整个课堂的数据
 */
public class StudentLearnRichProcessFunction extends ProcessAllWindowFunction<StudentInfoModel, String, TimeWindow> {
    private static final String CLASS_IN = "class_in";
    private static final String SPEAK = "speak";
    private static final String ANSWER = "answer";
    private static final Long WISH_STUDENT_AMOUNT = 10L;
    private MapState<String, StudentInfoModel> studentLearnMapState;

    @Override
    public void open(Configuration parameters) throws Exception { // 注册状态
// MapState :key是一个唯一的值，value是接收到的相同的key对应的value的值
        MapStateDescriptor<String, StudentInfoModel> descriptor = new MapStateDescriptor<String, StudentInfoModel>(
                "studentLearn", // 状态的名字
                String.class, StudentInfoModel.class
        );
// 状态存储的数据类型
        studentLearnMapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void process(ProcessAllWindowFunction<StudentInfoModel, String, TimeWindow
            >.Context context, Iterable<StudentInfoModel> elements, Collector<String> collector) throws Exception {
//System.out.println("name:" + name);
// 遍历本次窗口中的所有数据
        for (StudentInfoModel studentInfoModel : elements) {
// 先把本次窗口内新增数据更新到状态中
            updateStudentInfo(studentInfoModel);
        }
        Date date = new Date();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String curTime = df.format(date);
// 计算整个课程的数据和学生的数据
        long realStudentAmount = 0;
        long speakStudentAmount = 0;
        long answerStudentAmount = 0;
        for (String name : studentLearnMapState.keys()) {
            StudentInfoModel studentInfoState = studentLearnMapState.get(name);
// 计算单个学生的数据并推送
            statStudentLearnData(name, studentInfoState, curTime, collector);
// 计算整个课程的数据
            //    System.out.println("index: "+index+" name:"+name+" studentInfoStIterable<StudentInfoModel>elements,Collector<String> collectate:" + studentInfoState.toString());
            realStudentAmount += 1;
            if (studentInfoState.getSpeakAmount() > 0) {
// 表示该学生有过发言
                speakStudentAmount += 1;
            }
            if (studentInfoState.getAnswerAmount() > 0) { // 学生有过答题
                answerStudentAmount += 1;
            }
        }
        if (realStudentAmount > 0) {
            double LearnStudentRate = 100 * realStudentAmount / WISH_STUDENT_AMOUNT;
            double speakStudentRate = 100 * speakStudentAmount / realStudentAmount;
            double answerStudentRate = 100 * answerStudentAmount / realStudentAmount;
            String courseDataSummary = "【课堂数据】时间:" + curTime
                    + " 应到课人数:" + WISH_STUDENT_AMOUNT + " 教室的人数:" + realStudentAmount
                    + " 到课率:" + LearnStudentRate + "%" + " 互动发言人数:" + speakStudentAmount + " 发言率:" + speakStudentRate + "%" + " 答题人数:" + answerStudentAmount
                    + " 答题率:" + answerStudentRate + "%";
            collector.collect(courseDataSummary);
        }
    }

    private void statStudentLearnData(String name, StudentInfoModel studentInfoState, String curTime, Collector<String> collector) throws Exception {
        StudentInfoModel stundentInfoState = studentLearnMapState.get(name);
        String studentLearnDataSummary = "【学生数据】时间:" + curTime + " 姓名:" + name
                + " 到课时长:" + stundentInfoState.getLearnLength() / 1000 + " 发言次数:" + stundentInfoState.getSpeakAmount()
                + " 答题次数:" + stundentInfoState.getAnswerAmount();
// 把学生的数据写到下游
        collector.collect(studentLearnDataSummary);
    }

    private void updateStudentInfo(StudentInfoModel studentInfoModel) throws Exception {
        String name = studentInfoModel.getName();
        StudentInfoModel studentInfoState = studentLearnMapState.get(name);
        System.out.println("updateStudentInfo:" + studentInfoModel.toString());
        String eventId = studentInfoModel.getEventId();
// 当前这个学生没有历史状态数据，直接更新进去
        if (studentInfoState == null) {
            studentInfoState = new StudentInfoModel();
            if (CLASS_IN.equals(eventId)) {
                long learnLength = new Date().getTime() - Long.parseLong(studentInfoModel.getTime());
                studentInfoState.setLearnLength(learnLength);
            } else if (SPEAK.equals(eventId)) {
                studentInfoState.setSpeakAmount(1L);
            } else if (ANSWER.equals(eventId)) {
                studentInfoState.setAnswerAmount(1L);
            } else {
                if (CLASS_IN.equals(eventId)) {
                    long learnLength = new Date().getTime() - Long.parseLong(studentInfoModel.getTime());
                    studentInfoState.setLearnLength(learnLength + studentInfoState.getLearnLength());
                } else if (SPEAK.equals(eventId)) {
                    studentInfoState.setSpeakAmount(1L + studentInfoState.getSpeakAmount());
                } else if (ANSWER.equals(eventId)) {
                    studentInfoState.setAnswerAmount(1L + studentInfoState.getAnswerAmount());
                }
            }
// 更新状态
            studentLearnMapState.put(name, studentInfoState);
        }
    }
}
