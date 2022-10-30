package ca.sunlife.model.versiontwowithjackson;

import ca.sunlife.model.*;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class VersionTwoWithJackson {

    private static final Logger logger = Logger.getLogger(VersionTwoWithJackson.class.getName());
    private static final String TOPIC_NAME = "topicName";
    private static final String SCHEMA_FILE = "schemaFile";
    private static final String SCHEMA_FORMAT = "schemaFormat";
    private static final String CONSUMER_GROUP = "consumerGroup";
    private static final String READ_TOPIC = "readTopic";
    private static final String WRITE_TOPIC = "writeTopic";
    private static final String READ_SCHEMA = "readSchema";
    private static final String WRITE_SCHEMA = "writeSchema";
    private static final String TOPIC = "topic";
    private static final String GROUP = "Group:";
    private static final String DATA = " Data ";
    private static final String ALREADY_READ = "ALREADY_READ";
    private static final String DLQ = "dlq";
    private static final String PROJECT = "project";
    private static final String AUDIT = "audit";

    public void convertToYaml(String plan) throws IOException {
        FileReader fileReader = new FileReader("C:/YamlTopology/input/cxoclient.txt");

        try (BufferedReader bufferedReader = new BufferedReader(fileReader)) {

            Parameter parameter = new Parameter();
            List<String> lines = new ArrayList<>();
            List<Topic> topicList;
            List<Object> projectList = new ArrayList<>();
            List<ReadTopic> readTopicList;
            List<WriteTopic> writeTopicList;
            List<ReadSchema> readSchemaList;
            List<WriteSchema> writeSchemaList;
            Map<String, String> topicData = new HashMap<>();
            Map<String, Map<String, String>> topics = new HashMap<>();

            String readLine;

            while ((readLine = bufferedReader.readLine()) != null) {
                lines.add(readLine);
            }

            logger.log(Level.DEBUG, "Debugging");
            int index = 0;
            int topicCount = 0;
            while (index < lines.size()) {
                String line = lines.get(index);
                logger.log(Level.INFO, "Reading Line- " + line);
                String[] splitLine = line.split(":");
                switch (splitLine[0]) {
                    case TOPIC_NAME:
                        topicData.put(splitLine[0], splitLine[1]);
                        topicCount++;
                        break;
                    case SCHEMA_FILE:
                    case SCHEMA_FORMAT:
                    case CONSUMER_GROUP:
                    case READ_TOPIC:
                    case WRITE_TOPIC:
                    case READ_SCHEMA:
                    case WRITE_SCHEMA:
                    case AUDIT:
                        topicData.put(splitLine[0], splitLine[1]);
                        break;
                    case DLQ:
                        topicData.put(splitLine[0], splitLine[1]);
                        topics.put(PROJECT + topicCount, topicData);
                        topicData = new HashMap<>();
                        index++;
                        continue;
                    default:
                        index++;
                        break;
                }
                index++;
            }

            topicCount = 1;


            String[] splitParameterName = topics.get(PROJECT + topicCount).get(TOPIC_NAME).split("\\.");


            for (int i = 1; i <= topics.size(); i++) {

                if (topics.get(PROJECT + i).getOrDefault(ALREADY_READ, "NONE").equalsIgnoreCase("Y")) continue;
                topicList = new ArrayList<>();
                readTopicList = new ArrayList<>();
                writeTopicList = new ArrayList<>();
                readSchemaList = new ArrayList<>();
                writeSchemaList = new ArrayList<>();
                String[] splitName = topics.get(PROJECT + i).get(TOPIC_NAME).split("\\.");
                topics.get(PROJECT + i).put(ALREADY_READ, "Y");
                String dlq = topics.get(PROJECT + i).get(DLQ);
                Project project = new Project();
                Topic topic = new Topic();
                ReadTopic readTopic = new ReadTopic();
                WriteTopic writeTopic = new WriteTopic();
                ReadSchema readSchema = new ReadSchema();
                WriteSchema writeSchema = new WriteSchema();

                //Setting up Project Name
                if (splitName.length == 6) {
                    project.setName(splitName[3]);
                } else if (splitName.length == 8) {
                    project.setName(splitName[3] + "." + splitName[4] + "." + splitName[5]);
                } else {
                    project.setName(splitName[3] + "." + splitName[4]);
                }

                String setDlq = topics.get(PROJECT + i).get(TOPIC_NAME);
                if (dlq.equals("yes")) {
                    topic.setDataType(setDlq.substring(setDlq.lastIndexOf("0") + 1));
                } else {
                    topic.setDataType("0");
                }

                if (splitName.length <= 6) {
                    topic.setName(splitName[4]);
                } else if (splitName.length == 8) {
                    topic.setName(splitName[4]);
                } else {
                    topic.setName(splitName[5]);
                }
                topic.setPlan(plan);
                Schemas schemas = new Schemas();
                Value value = new Value();
                Schema schema = new Schema();

                schema.setFile(topics.get(PROJECT + i).get(SCHEMA_FILE));

                value.setSchema(schema);
                value.setFormat(topics.get(PROJECT + i).get(SCHEMA_FORMAT));

                schemas.setValue(value);

                topic.setSchemas(schemas);

                // Setting up values in ReadTopic
                String audit = topics.get(PROJECT + i).getOrDefault("audit", "NONE");
                if (audit.equals("yes")) {
                    readTopic.setPrincipal(GROUP + topics.get(PROJECT + i).get(READ_TOPIC));
                    readTopic.setTopic(topics.get(PROJECT + i).get(TOPIC_NAME));
                    readTopic.setGroup(topics.get(PROJECT + i).get(CONSUMER_GROUP));
                    readTopicList.add(readTopic);
                    readTopic = new ReadTopic();
                    readTopic.setPrincipal(GROUP + topics.get(PROJECT + i).get(READ_TOPIC));
                    readTopic.setTopic(splitName[0] + ".ent.all.kafka.audit.0");
                }else {
                    readTopic.setPrincipal(GROUP + topics.get(PROJECT + i).get(READ_TOPIC));
                    readTopic.setTopic(topics.get(PROJECT + i).get(TOPIC_NAME));
                    readTopic.setGroup(topics.get(PROJECT + i).get(CONSUMER_GROUP));
                }


                // Setting up values in writeTopic
                if (audit.equals("yes")) {
                    writeTopic.setPrincipal(GROUP + topics.get(PROJECT + i).get(WRITE_TOPIC));
                    writeTopic.setTopic(topics.get(PROJECT + i).get(TOPIC_NAME));
                    writeTopic.setGroup(topics.get(PROJECT + i).get(CONSUMER_GROUP));
                    writeTopicList.add(writeTopic);
                    writeTopic = new WriteTopic();
                    writeTopic.setPrincipal(GROUP + topics.get(PROJECT + i).get(WRITE_TOPIC));
                    writeTopic.setTopic(splitName[0] + ".ent.all.kafka.audit.0");
                } else {
                    writeTopic.setPrincipal(GROUP + topics.get(PROJECT + i).get(WRITE_TOPIC));
                    writeTopic.setTopic(topics.get(PROJECT + i).get(TOPIC_NAME));
                    writeTopic.setGroup(topics.get(PROJECT + i).get(CONSUMER_GROUP));
                }



                // Setting up variables in readSchema
                readSchema.setPrincipal(GROUP + topics.get(PROJECT + i).get(READ_SCHEMA));
                readSchema.setSubject(topics.get(PROJECT + i).get(TOPIC_NAME));

                //Setting up variables in writeSchema
                writeSchema.setPrincipal(GROUP + topics.get(PROJECT + i).get(WRITE_SCHEMA));
                writeSchema.setSubject(topics.get(PROJECT + i).get(TOPIC_NAME));

                logger.log(Level.INFO, TOPIC + topicCount + DATA + topic);
                logger.log(Level.INFO, READ_TOPIC + topicCount + DATA + readTopic);
                logger.log(Level.INFO, WRITE_TOPIC + topicCount + DATA + writeTopic);
                logger.log(Level.INFO, READ_SCHEMA + topicCount + DATA + readSchema);
                logger.log(Level.INFO, WRITE_SCHEMA + topicCount + DATA + writeSchema);

                topicList.add(topic);
                readTopicList.add(readTopic);
                writeTopicList.add(writeTopic);
                readSchemaList.add(readSchema);
                writeSchemaList.add(writeSchema);

                for (int j = i + 1; j <= topics.size(); j++) {
                    topic = new Topic();
                    readTopic = new ReadTopic();
                    writeTopic = new WriteTopic();
                    readSchema = new ReadSchema();
                    writeSchema = new WriteSchema();
                    if (topics.get(PROJECT + j).getOrDefault(ALREADY_READ, "NONE").equalsIgnoreCase("Y")) continue;
                    String[] splitVariable = topics.get(PROJECT + j).get(TOPIC_NAME).split("\\.");

                    String sameName;
                    if (splitName.length == 6) {
                        sameName = splitName[3];
                    } else if (splitName.length == 7) {
                        sameName = splitName[4];
                    } else if (splitName.length == 8) {
                        sameName = splitName[3] + "." + splitName[4] + "." + splitName[5];
                    } else {
                        sameName = splitName[3] + "." + splitName[4];
                    }


                    if (topics.get(PROJECT + j).get(TOPIC_NAME).contains(sameName)) {
                        topics.get(PROJECT + j).put(ALREADY_READ, "Y");
                        dlq = topics.get(PROJECT + j).get(DLQ);

                        setDlq = topics.get(PROJECT + j).get(TOPIC_NAME);
                        if (dlq.equals("yes")) {
                            topic.setDataType("0" + setDlq.substring(setDlq.lastIndexOf("0") + 1));
                        } else {
                            topic.setDataType("0");
                        }

                        if (splitVariable.length <= 6) {
                            topic.setName(splitVariable[4]);
                        } else if (splitVariable.length == 8) {
                            topic.setName(splitVariable[4]);
                        } else {
                            topic.setName(splitVariable[5]);
                        }
                        topic.setPlan(plan);
                        schemas = new Schemas();
                        value = new Value();
                        schema = new Schema();

                        schema.setFile(topics.get(PROJECT + j).get(SCHEMA_FILE));

                        value.setSchema(schema);
                        value.setFormat(topics.get(PROJECT + j).get(SCHEMA_FORMAT));

                        schemas.setValue(value);

                        topic.setSchemas(schemas);

                        // Setting up values in ReadTopic
                        audit = topics.get(PROJECT + j).getOrDefault("audit", "NONE");
                        if (audit.equals("yes")) {
                            readTopic.setPrincipal(GROUP + topics.get(PROJECT + j).get(READ_TOPIC));
                            readTopic.setTopic(topics.get(PROJECT + j).get(TOPIC_NAME));
                            readTopic.setGroup(topics.get(PROJECT + j).get(CONSUMER_GROUP));
                            readTopicList.add(readTopic);
                            readTopic = new ReadTopic();
                            readTopic.setPrincipal(GROUP + topics.get(PROJECT + j).get(READ_TOPIC));
                            readTopic.setTopic(splitName[0] + ".ent.all.kafka.audit.0");
                        }else {
                            readTopic.setPrincipal(GROUP + topics.get(PROJECT + j).get(READ_TOPIC));
                            readTopic.setTopic(topics.get(PROJECT + j).get(TOPIC_NAME));
                            readTopic.setGroup(topics.get(PROJECT + j).get(CONSUMER_GROUP));
                        }

                        // Setting up values in writeTopic
                        if (audit.equals("yes")) {
                            writeTopic.setPrincipal(GROUP + topics.get(PROJECT + j).get(WRITE_TOPIC));
                            writeTopic.setTopic(topics.get(PROJECT + j).get(TOPIC_NAME));
                            writeTopicList.add(writeTopic);
                            writeTopic = new WriteTopic();
                            writeTopic.setPrincipal(GROUP + topics.get(PROJECT + j).get(WRITE_TOPIC));
                            writeTopic.setTopic(splitName[0] + "ent.all.kafka.audit.0");
                        } else {
                            writeTopic.setPrincipal(GROUP + topics.get(PROJECT + j).get(WRITE_TOPIC));
                            writeTopic.setTopic(topics.get(PROJECT + j).get(TOPIC_NAME));
                            writeTopic.setGroup(topics.get(PROJECT + j).get(CONSUMER_GROUP));
                        }


                        // Setting up variables in readSchema
                        readSchema.setPrincipal(GROUP + topics.get(PROJECT + j).get(READ_SCHEMA));
                        readSchema.setSubject(topics.get(PROJECT + j).get(TOPIC_NAME));

                        //Setting up variables in writeSchema
                        writeSchema.setPrincipal(GROUP + topics.get(PROJECT + j).get(WRITE_SCHEMA));
                        writeSchema.setSubject(topics.get(PROJECT + j).get(TOPIC_NAME));

                        logger.log(Level.INFO, TOPIC + topicCount + DATA + topic);
                        logger.log(Level.INFO, READ_TOPIC + topicCount + DATA + readTopic);
                        logger.log(Level.INFO, WRITE_TOPIC + topicCount + DATA + writeTopic);
                        logger.log(Level.INFO, READ_SCHEMA + topicCount + DATA + readSchema);
                        logger.log(Level.INFO, WRITE_SCHEMA + topicCount + DATA + writeSchema);

                        topicList.add(topic);
                        readTopicList.add(readTopic);
                        writeTopicList.add(writeTopic);
                        readSchemaList.add(readSchema);
                        writeSchemaList.add(writeSchema);
                    }
                }

                for (int k = 1; k < readTopicList.size(); k++) {
                    String group = readTopicList.get(0).getGroup();
                    if (group == null || group.isEmpty()) {
                        break;
                    }

                    if (group.equals(readTopicList.get(k).getGroup())) {
                        readTopicList.get(k).setGroup(null);
                    }
                }

                project.setTopics(topicList);
                project.setReadTopic(readTopicList);
                project.setWriteTopic(writeTopicList);
                project.setReadSchema(readSchemaList);
                project.setWriteSchema(writeSchemaList);

                logger.log(Level.INFO, "Project---> " + project);

                projectList.add(Project.convertToMap(project));
            }

            parameter.setContext(splitParameterName[0]);
            parameter.setRegion(splitParameterName[1]);
            parameter.setLob(splitParameterName[2]);
            parameter.setProjects(projectList);
            logger.log(Level.INFO, "Parameter---> " + parameter);

            File file = new File("./src/main/resources/opOne.yml");

            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            objectMapper.writeValue(file, parameter);
//            File schema = new File("./src/main/resources/schema");
//            schema.mkdir();

            System.out.println(topics);
        }

    }

    public static void main(String[] args) throws IOException {
        VersionTwoWithJackson v = new VersionTwoWithJackson();
        Scanner scanner = new Scanner(System.in);
        String plan = scanner.nextLine();
        v.convertToYaml(plan);
    }
}