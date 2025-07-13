- cmd to create the maven project
`
mvn archetype:generate \
  -DgroupId=com.example \
  -DartifactId=kafka-streams-aggregator \
  -DarchetypeArtifactId=maven-archetype-quickstart \
  -DinteractiveMode=false
`

- cmd to install libraries
`
mvn clean install
`

- start project
`
mvn exec:java -Dexec.mainClass="com.example.AggregatorApp"
`
or just
`
mvn exec:java"
`