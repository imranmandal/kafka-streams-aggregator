- cmd to create the maven project
  `mvn archetype:generate \
-DgroupId=com.kazam_dlg_ingest \
-DartifactId=kafka-streams-aggregator \
-DarchetypeArtifactId=maven-archetype-quickstart \
-DinteractiveMode=false`

- cmd to install libraries
  `mvn clean install`

<!-- - start project
  `mvn exec:java -Dexec.mainClass="com.kazam_dlg_ingest.AggregatorApp"`
  or just
  `mvn exec:java` -->

- start main project
  `mvn exec:java -Pmain"`

- start test project
  `mvn exec:java -Ptest`
