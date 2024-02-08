A simple java-based test harness to try out different jdbc/statement interactions between pgjdbc and postgres

To run the harness:
```
$ DB=postgres PORT=5432 java -jar \
  -Djava.util.logging.config.file=src/main/resources/logging.properties \
  target/pgjdbc-transactions-0.1.0.jar
```

The `DB` param takes one of (currently) two values: `postgres` or `mysql`
The `PORT` param allow you to pass the local port of the `DB` type. Making this a param allows you to easily swap between calling the upstream directly ot calling readyset.

The logging `-D` jvm arg allows dumping of the pgjdbc [logs](https://jdbc.postgresql.org/documentation/logging/) (super helpful)
