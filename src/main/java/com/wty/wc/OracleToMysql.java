package com.wty.wc;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class OracleToMysql {
    public static void main(String[] args) {
        String oraTable = " CREATE TABLE ora_products (\n" +
                "     ID INT NOT NULL,\n" +
                "     NAME STRING,\n" +
                "     DESCRIPTION STRING,\n" +
                "     WEIGHT DECIMAL(10, 3),\n" +
                "     PRIMARY KEY(ID) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'scan.startup.mode'='latest-offset',\n" +
                "     'connector' = 'oracle-cdc',\n" +
                "     'hostname' = '192.168.12.161',\n" +
                "     'port' = '1522',\n" +
                "     'username' = 'c##flinkuser',\n" +
                "     'password' = 'flinkpw',\n" +
                "     'database-name' = 'ORCL',\n" +
                "     'schema-name' = 'c##flinkuser',\n" +
                "     'table-name' = 'products')";

        String mysqlTable = "CREATE TABLE mysql_products (\n" +
                "     ID INT NOT NULL,\n" +
                "     NAME STRING,\n" +
                "     DESCRIPTION STRING,\n" +
                "     WEIGHT DECIMAL(10),\n" +
                "     PRIMARY KEY(ID) NOT ENFORCED\n" +
                "     )  WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://192.168.12.161:3306/flink?serverTimezone=UTC',\n" +
                "   'table-name' = 'products',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '123456')";

        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(bsSettings);

        tableEnv.executeSql(oraTable);

        tableEnv.executeSql(mysqlTable);

        String sql = "insert into mysql_products(ID,NAME,DESCRIPTION,WEIGHT) select ID,NAME,DESCRIPTION,WEIGHT FROM ora_products";

        tableEnv.executeSql(sql);
    }
}
