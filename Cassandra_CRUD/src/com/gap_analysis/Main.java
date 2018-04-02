package com.gap_analysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class Main {

    public static void main(String[] args) {

        // 1. Connect the Database
        String ipAddress = "127.0.0.1";
        String keySpace = "perfmonitor";

        Cluster cluster;
        Session session;

        System.out.println("*******Connect Cassandra DB (ipAddr: " + ipAddress + " , keySpace: " + keySpace + ")");
        cluster = Cluster.builder().addContactPoint(ipAddress).build();

        session = cluster.connect(keySpace);


        // 2. Create a column family
        createTable(session);

        // 3. Insert data into the column family
        insertData(session);

        // 4. Select data from the column family
        selectData(session);

        // 5. Update data from the column family
        updateData(session);
        selectData(session);

        // 6. Delete data from the column family
        deleteData(session);
        selectData(session);

        // 7. Close the DB
        System.out.println("*******Disconnect Cassandra DB");
        session.close();
        cluster.close();


    }

    public static void createTable(Session session)
    {
        System.out.println("*******Query for Create a column family");

        final String TABLE_NAME = "realEstate";
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME).append("(")
                .append("policyID int, ")
                .append("statecode text, ")
                .append("county text, ")
                .append("point_latitude double, ")
                .append("point_longitude double, ")
                .append("line text, ")
                .append("construction text, ")
                .append("point_granularity int, ")
                .append("PRIMARY KEY(county, policyID)")
                .append(");");

        String query = sb.toString();
        session.execute(query);
    }

    public static void insertData(Session session)
    {
        System.out.println("*******Query for insert data");

        final String TABLE_NAME = "realEstate";
        String csvFile = "/home/jay/Documents/Gap Analysis/realEstate.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        try {
            br = new BufferedReader(new FileReader(csvFile));
            br.readLine();  // skip the first title row
            String query;
            while((line = br.readLine()) != null) {
                //use comma as separator
                String[] rowData = line.split(cvsSplitBy);

                StringBuilder sb = new StringBuilder("INSERT INTO ")
                        .append(TABLE_NAME).append("(")
                        .append("policyID, statecode, county, point_latitude, point_longitude, ")
                        .append("line, construction, point_granularity) ")
                        .append("VALUES (")
                        .append(rowData[0] +",'"+rowData[1]+"','"+rowData[2]+"',"+rowData[3]+","+rowData[4]+",'")
                        .append(rowData[5]+"','"+rowData[6]+"',"+rowData[7]+");");

                query = sb.toString();
                session.execute(query);

            }
        } catch(FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(br != null) {
                try {
                    br.close();
                } catch(IOException e) {
                    e.printStackTrace();
                }
            }

        }

    }

    public static void selectData(Session session)
    {
        System.out.println("/n/n*******Query for select all data");

        final String TABLE_NAME = "realEstate";
        StringBuilder sb = new StringBuilder("SELECT * FROM ")
                .append(TABLE_NAME)
                .append(";");

        String query = sb.toString();
        ResultSet resultSet = session.execute(query);

        for(Row row : resultSet) {
            System.out.format("%d, %s, %s, %f, %f, %s, %s, %d \n", row.getInt("policyID"), row.getString("statecode")
                    , row.getString("county"), row.getDouble("point_latitude")
                    , row.getDouble("point_longitude") , row.getString("line")
                    , row.getString("construction"), row.getInt("point_granularity"));
        }

        System.out.println("\n\n*******Query for select 'CLAY COUNTY' data");

        sb = new StringBuilder("SELECT * FROM ")
                .append(TABLE_NAME)
                .append(" WHERE county='CLAY COUNTY' and policyID > 0")
                .append(";");

        query = sb.toString();
        resultSet = session.execute(query);

        for(Row row : resultSet) {
            System.out.format("%d, %s, %s, %f, %f, %s, %s, %d \n", row.getInt("policyID"), row.getString("statecode")
                    , row.getString("county"), row.getDouble("point_latitude")
                    , row.getDouble("point_longitude") , row.getString("line")
                    , row.getString("construction"), row.getInt("point_granularity"));
        }

    }

    public static void updateData(Session session)
    {
        System.out.println("*******Query for update data");

        final String TABLE_NAME = "realEstate";
        StringBuilder sb = new StringBuilder("UPDATE ")
                .append(TABLE_NAME)
                .append(" SET construction = 'Frame'")
                .append(" WHERE county = 'CLAY COUNTY' and policyID = 995932")
                .append(";");

        String query = sb.toString();
        session.execute(query);
    }

    public static void deleteData(Session session)
    {
        System.out.println("*******Query for delete data");

        final String TABLE_NAME = "realEstate";
        StringBuilder sb = new StringBuilder("DELETE FROM ")
                .append(TABLE_NAME)
                .append(" WHERE county = 'CLAY COUNTY' and policyID > 0")
                .append(";");

        String query = sb.toString();
        session.execute(query);
    }


}