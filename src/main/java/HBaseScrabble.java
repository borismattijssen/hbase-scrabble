import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class HBaseScrabble {
    private Configuration config;
    private HBaseAdmin hBaseAdmin;

    /**
     * The Constructor. Establishes the connection with HBase.
     * @param zkHost
     * @throws IOException
     */
    public HBaseScrabble(String zkHost) throws IOException {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkHost.split(":")[0]);
        config.set("hbase.zookeeper.property.clientPort", zkHost.split(":")[1]);
        HBaseConfiguration.addHbaseResources(config);
        this.hBaseAdmin = new HBaseAdmin(config);
    }

    public void createTable() throws IOException {
        HTableDescriptor table = new HTableDescriptor(Bytes.toBytes("Games"));
        HColumnDescriptor infoFamily = new HColumnDescriptor(Bytes.toBytes("d"));
        HColumnDescriptor winnerFamily = new HColumnDescriptor(Bytes.toBytes("w"));
        HColumnDescriptor loserFamily = new HColumnDescriptor(Bytes.toBytes("l"));
        table.addFamily(infoFamily);
        table.addFamily(winnerFamily);
        table.addFamily(loserFamily);
        hBaseAdmin.createTable(table);
    }

    public void loadTable(String folder)throws IOException{
        List<Path> files = Files.walk(Paths.get(folder))
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());

        HConnection conn = HConnectionManager.createConnection(this.config);
        HTable table = new HTable(TableName.valueOf("Games"),conn);

        List<Put> puts = new ArrayList<>();

        byte[] infoFamily = Bytes.toBytes("d");
        byte[] winnerFamily = Bytes.toBytes("w");
        byte[] loserFamily = Bytes.toBytes("l");

        for(Path file : files) {
            Reader reader = Files.newBufferedReader(file);
            CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader());
            for (CSVRecord csvRecord : csvParser) {
                ArrayList<String> values = new ArrayList<>();
                csvRecord.iterator().forEachRemaining(values::add);
                byte[] key = getKey(values.toArray(new String[0]), new int[]{0,1});
                Put put = new Put(key);

                put.add(infoFamily, Bytes.toBytes("gid"), Bytes.toBytes(csvRecord.get(0)));
                put.add(infoFamily, Bytes.toBytes("tid"), Bytes.toBytes(csvRecord.get(1)));
                put.add(infoFamily, Bytes.toBytes("tie"), Bytes.toBytes(csvRecord.get(2)));
                put.add(infoFamily, Bytes.toBytes("rnd"), Bytes.toBytes(csvRecord.get(15)));
                put.add(infoFamily, Bytes.toBytes("div"), Bytes.toBytes(csvRecord.get(16)));
                put.add(infoFamily, Bytes.toBytes("date"), Bytes.toBytes(csvRecord.get(17)));
                put.add(infoFamily, Bytes.toBytes("lex"), Bytes.toBytes(csvRecord.get(18)));

                put.add(winnerFamily, Bytes.toBytes("id"), Bytes.toBytes(csvRecord.get(3)));
                put.add(winnerFamily, Bytes.toBytes("name"), Bytes.toBytes(csvRecord.get(4)));
                put.add(winnerFamily, Bytes.toBytes("score"), Bytes.toBytes(csvRecord.get(5)));
                put.add(winnerFamily, Bytes.toBytes("or"), Bytes.toBytes(csvRecord.get(6)));
                put.add(winnerFamily, Bytes.toBytes("nr"), Bytes.toBytes(csvRecord.get(7)));
                put.add(winnerFamily, Bytes.toBytes("pos"), Bytes.toBytes(csvRecord.get(8)));

                put.add(loserFamily, Bytes.toBytes("id"), Bytes.toBytes(csvRecord.get(9)));
                put.add(loserFamily, Bytes.toBytes("name"), Bytes.toBytes(csvRecord.get(10)));
                put.add(loserFamily, Bytes.toBytes("score"), Bytes.toBytes(csvRecord.get(11)));
                put.add(loserFamily, Bytes.toBytes("or"), Bytes.toBytes(csvRecord.get(12)));
                put.add(loserFamily, Bytes.toBytes("nr"), Bytes.toBytes(csvRecord.get(13)));
                put.add(loserFamily, Bytes.toBytes("pos"), Bytes.toBytes(csvRecord.get(14)));


                puts.add(put);
            }
        }
        table.put(puts);
    }

    /**
     * This method generates the key
     * @param values The value of each column
     * @param keyTable The position of each value that is required to create the key in the array of values.
     * @return The encoded key to be inserted in HBase
     */
    private byte[] getKey(String[] values, int[] keyTable) {
        String[] keyValues = new String[keyTable.length];
        for (int i = 0; i < keyTable.length; i++) {
           keyValues[i] = values[keyTable[i]];
        }
        byte[] key = Bytes.toBytes(String.join(":", keyValues));

        return key;
    }



    public List<String> query1(String tourneyid, String winnername) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;

    }

    public List<String> query2(String firsttourneyid, String lasttourneyid) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;
    }

    public List<String> query3(String tourneyid) throws IOException {
        //TO IMPLEMENT
        System.exit(-1);
        return null;
    }


    public static void main(String[] args) throws IOException {
        if(args.length<2){
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }
        HBaseScrabble hBaseScrabble = new HBaseScrabble(args[0]);
        if(args[1].toUpperCase().equals("CREATETABLE")){
            hBaseScrabble.createTable();
        }
        else if(args[1].toUpperCase().equals("LOADTABLE")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)action [createTables, loadTables], 3)csvsFolder");
                System.exit(-1);
            }
            else if(!(new File(args[2])).isDirectory()){
                System.out.println("Error: Folder "+args[2]+" does not exist.");
                System.exit(-2);
            }
            hBaseScrabble.loadTable(args[2]);
        }
        else if(args[1].toUpperCase().equals("QUERY1")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query1, " +
                        "3) tourneyid 4) winnername");
                System.exit(-1);
            }

            List<String> opponentsName = hBaseScrabble.query1(args[2], args[3]);
            System.out.println("There are "+opponentsName.size()+" opponents of winner "+args[3]+" that play in tourney "+args[2]+".");
            System.out.println("The list of opponents is: "+Arrays.toString(opponentsName.toArray(new String[opponentsName.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY2")){
            if(args.length!=4){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2)query2, " +
                        "3) firsttourneyid 4) lasttourneyid");
                System.exit(-1);
            }
            List<String> playerNames =hBaseScrabble.query2(args[2], args[3]);
            System.out.println("There are "+playerNames.size()+" players that participates in more than one tourney between tourneyid "+args[2]+" and tourneyid "+args[3]+" .");
            System.out.println("The list of players is: "+Arrays.toString(playerNames.toArray(new String[playerNames.size()])));
        }
        else if(args[1].toUpperCase().equals("QUERY3")){
            if(args.length!=3){
                System.out.println("Error: 1) ZK_HOST:ZK_PORT, 2) query3, " +
                        "3) tourneyid");
                System.exit(-1);
            }
            List<String> games = hBaseScrabble.query3(args[2]);
            System.out.println("There are "+games.size()+" that ends in tie in tourneyid "+args[2]+" .");
            System.out.println("The list of games is: "+Arrays.toString(games.toArray(new String[games.size()])));
        }
        else{
            System.out.println("Error: \n1)ZK_HOST:ZK_PORT, \n2)action [createTable, loadTable, query1, query2, query3], \n3)Extra parameters for loadTables and queries:\n" +
                    "\ta) If loadTable: csvsFolder.\n " +
                    "\tb) If query1: tourneyid winnername.\n  " +
                    "\tc) If query2: firsttourneyid lasttourneyid.\n  " +
                    "\td) If query3: tourneyid.\n  ");
            System.exit(-1);
        }

    }



}
