import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;

public class main {

    public static void main(String[] args){

        if(args.length != 3){
            System.out.println("Please input keytab location, principal, namenode location");
            System.exit(1);
        }

        String keyTab = args[0];
        String principal = args[1];
        String namenode = args[2];

        System.out.println("Connecting to namenode" + namenode + " using principal " + principal + " with password Keytab" + keyTab);

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", namenode);
        conf.set("hadoop.security.authentication", "kerberos");

        UserGroupInformation.setConfiguration(conf);
        try{
            UserGroupInformation.loginUserFromKeytab(principal,keyTab);
        }catch (FileNotFoundException e2){
            System.err.println("Caught FileNotFoundException: " + e2.getMessage());
        }
        catch (IOException e){
            System.err.println("Caught IOException: " + e.getMessage());
        }


        try{
            FileSystem fs;
            fs = FileSystem.get(conf);
            FileStatus[] fsStatus = fs.listStatus(new Path("/"));
            for(int i = 0; i < fsStatus.length; i++){
                System.out.println(fsStatus[i].getPath().toString());
            }
        }catch (FileNotFoundException e2){
            System.err.println("Caught FileNotFoundException: " + e2.getMessage());
        }
        catch (IOException e){
            System.err.println("Caught IOException: " + e.getMessage());
        }
    }
}
