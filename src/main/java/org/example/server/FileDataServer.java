package org.example.server;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.ServerSocket;

public class FileDataServer {
    public static void main(String[] args) throws IOException {
        ServerSocket listener = new ServerSocket(9090);
        try{
            Socket socket = listener.accept();
            BufferedReader br = new BufferedReader(
                    new FileReader("/home/sitaru/IdeaProjects/Flink_intro/src/main/resources/cab_flink.txt"));
            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String line;
                while ((line = br.readLine()) != null){
                    out.println(line);
                    System.out.println(line);
                    Thread.sleep(5);
                }
            } finally{
                socket.close();
            }
        } catch(Exception e ){
            e.printStackTrace();
        } finally{
            listener.close();
        }
    }
}

