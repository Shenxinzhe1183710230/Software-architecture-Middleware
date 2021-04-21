import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import database.DBBean;

public class middleware {
    private ServerSocket serversocket = null;
    private Executor executor = Executors.newCachedThreadPool();//线程池;
    private HashMap<String, ReceiveRunnable> consumer_hashMap = new HashMap();//存储着当前连接着的所有的consumer的socket



    public middleware() {
        try {
            //创建服务端套接字，之后等待客户端连接
            serversocket = new ServerSocket(8888);
            while (!serversocket.isClosed()) {
                Socket acceptedSocket_tmp = serversocket.accept();
                System.out.println("连接成功");
                //启动接收客户端数据的线程
                executor.execute(new ReceiveRunnable(acceptedSocket_tmp));
                Thread.sleep(1000);
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (serversocket != null) {
                try {
                    serversocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    //服务端接收数据线程
    private class ReceiveRunnable implements Runnable {
        private Socket mySocket;
        private DataInputStream dataInput;
        private DataOutputStream dataOutPut;
        private String message_type;//从收到的Message中提取出消息类型
        private String sender_name;//从收到的Message中提取出发送者的名字
        private Boolean send_little_flag = true;
        private MyMessage myMessage;

        public ReceiveRunnable(Socket s) throws IOException {
            this.mySocket = s;
            //客户端接收服务端发送的数据的缓冲区
            dataInput = new DataInputStream(
                    s.getInputStream());
            dataOutPut = new DataOutputStream(mySocket.getOutputStream());
        }

        public void setMyMessage(MyMessage myMessage) {
            this.myMessage = myMessage;
        }

        @Override
        public void run() {
            while (!mySocket.isClosed()) {
                try {
                    int size = 0;
                    try {
                        //接收数据
                        size = dataInput.readInt();//获取服务端发送的数据的大小
                    } catch (IOException e) {
                        e.printStackTrace();
                        mySocket.close();
                    }
                    byte[] data = new byte[size];
                    int len = 0;
                    //将二进制数据写入data数组
                    while (len < size) {
                        len += dataInput.read(data, len, size - len);
                    }
                    MyMessage message = analysis_receiveData(data);
                    sender_name = message.getSenderName();
                    message_type = message.getMessage_type();
                    if (message_type.equals("consumer_ask")) {
                        consumer_hashMap.put(sender_name, this);
                        System.out.println(sender_name + "_consumer已连接");
                        //为了保持连接 所以每次发一个小的字符串
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                while (send_little_flag && !mySocket.isClosed()) {
                                    sendMessage(new MyMessage(sender_name + "littleString", "", ""));
                                    try {
                                        Thread.sleep(2000);
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }).start();
                        continue;
                    }
                    else if(message_type.equals("producer_send_database")){
                        Object o = operateDataBase(message);
                        if(o instanceof Integer){
                            int value = ((Integer) o).intValue();
                            System.out.println(value);
                        }
                        else {
                            ResultSet rs = (ResultSet) o;
                            ResultSetMetaData resultSetMetaData = rs.getMetaData();
                            int ColumnCount = resultSetMetaData.getColumnCount();
                            while (rs.next()) {
                                for (int i = 0; i < ColumnCount; i++) {
                                    String s = rs.getString(1+i);
                                    System.out.print(s+",");
                                }
                                System.out.println();
                            }
                        }
                    }
                    else if(message_type.equals("consumer_topic")){
                        getTopic(message);
                    }
                    else if(message_type.equals("producer_topic")){
                        String object = message.getMessage_content();
                        File file = new File("src\\txt\\好友\\"+object+"的topic注册列表.txt");
                        file.createNewFile();
                    }
                    else if (message_type.equals("producer_topic_update")) {
                        File file = new File("src\\txt\\好友\\" + sender_name + "的topic注册列表.txt");
                        FileInputStream fileInputStream = new FileInputStream(file);
                        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                        String text_line;
                        while ((text_line = bufferedReader.readLine()) != null) {
                            System.out.println( text_line);
                            if (consumer_hashMap.get(text_line)!=null){
                                consumer_hashMap.get(text_line).sendMessage(new MyMessage(sender_name,"producer_message",message.getMessage_content()));
                            }
                        }
                    }
//                    BufferedWriter out = new BufferedWriter(new FileWriter("src\\txt\\" + sender_name + "_message.txt"));
//                    out.write(message.getMessage_content());
//                    out.close();
                } catch (IOException | SQLException e) {
                    e.printStackTrace();
                }
            }
            try {
                mySocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * 处理发来的topic
         * @param message 发来的topic
         */

        private void getTopic(MyMessage message){
            String sender = message.getSenderName();
            String object = message.getMessage_content();
            System.out.println("sender:"+sender);
            System.out.println("object:"+object);
            boolean b=true;
            File file = new File("src\\txt\\好友\\"+object+"的topic注册列表.txt");
            if(!file.exists()){
                b = false;
                sendMessage(new MyMessage("middleware", "error", "no topic"));
            }
            else {
                try {
                    FileInputStream  fileInputStream = new FileInputStream(file);
                    InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                    BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                    String text = null;
                    while((text = bufferedReader.readLine()) != null){
                        System.out.println("text:"+text);
                        if(text.equals(sender)){
                            System.out.println(object+"已和"+sender+"注册了关系");
                            b=false;
                            break;
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if(b){
                try {
                    String s = sender +"\r\n";
                    byte[] buff=s.getBytes();
                    FileOutputStream o=new FileOutputStream(file,true);
                    o.write(buff);
                    o.flush();
                    o.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * 发送消息
         *
         * @param message 发送的消息
         */
        private void sendMessage(MyMessage message) {
            try {
//                System.out.println("准备发送");
                byte[] tmp = message.getBytes();
                dataOutPut.writeInt(tmp.length);
                dataOutPut.write(tmp, 0, tmp.length);
                dataOutPut.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        /**
         * 解析收到的数据   构造MyMessage类型的对象
         *
         * @param data 收到的数据
         * @return MyMessage
         */
        private MyMessage analysis_receiveData(byte[] data) {
            String result = new String(data);
            String[] senderName = result.split("\\$");
            String senderNameString = senderName[0];
            String[] message_type = senderName[1].split("\\#");
            String message_typeString = null;
            String message_content = null;
            if (message_type.length == 1) {
                message_typeString = message_type[0];
                message_content = "";
            } else if (message_type.length == 2) {
                message_typeString = message_type[0];
                message_content = message_type[1];
            }
            return new MyMessage(senderNameString, message_typeString, message_content);
        }
    }

    /**
     * 分析并运行MyMessage里要求的函数
     * @param message 传入的信息
     * @return 运行函数得到的结果
     */
    //@TODO 陈文韬
    private Object operateDataBase(MyMessage message){
        String message_content = message.getMessage_content();
        String[] operate = message_content.split("%");
        String[] funcation = operate[1].split("\\(|\\)|,");
        ResultSet resultSet;
        int answer;
        DBBean dbBean = new DBBean();
        switch (funcation[0]){
            case "executeDelete":
                answer = dbBean.executeDelete(funcation[1], funcation[2], funcation[3]);
                return answer;
            case "executeUpdate":
                answer = dbBean.executeUpdate(funcation[1], funcation[2], funcation[3], funcation[4], funcation[5]);
                return answer;
            case "executeFind":
                resultSet = dbBean.executeFind(funcation[1], funcation[2], funcation[3]);
                return resultSet;
            case "executeFindAll":
                resultSet = dbBean.executeFindAll(funcation[1]);
                return resultSet;
            case "executeQuery":
                answer = dbBean.executeQuery(funcation[1], funcation[2]);
                return answer;
            case "executeFindMAXID":
                resultSet = dbBean.executeFindMAXID(funcation[1], funcation[2]);
                return resultSet;
            default:
                return null;
        }
    }

    public static void main(String[] args) {
        new middleware();
    }

}
