public class MyMessage {

    private String senderName;
    private String message_content;
    private String message_type;

    public MyMessage( String senderName, String message_type, String message_content){
        this.senderName = senderName;
        this.message_type = message_type;
        this.message_content = message_content;
    }

    /**
     * 将消息转成byte类型为了socket发送
     *
     * @return   senderName+$+消息类型+#+消息内容
     */
    public byte[] getBytes(){
        String allInformation = senderName;
        allInformation=allInformation+"$"+message_type;
        allInformation=allInformation+"#"+message_content;
        byte[] data = allInformation.getBytes();
        return data;
    }

    public String getMessage_type() {
        return message_type;
    }

    public String getSenderName() {
        return senderName;
    }

    public String getMessage_content() {
        return message_content;
    }

    public static void main(String[] args) {
        String sender = "发送者";
        String message_type="查询数据库";
        String message = "你好%f1(p1, p2);f2(p1, p2, p3)";
        MyMessage m = new MyMessage(sender, message_type, message);
        byte[] d = m.getBytes();
        String s = new String(d);
        System.out.println(s);
        for(int i=0; i<d.length; i++)
            System.out.print(d[i]);
    }
}
