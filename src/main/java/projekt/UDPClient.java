package projekt;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UDPClient {

    private DatagramChannel multicastChannel;
    private DatagramChannel broadcastChannel;
    private NetworkInterface nic = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());

    private ExecutorService IOHandling = Executors.newFixedThreadPool(2);

    private final int PORT;
    private final String MULTICAST_GROUP_ADDRESS = "239.1.1.1";
    
    private volatile boolean isRunning;

    public UDPClient(int port) throws IOException {
        this.PORT = port;
        this.isRunning = false;
        setupMulticast();
        setupBroadcast();
    }

    private void setupMulticast() throws IOException {
        this.multicastChannel = DatagramChannel.open();
        this.multicastChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.multicastChannel.bind(new InetSocketAddress(this.PORT));
        this.multicastChannel.join(InetAddress.getByName(this.MULTICAST_GROUP_ADDRESS), this.nic);
    }

    private void setupBroadcast() throws IOException {
        this.broadcastChannel = DatagramChannel.open();
        this.broadcastChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.broadcastChannel.setOption(StandardSocketOptions.SO_BROADCAST, true);
        this.broadcastChannel.bind(new InetSocketAddress(this.PORT));
    }

    public void start(){
        this.isRunning = true;
        this.IOHandling.submit(this::handleSend);
        this.IOHandling.submit(this::handleReceive);
    }
    
    private void handleReceive(){
        try(Selector selector = Selector.open()){
            this.multicastChannel.register(selector, SelectionKey.OP_READ, "MULTICAST");
            this.broadcastChannel.register(selector, SelectionKey.OP_READ, "BROADCAST");
            
            ByteBuffer buffer = ByteBuffer.allocate(4096);

            while(this.isRunning){
                selector.select();
                for(SelectionKey key : selector.selectedKeys()){
                    buffer.clear();
                    String type = (String)key.attachment();
                    DatagramChannel channel = (DatagramChannel)key.channel();

                    InetSocketAddress senderAddress = (InetSocketAddress) channel.receive(buffer);
                    String message = new String(buffer.array(), 0, buffer.limit()); 
                    System.out.println(type + "|" + senderAddress + ": " + message);
                }
                selector.selectedKeys().clear();
            }

        }catch(Exception e){
            if(this.isRunning){
                e.printStackTrace();
            }
        }
    }

    private void handleSend(){
        try(Scanner scanner = new Scanner(System.in)){
            while(this.isRunning){
                String input = scanner.nextLine();
                ByteBuffer buffer = ByteBuffer.wrap(input.getBytes(StandardCharsets.UTF_8));

                this.multicastChannel.send(buffer, new InetSocketAddress(this.MULTICAST_GROUP_ADDRESS, this.PORT));
                this.broadcastChannel.send(buffer, new InetSocketAddress("255.255.255.255", this.PORT));
            }
        }catch(Exception e){
            if(this.isRunning){
                e.printStackTrace();
            }
        
        }
    }
}
