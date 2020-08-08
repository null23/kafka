/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.common.network;


import java.io.IOException;

import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.apache.kafka.common.utils.Utils;

public class KafkaChannel {

    /**
     * BrokerId
     */
    private final String id;

    /**
     * 封装了 NIO 的 SocketChannel
     */
    private final TransportLayer transportLayer;
    private final Authenticator authenticator;
    private final int maxReceiveSize;
    private NetworkReceive receive;

    /**
     * send 是针对 KafkaChannel 的，而不是针对 Selector 的
     */
    private Send send;

    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.maxReceiveSize = maxReceiveSize;
    }

    public void close() throws IOException {
        Utils.closeAll(transportLayer, authenticator);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public Principal principal() throws IOException {
        return authenticator.principal();
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator
     */
    public void prepare() throws IOException {
        if (!transportLayer.ready())
            transportLayer.handshake();
        if (transportLayer.ready() && !authenticator.complete())
            authenticator.authenticate();
    }

    public void disconnect() {
        transportLayer.disconnect();
    }


    public boolean finishConnect() throws IOException {
        return transportLayer.finishConnect();
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public void mute() {
        transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }

    public void unmute() {
        transportLayer.addInterestOps(SelectionKey.OP_READ);
    }

    public boolean isMute() {
        return transportLayer.isMute();
    }

    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    /**
     * 暂存将要发送的数据，并且对 OP_WRITE 事件感兴趣
     * @param send
     */
    public void setSend(Send send) {
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        // 暂存将要发送的数据
        this.send = send;
        // 关注 OP_WRITE 事件
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    /**
     * 读取响应，后续会处理粘包和拆包
     */
    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        // 每读取一条响应消息，都会创建一个新的 NetworkReceive
        // receive 什么时候不为空？就是发生拆包问题的时候，上次响应的数据没读完，然后给暂存起来了
        // 这次就可以接着上次的 receive 继续往后读
        // 就比如，size 拆包了，上次只读了 2 个字节，然后，这次就上次暂存的，继续读取 2 个字节
        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id);
        }

        // 处理粘包拆包
        receive(receive);
        if (receive.complete()) {
            receive.payload().rewind();
            result = receive;
            receive = null;
        }
        return result;
    }

    /**
     * 发送请求数据，并且清空 Channel 的缓存的 send
     */
    public Send write() throws IOException {
        Send result = null;
        // 判断拆包
        // 如果 send != null，说明上次的请求没有发送完
        // 如果 !send(send)，说明这次的请求没有发送完
        // 没有发送完，就从上次的 remaining 继续发送就可以了
        if (send != null && send(send)) {
            result = send;
            send = null;
        }
        return result;
    }

    /**
     * 会处理拆包问题
     * @param receive 这里的 receive，可能是新的，也可能是之前发生拆包之后暂存的
     * @return 读取了多少个字节
     */
    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    /**
     * 发送暂存的消息
     * 如果发送完了，取消对 OP_WRITE 事件的关注
     * 如果没发送完，其实就是粘包和拆包，下次继续发送
     * @param send  暂存的要发送的数据
     */
    private boolean send(Send send) throws IOException {
        send.writeTo(transportLayer);
        if (send.completed())
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);

        return send.completed();
    }

}
