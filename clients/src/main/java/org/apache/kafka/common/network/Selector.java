/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A nioSelector interface for doing non-blocking multi-connection network I/O.
 * <p>
 * This class works with {@link NetworkSend} and {@link NetworkReceive} to transmit size-delimited network requests and
 * responses.
 * <p>
 * A connection can be added to the nioSelector associated with an integer id by doing
 *
 * <pre>
 * nioSelector.connect(&quot;42&quot;, new InetSocketAddress(&quot;google.com&quot;, server.port), 64000, 64000);
 * </pre>
8 *
 * The connect call does not block on the creation of the TCP connection, so the connect method only begins initiating
 * the connection. The successful invocation of this method does not mean a valid connection has been established.
 *
 * Sending requests, receiving responses, processing connection completions, and disconnections on the existing
 * connections are all done using the <code>poll()</code> call.
 *
 * <pre>
 * nioSelector.send(new NetworkSend(myDestination, myBytes));
 * nioSelector.send(new NetworkSend(myOtherDestination, myOtherBytes));
 * nioSelector.poll(TIMEOUT_MS);
 * </pre>
 *
 * The nioSelector maintains several lists that are reset by each call to <code>poll()</code> which are available via
 * various getters. These are reset by each call to <code>poll()</code>.
 *
 * This class is not thread safe!
 *
 * Kafka 的 Selector 其实就是封装了 Java 原生的 NIO
 * 一个 Client 只会有一个 Selector
 */
public class Selector implements Selectable {

    public static final long NO_IDLE_TIMEOUT_MS = -1;
    private static final Logger log = LoggerFactory.getLogger(Selector.class);

    /**
     * JDK NIO 的 Selector
     * 一个 Client 只有一个 Selector，负责和所有 Broker 的网络通信
     */
    private final java.nio.channels.Selector nioSelector;

    /**
     * 每个 BrokerId 对应的 Channel
     */
    private final Map<String, KafkaChannel> channels;

    /**
     * 已经发送出去的请求
     */
    private final List<Send> completedSends;

    /**
     * 已经收到的，并且已经被处理完的响应
     */
    private final List<NetworkReceive> completedReceives;

    /**
     * 已经收到的，但是还没被处理的响应
     */
    private final Map<KafkaChannel, Deque<NetworkReceive>> stagedReceives;

    /**
     * Java NIO 的那个 Key
     */
    private final Set<SelectionKey> immediatelyConnectedKeys;

    /**
     * 断开连接的 BrokerId 集合
     */
    private final List<String> disconnected;

    /**
     * 已经建立连接的 BrokerId 集合
     */
    private final List<String> connected;

    /**
     * 发送请求失败的 BrokerId 集合
     */
    private final List<String> failedSends;
    private final Time time;
    private final SelectorMetrics sensors;
    private final String metricGrpPrefix;
    private final Map<String, String> metricTags;
    private final ChannelBuilder channelBuilder;

    /**
     * 最多可以接受数据量的大小
     */
    private final int maxReceiveSize;
    private final boolean metricsPerConnection;
    private final IdleExpiryManager idleExpiryManager;

    /**
     * Create a new nioSelector
     *
     * @param maxReceiveSize Max size in bytes of a single network receive (use {@link NetworkReceive#UNLIMITED} for no limit)
     * @param connectionMaxIdleMs Max idle connection time (use {@link #NO_IDLE_TIMEOUT_MS} to disable idle timeout)
     * @param metrics Registry for Selector metrics
     * @param time Time implementation
     * @param metricGrpPrefix Prefix for the group of metrics registered by Selector
     * @param metricTags Additional tags to add to metrics registered by Selector
     * @param metricsPerConnection Whether or not to enable per-connection metrics
     * @param channelBuilder Channel builder for every new connection
     */
    public Selector(int maxReceiveSize,
                    long connectionMaxIdleMs,
                    Metrics metrics,
                    Time time,
                    String metricGrpPrefix,
                    Map<String, String> metricTags,
                    boolean metricsPerConnection,
                    ChannelBuilder channelBuilder) {
        try {
            this.nioSelector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        this.maxReceiveSize = maxReceiveSize;
        this.time = time;
        this.metricGrpPrefix = metricGrpPrefix;
        this.metricTags = metricTags;
        this.channels = new HashMap<>();
        this.completedSends = new ArrayList<>();
        this.completedReceives = new ArrayList<>();
        this.stagedReceives = new HashMap<>();
        this.immediatelyConnectedKeys = new HashSet<>();
        this.connected = new ArrayList<>();
        this.disconnected = new ArrayList<>();
        this.failedSends = new ArrayList<>();
        this.sensors = new SelectorMetrics(metrics);
        this.channelBuilder = channelBuilder;
        this.metricsPerConnection = metricsPerConnection;
        this.idleExpiryManager = connectionMaxIdleMs < 0 ? null : new IdleExpiryManager(time, connectionMaxIdleMs);
    }

    public Selector(long connectionMaxIdleMS, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, metrics, time, metricGrpPrefix, new HashMap<String, String>(), true, channelBuilder);
    }

    /**
     * Begin connecting to the given address and add the connection to this nioSelector associated with the given id
     * number.
     * <p>
     * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long)}
     * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
     * @param id The id for the new connection
     * @param address The address to connect to
     * @param sendBufferSize The send buffer for the new connection
     * @param receiveBufferSize The receive buffer for the new connection
     * @throws IllegalStateException if there is already a connection for that id
     * @throws IOException if DNS resolution fails on the hostname or if the broker is down
     *
     * 和目标 Broker 机器建立 NIO 连接
     */
    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        if (this.channels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id);

        SocketChannel socketChannel = SocketChannel.open();

        // 设置为非阻塞
        socketChannel.configureBlocking(false);
        Socket socket = socketChannel.socket();

        // 避免客户端和服务端，任何一方断开连接，别人不知道，还一直保持网络连接占用资源
        // 设置 KeepAlive 之后，2小时内如果双方没有任何通信，那么发送一个探测包，根据探测包的结果，保持连接，重新连接，或者断开连接
        socket.setKeepAlive(true);

        // 底层的 socket 缓冲区的发送的大小
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setSendBufferSize(sendBufferSize);

        // 底层的 socket 缓冲区的接收的大小
        if (receiveBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setReceiveBufferSize(receiveBufferSize);

        // 默认设置为 false 的话，那么就开启 Nagle 算法，会把网络通信中的一些小的数据包给收集起来，然后组装成一个大的包一次性发送出去
        // 如果大量的小包在传递，会导致网络拥塞
        // 如果设置为 true 的话，意思就是关闭 Nagle，让发送出去的数据包立马通过网络传输出去
        // 应该是和 Socket 缓冲区有关系
        socket.setTcpNoDelay(true);
        boolean connected;
        try {
            // 尝试连接到 Broker 对应的 Channel 上
            connected = socketChannel.connect(address);
        } catch (UnresolvedAddressException e) {
            socketChannel.close();
            throw new IOException("Can't resolve address: " + address, e);
        } catch (IOException e) {
            socketChannel.close();
            throw e;
        }

        // 注册并且监听 OP_CONNECT 事件，和 Client 唯一的一个 Selector 绑定
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_CONNECT);

        // 将 SelectionKey，BrokerId，接收最大大小，封装为一个 KafkaChannel
        // 一个 KafkaChannel 就可以代表一个连接
        KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize);

        // attach 之后，可以根据 SelectionKey 获取到一个 KafkaChannel
        key.attach(channel);
        this.channels.put(id, channel);

        // 如果立即连接完毕的话，如果 Client 和 Broker 在一台机器上的话，一般可以立即连接成功
        if (connected) {
            // OP_CONNECT won't trigger for immediately connected channels
            log.debug("Immediately connected to node {}", channel.id());
            // 把 SelectionKey 缓存起来
            immediatelyConnectedKeys.add(key);
            key.interestOps(0);
        }
    }

    /**
     * Register the nioSelector with an existing channel
     * Use this on server-side, when a connection is accepted by a different thread but processed by the Selector
     * Note that we are not checking if the connection id is valid - since the connection already exists
     *
     * 关注 OP_READ 事件，并且封装了一套 NIO 的 API
     */
    public void register(String id, SocketChannel socketChannel) throws ClosedChannelException {
        // Kafka 统一的封装了一套 NIO 的 API
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_READ);
        KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize);
        key.attach(channel);
        this.channels.put(id, channel);
    }

    /**
     * Interrupt the nioSelector if it is blocked waiting to do I/O.
     */
    @Override
    public void wakeup() {
        this.nioSelector.wakeup();
    }

    /**
     * Close this selector and all associated connections
     */
    @Override
    public void close() {
        List<String> connections = new ArrayList<>(channels.keySet());
        for (String id : connections)
            close(id);
        try {
            this.nioSelector.close();
        } catch (IOException | SecurityException e) {
            log.error("Exception closing nioSelector:", e);
        }
        sensors.close();
        channelBuilder.close();
    }

    /**
     * 把要发送的请求暂存到 Selector 上
     * Queue the given request for sending in the subsequent {@link #poll(long)} calls
     * @param send The request to send
     */
    public void send(Send send) {
        KafkaChannel channel = channelOrFail(send.destination());
        try {
            channel.setSend(send);
        } catch (CancelledKeyException e) {
            this.failedSends.add(send.destination());
            close(channel);
        }
    }

    /**
     * Do whatever I/O can be done on each connection without blocking. This includes completing connections, completing
     * disconnections, initiating new sends, or making progress on in-progress sends or receives.
     *
     * When this call is completed the user can check for completed sends, receives, connections or disconnects using
     * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
     * lists will be cleared at the beginning of each `poll` call and repopulated by the call if there is
     * any completed I/O.
     *
     * In the "Plaintext" setting, we are using socketChannel to read & write to the network. But for the "SSL" setting,
     * we encrypt the data before we use socketChannel to write data to the network, and decrypt before we return the responses.
     * This requires additional buffers to be maintained as we are reading from network, since the data on the wire is encrypted
     * we won't be able to read exact no.of bytes as kafka protocol requires. We read as many bytes as we can, up to SSLEngine's
     * application buffer size. This means we might be reading additional bytes than the requested size.
     * If there is no further data to read from socketChannel selector won't invoke that channel and we've have additional bytes
     * in the buffer. To overcome this issue we added "stagedReceives" map which contains per-channel deque. When we are
     * reading a channel we read as many responses as we can and store them into "stagedReceives" and pop one response during
     * the poll to add the completedReceives. If there are any active channels in the "stagedReceives" we set "timeout" to 0
     * and pop response and add to the completedReceives.
     *
     * @param timeout The amount of time to wait, in milliseconds, which must be non-negative
     * @throws IllegalArgumentException If `timeout` is negative
     * @throws IllegalStateException If a send is given for which we have no existing connection or for which there is
     *         already an in-progress send
     */
    @Override
    public void poll(long timeout) throws IOException {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout should be >= 0");

        clear();

        // 如果有
        if (hasStagedReceives() || !immediatelyConnectedKeys.isEmpty())
            timeout = 0;

        /* check ready keys */
        long startSelect = time.nanoseconds();

        // 这里调用了 NIO 的 select 方法，阻塞监听就绪的事件，并且设置了一个超时的时间
        // 其实就是看有没有就绪的事件
        int readyKeys = select(timeout);
        long endSelect = time.nanoseconds();
        this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds());

        if (readyKeys > 0 || !immediatelyConnectedKeys.isEmpty()) {
            // 核心方法，对就绪的事件进行处理
            // this.nioSelector.selectedKeys() 就是就绪的 SelectionKeys
            pollSelectionKeys(this.nioSelector.selectedKeys(), false, endSelect);
            pollSelectionKeys(immediatelyConnectedKeys, true, endSelect);
        }

        // 处理刚才读取完的响应
        // 其实就是对刚才从保存了二进制数据的 ByteBuffer 的 NetworkReceive 进行处理
        addToCompletedReceives();

        long endIo = time.nanoseconds();
        this.sensors.ioTime.record(endIo - endSelect, time.milliseconds());

        // we use the time at the end of select to ensure that we don't close any connections that
        // have just been processed in pollSelectionKeys
        // 处理空闲的长连接
        maybeCloseOldestConnection(endSelect);
    }

    /**
     * select 方法，监听到有事件就绪了
     * 这里就处理那些就绪的事件
     */
    private void pollSelectionKeys(Iterable<SelectionKey> selectionKeys,
                                   boolean isImmediatelyConnected,
                                   long currentTimeNanos) {
        Iterator<SelectionKey> iterator = selectionKeys.iterator();
        // 遍历所有 Broker 的就绪事件
        while (iterator.hasNext()) {
            // 一个 key 代表了一个 Broker 的就绪的事件
            SelectionKey key = iterator.next();
            iterator.remove();

            // 之前把 KafkaChannel attch 到 SelectionKey 上了，现在就可以拿到了
            KafkaChannel channel = channel(key);

            // register all per-connection metrics at once
            sensors.maybeRegisterConnectionMetrics(channel.id());
            if (idleExpiryManager != null)
                // 更新一下 Client 和这个 Broker 最近一次通信的时间
                // 避免连接被回收掉
                idleExpiryManager.update(channel.id(), currentTimeNanos);

            try {

                /* complete any connections that have finished their handshake (either normally or immediately) */
                if (isImmediatelyConnected || key.isConnectable()) {
                    // 直到连接建立完成，其实最后调用的还是 SocketChannel 的 finishConnect 的方法
                    if (channel.finishConnect()) {
                        this.connected.add(channel.id());
                        this.sensors.connectionCreated.record();
                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        log.debug("Created socket with SO_RCVBUF = {}, SO_SNDBUF = {}, SO_TIMEOUT = {} to node {}",
                                socketChannel.socket().getReceiveBufferSize(),
                                socketChannel.socket().getSendBufferSize(),
                                socketChannel.socket().getSoTimeout(),
                                channel.id());
                    } else
                        continue;
                }

                /* if channel is not ready finish prepare */
                if (channel.isConnected() && !channel.ready())
                    channel.prepare();

                /* if channel is ready read from any connections that have readable data */
                // 读取响应，必须是没有 stagedReceives 才行，也就是必须处理完积压的响应消息
                // todo 那这里就有个问题了，socket 缓冲区可能长期保持 READABLE 的状态？然后一直写入，导致 socket 缓冲区满了？
                if (channel.ready() && key.isReadable() && !hasStagedReceive(channel)) {
                    // NetworkReceive 其实就是针对一个 RecordBatch 的相应
                    NetworkReceive networkReceive;
                    // 这里可能读到多个针对这个 Broker 的响应，需要处理粘包
                    // 如果发现 networkReceive 不是 null，说明发生粘包，需要继续读取，也就是会读取出多个 NetworkReceive
                    // 每一个 NetworkReceive 对应一个 ClientRequest，也就是说可能一次 OP_READ 事件，可能读取到多个 ClientRequest 对应的请求
                    while ((networkReceive = channel.read()) != null)
                        // stagedReceives 里保存的都是对粘包拆包处理完的 ByteBuffer，这些 ByteBuffer 被封装到了 NetworkReceive 里去
                        // 只有完整读取完了一条响应，才会把响应的结果添加到 stagedReceives 里去
                        // 如果一次 OP_READ 事件，socket 缓冲区里对应了多条响应，就会创建多个 NetworkReceive 对象，放到 stagedReceives
                        // 假如最后一条没读完，那就暂存起来，下一次 poll 操作继续处理，直到一条响应完全读取完了才能放到 stagedReceives
                        addToStagedReceives(channel, networkReceive);
                }

                /* if channel is ready write to any sockets that have space in their buffer and for which we have data */
                // 发送消息
                if (channel.ready() && key.isWritable()) {
                    // 通过 SocketChannel 真正的把暂存的请求发送出去
                    // 这里暂存的 Send，里边封装了 Client
                    Send send = channel.write();
                    if (send != null) {
                        // 已经发送出去的请求
                        this.completedSends.add(send);
                        this.sensors.recordBytesSent(channel.id(), send.size());
                    }
                }

                /* cancel any defunct sockets */
                if (!key.isValid()) {
                    close(channel);
                    this.disconnected.add(channel.id());
                }

            } catch (Exception e) {
                String desc = channel.socketDescription();
                if (e instanceof IOException)
                    log.debug("Connection with {} disconnected", desc, e);
                else
                    log.warn("Unexpected error from {}; closing connection", desc, e);
                close(channel);
                this.disconnected.add(channel.id());
            }
        }
    }

    @Override
    public List<Send> completedSends() {
        return this.completedSends;
    }

    @Override
    public List<NetworkReceive> completedReceives() {
        return this.completedReceives;
    }

    @Override
    public List<String> disconnected() {
        return this.disconnected;
    }

    @Override
    public List<String> connected() {
        return this.connected;
    }

    @Override
    public void mute(String id) {
        KafkaChannel channel = channelOrFail(id);
        mute(channel);
    }

    private void mute(KafkaChannel channel) {
        channel.mute();
    }

    @Override
    public void unmute(String id) {
        KafkaChannel channel = channelOrFail(id);
        unmute(channel);
    }

    private void unmute(KafkaChannel channel) {
        channel.unmute();
    }

    @Override
    public void muteAll() {
        for (KafkaChannel channel : this.channels.values())
            mute(channel);
    }

    @Override
    public void unmuteAll() {
        for (KafkaChannel channel : this.channels.values())
            unmute(channel);
    }

    private void maybeCloseOldestConnection(long currentTimeNanos) {
        if (idleExpiryManager == null)
            return;

        Map.Entry<String, Long> expiredConnection = idleExpiryManager.pollExpiredConnection(currentTimeNanos);
        if (expiredConnection != null) {
            String connectionId = expiredConnection.getKey();

            if (log.isTraceEnabled())
                log.trace("About to close the idle connection from {} due to being idle for {} millis",
                        connectionId, (currentTimeNanos - expiredConnection.getValue()) / 1000 / 1000);

            disconnected.add(connectionId);
            close(connectionId);
        }
    }

    /**
     * Clear the results from the prior poll
     */
    private void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();
        this.disconnected.addAll(this.failedSends);
        this.failedSends.clear();
    }

    /**
     * Check for data, waiting up to the given timeout.
     *
     * @param ms Length of time to wait, in milliseconds, which must be non-negative
     * @return The number of keys ready
     * @throws IllegalArgumentException
     * @throws IOException
     */
    private int select(long ms) throws IOException {
        if (ms < 0L)
            throw new IllegalArgumentException("timeout should be >= 0");

        if (ms == 0L)
            return this.nioSelector.selectNow();
        else
            // 第一次肯定会进到这里
            return this.nioSelector.select(ms);
    }

    /**
     * Close the connection identified by the given id
     */
    public void close(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel != null)
            close(channel);
    }

    /**
     * Begin closing this connection
     */
    private void close(KafkaChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            log.error("Exception closing connection to node {}:", channel.id(), e);
        }
        this.stagedReceives.remove(channel);
        this.channels.remove(channel.id());
        this.sensors.connectionClosed.record();

        if (idleExpiryManager != null)
            idleExpiryManager.remove(channel.id());
    }


    /**
     * check if channel is ready
     * Broker 和 Client 交互的 channel 是否就绪
     */
    @Override
    public boolean isChannelReady(String id) {
        KafkaChannel channel = this.channels.get(id);
        return channel != null && channel.ready();
    }

    private KafkaChannel channelOrFail(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel == null)
            throw new IllegalStateException("Attempt to retrieve channel for which there is no open connection. Connection id " + id + " existing connections " + channels.keySet());
        return channel;
    }

    /**
     * Return the selector channels.
     */
    public List<KafkaChannel> channels() {
        return new ArrayList<>(channels.values());
    }

    /**
     * Return the channel associated with this connection or `null` if there is no channel associated with the
     * connection.
     */
    public KafkaChannel channel(String id) {
        return this.channels.get(id);
    }

    /**
     * Get the channel associated with selectionKey
     */
    private KafkaChannel channel(SelectionKey key) {
        return (KafkaChannel) key.attachment();
    }

    /**
     * Check if given channel has a staged receive
     * KafkaChannel，其实也就是 Broker，是否已经读取过一个 NetworkReceive
     */
    private boolean hasStagedReceive(KafkaChannel channel) {
        return stagedReceives.containsKey(channel);
    }

    /**
     * check if stagedReceives have unmuted channel
     */
    private boolean hasStagedReceives() {
        for (KafkaChannel channel : this.stagedReceives.keySet()) {
            if (!channel.isMute())
                return true;
        }
        return false;
    }


    /**
     * adds a receive to staged receives
     * 获取到 每个 Broker/Channel 对应的队列
     */
    private void addToStagedReceives(KafkaChannel channel, NetworkReceive receive) {
        if (!stagedReceives.containsKey(channel))
            stagedReceives.put(channel, new ArrayDeque<NetworkReceive>());

        Deque<NetworkReceive> deque = stagedReceives.get(channel);
        deque.add(receive);
    }

    /**
     * checks if there are any staged receives and adds to completedReceives
     * 处理刚才读取完的响应
     * 如果一次读取出来多个响应消息，在这里仅仅只会把每个连接的第一个响应消息放进 completedReceives
     * todo 即使是多个 Partition 的响应，也只是取第一个 Partition 的响应进行处理？
     * todo 不是的，NetworkReceive 针对的是一个完整的 ClientRequest，包含了多个 Partition 的响应信息
     */
    private void addToCompletedReceives() {
        if (!this.stagedReceives.isEmpty()) {
            // 遍历每个 Entry，也就是以 Broker 维度来遍历的
            Iterator<Map.Entry<KafkaChannel, Deque<NetworkReceive>>> iter = this.stagedReceives.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<KafkaChannel, Deque<NetworkReceive>> entry = iter.next();
                KafkaChannel channel = entry.getKey();
                if (!channel.isMute()) {
                    // 取出每个 Broker 对应的 响应队列 里的第一个 响应
                    Deque<NetworkReceive> deque = entry.getValue();
                    NetworkReceive networkReceive = deque.poll();
                    this.completedReceives.add(networkReceive);
                    this.sensors.recordBytesReceived(channel.id(), networkReceive.payload().limit());
                    if (deque.isEmpty())
                        iter.remove();
                }
            }
        }
    }


    private class SelectorMetrics {
        private final Metrics metrics;
        public final Sensor connectionClosed;
        public final Sensor connectionCreated;
        public final Sensor bytesTransferred;
        public final Sensor bytesSent;
        public final Sensor bytesReceived;
        public final Sensor selectTime;
        public final Sensor ioTime;

        /* Names of metrics that are not registered through sensors */
        private final List<MetricName> topLevelMetricNames = new ArrayList<>();
        private final List<Sensor> sensors = new ArrayList<>();

        public SelectorMetrics(Metrics metrics) {
            this.metrics = metrics;
            String metricGrpName = metricGrpPrefix + "-metrics";
            StringBuilder tagsSuffix = new StringBuilder();

            for (Map.Entry<String, String> tag: metricTags.entrySet()) {
                tagsSuffix.append(tag.getKey());
                tagsSuffix.append("-");
                tagsSuffix.append(tag.getValue());
            }

            this.connectionClosed = sensor("connections-closed:" + tagsSuffix.toString());
            MetricName metricName = metrics.metricName("connection-close-rate", metricGrpName, "Connections closed per second in the window.", metricTags);
            this.connectionClosed.add(metricName, new Rate());

            this.connectionCreated = sensor("connections-created:" + tagsSuffix.toString());
            metricName = metrics.metricName("connection-creation-rate", metricGrpName, "New connections established per second in the window.", metricTags);
            this.connectionCreated.add(metricName, new Rate());

            this.bytesTransferred = sensor("bytes-sent-received:" + tagsSuffix.toString());
            metricName = metrics.metricName("network-io-rate", metricGrpName, "The average number of network operations (reads or writes) on all connections per second.", metricTags);
            bytesTransferred.add(metricName, new Rate(new Count()));

            this.bytesSent = sensor("bytes-sent:" + tagsSuffix.toString(), bytesTransferred);
            metricName = metrics.metricName("outgoing-byte-rate", metricGrpName, "The average number of outgoing bytes sent per second to all servers.", metricTags);
            this.bytesSent.add(metricName, new Rate());
            metricName = metrics.metricName("request-rate", metricGrpName, "The average number of requests sent per second.", metricTags);
            this.bytesSent.add(metricName, new Rate(new Count()));
            metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of all requests in the window..", metricTags);
            this.bytesSent.add(metricName, new Avg());
            metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent in the window.", metricTags);
            this.bytesSent.add(metricName, new Max());

            this.bytesReceived = sensor("bytes-received:" + tagsSuffix.toString(), bytesTransferred);
            metricName = metrics.metricName("incoming-byte-rate", metricGrpName, "Bytes/second read off all sockets", metricTags);
            this.bytesReceived.add(metricName, new Rate());
            metricName = metrics.metricName("response-rate", metricGrpName, "Responses received sent per second.", metricTags);
            this.bytesReceived.add(metricName, new Rate(new Count()));

            this.selectTime = sensor("select-time:" + tagsSuffix.toString());
            metricName = metrics.metricName("select-rate", metricGrpName, "Number of times the I/O layer checked for new I/O to perform per second", metricTags);
            this.selectTime.add(metricName, new Rate(new Count()));
            metricName = metrics.metricName("io-wait-time-ns-avg", metricGrpName, "The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.", metricTags);
            this.selectTime.add(metricName, new Avg());
            metricName = metrics.metricName("io-wait-ratio", metricGrpName, "The fraction of time the I/O thread spent waiting.", metricTags);
            this.selectTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

            this.ioTime = sensor("io-time:" + tagsSuffix.toString());
            metricName = metrics.metricName("io-time-ns-avg", metricGrpName, "The average length of time for I/O per select call in nanoseconds.", metricTags);
            this.ioTime.add(metricName, new Avg());
            metricName = metrics.metricName("io-ratio", metricGrpName, "The fraction of time the I/O thread spent doing I/O", metricTags);
            this.ioTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

            metricName = metrics.metricName("connection-count", metricGrpName, "The current number of active connections.", metricTags);
            topLevelMetricNames.add(metricName);
            this.metrics.addMetric(metricName, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return channels.size();
                }
            });
        }

        private Sensor sensor(String name, Sensor... parents) {
            Sensor sensor = metrics.sensor(name, parents);
            sensors.add(sensor);
            return sensor;
        }

        public void maybeRegisterConnectionMetrics(String connectionId) {
            if (!connectionId.isEmpty() && metricsPerConnection) {
                // if one sensor of the metrics has been registered for the connection,
                // then all other sensors should have been registered; and vice versa
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest == null) {
                    String metricGrpName = metricGrpPrefix + "-node-metrics";

                    Map<String, String> tags = new LinkedHashMap<>(metricTags);
                    tags.put("node-id", "node-" + connectionId);

                    nodeRequest = sensor(nodeRequestName);
                    MetricName metricName = metrics.metricName("outgoing-byte-rate", metricGrpName, tags);
                    nodeRequest.add(metricName, new Rate());
                    metricName = metrics.metricName("request-rate", metricGrpName, "The average number of requests sent per second.", tags);
                    nodeRequest.add(metricName, new Rate(new Count()));
                    metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of all requests in the window..", tags);
                    nodeRequest.add(metricName, new Avg());
                    metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent in the window.", tags);
                    nodeRequest.add(metricName, new Max());

                    String nodeResponseName = "node-" + connectionId + ".bytes-received";
                    Sensor nodeResponse = sensor(nodeResponseName);
                    metricName = metrics.metricName("incoming-byte-rate", metricGrpName, tags);
                    nodeResponse.add(metricName, new Rate());
                    metricName = metrics.metricName("response-rate", metricGrpName, "The average number of responses received per second.", tags);
                    nodeResponse.add(metricName, new Rate(new Count()));

                    String nodeTimeName = "node-" + connectionId + ".latency";
                    Sensor nodeRequestTime = sensor(nodeTimeName);
                    metricName = metrics.metricName("request-latency-avg", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Avg());
                    metricName = metrics.metricName("request-latency-max", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Max());
                }
            }
        }

        public void recordBytesSent(String connectionId, long bytes) {
            long now = time.milliseconds();
            this.bytesSent.record(bytes, now);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }

        public void recordBytesReceived(String connection, int bytes) {
            long now = time.milliseconds();
            this.bytesReceived.record(bytes, now);
            if (!connection.isEmpty()) {
                String nodeRequestName = "node-" + connection + ".bytes-received";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }

        public void close() {
            for (MetricName metricName : topLevelMetricNames)
                metrics.removeMetric(metricName);
            for (Sensor sensor : sensors)
                metrics.removeSensor(sensor.name());
        }
    }

    // helper class for tracking least recently used connections to enable idle connection closing

    /**
     * 这个的核心思路是啥，因为一个 Client，总不能和所有 Broker 永远的维持连接，太消耗资源了
     * 长期不用的长连接，那就应该断开
     * 因此，就是用了一个 LRU 链表的方式来维护 Client 和 Broker 的连接的热度
     * 通过维护 LinkedHashMap（JDK 自己实现的 LRU）的数据结构，来对连接是否断开进行管理
     */
    private static class IdleExpiryManager {
        private final Map<String, Long> lruConnections;

        /**
         * 一个长连接最多可以空闲多久
         */
        private final long connectionsMaxIdleNanos;

        /**
         * 下一次什么时候检查是否应该清理长连接
         */
        private long nextIdleCloseCheckTime;

        public IdleExpiryManager(Time time, long connectionsMaxIdleMs) {
            this.connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000;
            // initial capacity and load factor are default, we set them explicitly because we want to set accessOrder = true
            this.lruConnections = new LinkedHashMap<>(16, .75F, true);
            this.nextIdleCloseCheckTime = time.nanoseconds() + this.connectionsMaxIdleNanos;
        }

        /**
         * 更新 Client 和某个 Broker 最近的通信时间
         * @param connectionId  BrokerId
         * @param currentTimeNanos  最近一次通信的时间
         */
        public void update(String connectionId, long currentTimeNanos) {
            lruConnections.put(connectionId, currentTimeNanos);
        }

        public Map.Entry<String, Long> pollExpiredConnection(long currentTimeNanos) {
            if (currentTimeNanos <= nextIdleCloseCheckTime)
                return null;

            if (lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
                return null;
            }

            Map.Entry<String, Long> oldestConnectionEntry = lruConnections.entrySet().iterator().next();
            Long connectionLastActiveTime = oldestConnectionEntry.getValue();
            nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos;

            if (currentTimeNanos > nextIdleCloseCheckTime)
                return oldestConnectionEntry;
            else
                return null;
        }

        public void remove(String connectionId) {
            lruConnections.remove(connectionId);
        }
    }

}
