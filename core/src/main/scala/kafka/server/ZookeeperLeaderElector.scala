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
package kafka.server

import kafka.utils.ZkUtils._
import kafka.utils.CoreUtils._
import kafka.utils.{Json, SystemTime, Logging, ZKCheckedEphemeral}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.IZkDataListener
import kafka.controller.ControllerContext
import kafka.controller.KafkaController
import org.apache.kafka.common.security.JaasUtils

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 */
class ZookeeperLeaderElector(controllerContext: ControllerContext,
                             electionPath: String,
                             onBecomingLeader: () => Unit,
                             onResigningAsLeader: () => Unit,
                             brokerId: Int)
  extends LeaderElector with Logging {
  var leaderId = -1
  // create the election path in ZK, if one does not exist
  val index = electionPath.lastIndexOf("/")
  if (index > 0)
    controllerContext.zkUtils.makeSurePersistentPathExists(electionPath.substring(0, index))
  val leaderChangeListener = new LeaderChangeListener

  /**
    * 注册 Controller
    */
  def startup {
    inLock(controllerContext.controllerLock) {
      /**
        * 在 electionPath 这个路径上注册一个监听器，并且注册 Controller 节点变化之后的回调函数（所谓 "/controller" 节点变化，其实就是 Controller 宕机了）
        * 如果有人竞争成为了 Controller，他会感知到
        * 如果有 Controller 挂了，他也能感知到
        */
      // electionPath 是 zk 里 ZNode 的路径
      controllerContext.zkUtils.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)

      /**
        * 发起一个选举
        */
      elect
    }
  }

  /**
    * 从 zk 上的 "/controller" ZNode 读取值
    * 如果存在值，则说明有人竞争成为了 Controller
    * 如果不存在值，则返回 -1
    * @return
    */
  private def getControllerID(): Int = {
    controllerContext.zkUtils.readDataMaybeNull(electionPath)._1 match {
       case Some(controller) => KafkaController.parseControllerId(controller)
       case None => -1
    }
  }

  /**
    * 发起在 zk 上的选举
    * 其实就是多个 Broker 抢着创建一个 "/controller" 节点
    * @return
    */
  def elect: Boolean = {
    val timestamp = SystemTime.milliseconds.toString
    val electString = Json.encode(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp))

    /**
      * 从 zk 上获取 "/controller" 这个 ZNode 的值
      */
    leaderId = getControllerID
    /* 
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition, 
     * it's possible that the controller has already been elected when we get here. This check will prevent the following 
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     *
     * 集群里已经有人成为 Controller 了
     */
    if(leaderId != -1) {
       debug("Broker %d has been elected as leader, so stopping the election process.".format(leaderId))
       return amILeader
    }

    try {
      // 由于其他 Broker 还没有注册成功，因此当前 Broker 就尝试注册
      // 这里会有并发竞争注册的情况
      // 这个理解为一个 zk 的节点，尝试去创建 "controller" znode
      val zkCheckedEphemeral = new ZKCheckedEphemeral(electionPath,
                                                      electString,
                                                      controllerContext.zkUtils.zkConnection.getZookeeper,
                                                      JaasUtils.isZkSecurityEnabled())

      // 尝试去 "/controller" znode 目录下创建一个节点
      // 当然有可能会失败，因为这里是分布式环境下多个 Broker 同时竞争的
      // 如果失败，就会被下边的 cache 捕获到异常
      zkCheckedEphemeral.create()
      info(brokerId + " successfully elected as leader")

      // 当前的 leaderId 设置为当前的 BrokerId，也就是这个 Broker 成功的成为了 Controller
      leaderId = brokerId

      onBecomingLeader()
    } catch {
      // 有其他 Broker 已经注册了
      case e: ZkNodeExistsException =>
        // If someone else has written the path, then
        leaderId = getControllerID 

        if (leaderId != -1)
          debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
        else
          warn("A leader has been elected but just resigned, this will result in another round of election")

      case e2: Throwable =>
        error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
        resign()
    }
    amILeader
  }

  def close = {
    leaderId = -1
  }

  def amILeader : Boolean = leaderId == brokerId

  def resign() = {
    leaderId = -1
    controllerContext.zkUtils.deletePath(electionPath)
  }

  /**
   * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
   * have its own session expiration listener and handler
    *
    * Controller 宕机之后，就会回调到这个 Listener
   */
  class LeaderChangeListener extends IZkDataListener with Logging {
    /**
     * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
     * @throws Exception On any error.
     */
    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      inLock(controllerContext.controllerLock) {
        val amILeaderBeforeDataChange = amILeader
        leaderId = KafkaController.parseControllerId(data.toString)
        info("New leader is %d".format(leaderId))
        // The old leader needs to resign leadership if it is no longer the leader
        if (amILeaderBeforeDataChange && !amILeader)
          onResigningAsLeader()
      }
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
     * @throws Exception
     *             On any error.
      * Controller 宕机之后就会回调这个方法，触发 Controller 的重新选举
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
      inLock(controllerContext.controllerLock) {
        debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
          .format(brokerId, dataPath))
        if(amILeader)
          onResigningAsLeader()
        elect
      }
    }
  }
}
