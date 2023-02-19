import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology
import org.apache.storm.topology.OutputFieldsDeclarer
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.tuple.{Fields, Values}

import java.util
import scala.util.Random

class MsgSpout extends BaseRichSpout{
  var collector :SpoutOutputCollector = _
  var random : Random = _
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("Message"))
  }

  override def open(conf: util.Map[String, AnyRef], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    this.collector = collector
    random = new Random()
  }

  override def nextTuple(): Unit = {
    Thread.sleep(5)
    collector.emit(new Values(random.nextInt(10)))
  }
}
