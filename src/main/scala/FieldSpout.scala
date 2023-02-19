import org.apache.logging.log4j.core.Logger
import org.apache.storm.spout.SpoutOutputCollector
import org.apache.storm.task.TopologyContext
import org.apache.storm.topology.base.BaseRichSpout
import org.apache.storm.topology.{ OutputFieldsDeclarer}
import org.apache.storm.tuple.{Fields, Values}

import java.util
import scala.util.Random

class FieldSpout extends BaseRichSpout{
  var collector : SpoutOutputCollector = _
  var random : Random  = _
  val key = Seq(1,2,3,4)
  override def declareOutputFields(declarer: OutputFieldsDeclarer): Unit = {
    declarer.declare(new Fields("key","value"))
  }

  override def open(conf: util.Map[String, AnyRef], context: TopologyContext, collector: SpoutOutputCollector): Unit = {
    this.collector = collector
    random = new Random()
  }

  override def nextTuple(): Unit = {
    collector.emit(new Values(key(random.nextInt(key.size)),random.nextInt(10)))
  }
}
