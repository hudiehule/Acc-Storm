package org.apache.storm.topology.accelerate;

import org.apache.storm.task.IBolt;
import org.apache.storm.topology.IComponent;

/**
 * Created by Administrator on 2018/5/15.
 */
public interface IRichAccBolt extends IBolt,IAccBolt,IComponent {
}
