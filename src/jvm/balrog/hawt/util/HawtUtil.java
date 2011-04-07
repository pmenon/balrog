package balrog.hawt.util;

import java.util.LinkedList;

import org.fusesource.hawtdispatch.CustomDispatchSource;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.DispatchSource;
import org.fusesource.hawtdispatch.EventAggregator;
import org.fusesource.hawtdispatch.EventAggregators;

public class HawtUtil {

    public static DispatchQueue createDispatchQueue(String name) {
	final DispatchQueue queue = Dispatch.createQueue(name);
	return queue;
    }

    public static CustomDispatchSource<Object, LinkedList<Object>> createDispatchSource(DispatchQueue queue) {
	final CustomDispatchSource<Object, LinkedList<Object>> source = Dispatch.createSource(EventAggregators.linkedList(), queue);
	return source;
    }

    public static void merge(CustomDispatchSource<Object, LinkedList<Object>> source, Object event) {
	source.merge(event);
    }
}